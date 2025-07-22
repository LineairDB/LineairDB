/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#ifndef LINEAIRDB_DATABASE_IMPL_H
#define LINEAIRDB_DATABASE_IMPL_H

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/secondary_index_option.h>
#include <lineairdb/table.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <functional>
#include <tuple>
#include <utility>

#include "callback/callback_manager.h"
#include "index/concurrent_table.h"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.h"
#include "thread_pool/thread_pool.h"
#include "transaction_impl.h"
#include "util/backoff.hpp"
#include "util/epoch_framework.hpp"
#include "util/logger.hpp"

namespace LineairDB {
class Database::Impl {
  friend class Transaction::Impl;

 public:
  inline static Database::Impl* CurrentDBInstance;

  Impl(const Config& c = Config())
      : config_(c),
        thread_pool_(c.max_thread),
        logger_(config_),
        callback_manager_(config_),
        epoch_framework_(c.epoch_duration_ms, EventsOnEpochIsUpdated()),
        index_(epoch_framework_, config_),

        checkpoint_manager_(config_, index_, epoch_framework_) {
    if (Database::Impl::CurrentDBInstance == nullptr) {
      Database::Impl::CurrentDBInstance = this;
      SPDLOG_INFO("LineairDB instance has been constructed.");
    } else {
      SPDLOG_ERROR(
          "It is prohibited to allocate two LineairDB::Database instance at "
          "the same time.");
      exit(1);
    }
    if (config_.enable_recovery) {
      Recovery();
    }
    epoch_framework_.Start();
  }

  ~Impl() {
    Fence();
    thread_pool_.StopAcceptingTransactions();
    epoch_framework_.Sync();
    checkpoint_manager_.Stop();
    epoch_framework_.Stop();
    while (!thread_pool_.IsEmpty()) {
      std::this_thread::yield();
    }
    thread_pool_.Shutdown();
    SPDLOG_DEBUG(
        "Epoch number and Durable epoch number are ended at {0}, and {1}, "
        "respectively.",
        epoch_framework_.GetGlobalEpoch(), logger_.GetDurableEpoch());
    SPDLOG_INFO("LineairDB instance has been destructed.");
    assert(Database::Impl::CurrentDBInstance == this);
    Database::Impl::CurrentDBInstance = nullptr;
  }

  void ExecuteTransaction(ProcedureType proc, CallbackType clbk,
                          std::optional<CallbackType> prclbk) {
    for (;;) {
      bool success = thread_pool_.Enqueue([&, transaction_procedure = proc,
                                           callback = clbk,
                                           precommit_clbk = prclbk]() {
        epoch_framework_.MakeMeOnline();
        Transaction tx(this);

        transaction_procedure(tx);
        if (tx.IsAborted()) {
          if (precommit_clbk)
            precommit_clbk.value()(LineairDB::TxStatus::Aborted);
          callback(LineairDB::TxStatus::Aborted);
          epoch_framework_.MakeMeOffline();
          return;
        }

        bool committed = tx.Precommit();
        if (committed) {
          tx.tx_pimpl_->PostProcessing(TxStatus::Committed);

          if (precommit_clbk.has_value()) {
            precommit_clbk.value()(TxStatus::Committed);
          }
          const auto current_epoch = epoch_framework_.GetMyThreadLocalEpoch();
          callback_manager_.Enqueue(std::move(callback), current_epoch);
          if (config_.enable_logging) {
            logger_.Enqueue(tx.tx_pimpl_->write_set_, current_epoch);
          }
        } else {
          tx.tx_pimpl_->PostProcessing(TxStatus::Aborted);
          if (precommit_clbk.has_value()) {
            precommit_clbk.value()(TxStatus::Aborted);
          }
          callback(LineairDB::TxStatus::Aborted);
        }

        epoch_framework_.MakeMeOffline();
      });
      if (success) break;
    }
  }

  Transaction& BeginTransaction() {
    epoch_framework_.MakeMeOnline();
    return *(new Transaction(this));
  }

  bool EndTransaction(Transaction& tx, CallbackType clbk) {
    if (tx.IsAborted()) {
      clbk(TxStatus::Aborted);
      delete &tx;
      epoch_framework_.MakeMeOffline();
      return false;
    }

    bool committed = tx.Precommit();
    if (committed) {
      tx.tx_pimpl_->PostProcessing(TxStatus::Committed);

      tx.tx_pimpl_->current_status_ = TxStatus::Committed;
      const auto current_epoch = epoch_framework_.GetMyThreadLocalEpoch();
      callback_manager_.Enqueue(std::move(clbk), current_epoch, true);

      if (config_.enable_logging) {
        logger_.Enqueue(tx.tx_pimpl_->write_set_, current_epoch, true);
      }
    } else {
      tx.tx_pimpl_->PostProcessing(TxStatus::Aborted);
      clbk(TxStatus::Aborted);
    }
    epoch_framework_.MakeMeOffline();

    if (config_.enable_checkpointing) {
      auto checkpoint_completed =
          checkpoint_manager_.GetCheckpointCompletedEpoch();
      logger_.TruncateLogs(checkpoint_completed);
    }
    delete &tx;
    return committed;
  }

  void RequestCallbacks() {
    const auto current_epoch = epoch_framework_.GetGlobalEpoch();
    callback_manager_.ExecuteCallbacks(current_epoch);
  }

  const EpochNumber& GetMyThreadLocalEpoch() {
    return epoch_framework_.GetMyThreadLocalEpoch();
  }

  void Fence() {
    epoch_framework_.Sync();
    thread_pool_.WaitForQueuesToBecomeEmpty();
    callback_manager_.WaitForAllCallbacksToBeExecuted();
  }
  const Config& GetConfig() const { return config_; }
  Index::ConcurrentTable& GetIndex() { return index_; }

  // NOTE: Called by a special thread managed by EpochFramework.
  std::function<void(EpochNumber)> EventsOnEpochIsUpdated() {
    return [&](EpochNumber old_epoch) {
      // Logging
      if (config_.enable_logging) {
        EpochNumber durable_epoch = logger_.FlushDurableEpoch();
        thread_pool_.EnqueueForAllThreads(
            [&, old_epoch]() { logger_.FlushLogs(old_epoch); });
        thread_pool_.EnqueueForAllThreads([&, durable_epoch] {
          callback_manager_.ExecuteCallbacks(durable_epoch);
        });
      }

      // Execute Callbacks
      thread_pool_.EnqueueForAllThreads(
          [&, old_epoch]() { callback_manager_.ExecuteCallbacks(old_epoch); });

      if (config_.enable_checkpointing) {
        auto checkpoint_completed =
            checkpoint_manager_.GetCheckpointCompletedEpoch();

        thread_pool_.EnqueueForAllThreads([&, checkpoint_completed]() {
          logger_.TruncateLogs(checkpoint_completed);
        });
      }
    };
  }

  void WaitForCheckpoint() {
    const auto start = checkpoint_manager_.GetCheckpointCompletedEpoch();
    Util::RetryWithExponentialBackoff([&]() {
      const auto current = checkpoint_manager_.GetCheckpointCompletedEpoch();
      return start != current;
    });
  }

  bool IsNeedToCheckpointing(const EpochNumber epoch) {
    return checkpoint_manager_.IsNeedToCheckpointing(epoch);
  }

  bool CreateTable(const std::string table_name) {
    if (tables_.find(table_name) != tables_.end()) {
      return false;
    }
    auto [it, inserted] = tables_.emplace(
        std::piecewise_construct, std::forward_as_tuple(table_name),
        std::forward_as_tuple(epoch_framework_, config_));
    return inserted;
  }

  template <typename T>
  bool CreateSecondaryIndex(const std::string table_name,
                            const std::string index_name,
                            const SecondaryIndexOption::Constraint constraint) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
      return false;
    }
    return it->second.CreateSecondaryIndex<T>(index_name, constraint);
  }

 private:
  void Recovery() {
    SPDLOG_INFO("Start recovery process");
    // Start recovery from logfiles
    EpochNumber highest_epoch = 1;
    const auto durable_epoch = logger_.GetDurableEpochFromLog();
    SPDLOG_DEBUG("  Durable epoch is resumed from {0}", highest_epoch);
    logger_.SetDurableEpoch(durable_epoch);
    [[maybe_unused]] auto enqueued = thread_pool_.EnqueueForAllThreads(
        [&]() { logger_.RememberMe(durable_epoch); });
    assert(enqueued);

    thread_pool_.WaitForQueuesToBecomeEmpty();

    epoch_framework_.MakeMeOnline();

    auto& local_epoch = epoch_framework_.GetMyThreadLocalEpoch();
    local_epoch = durable_epoch;

    highest_epoch = std::max(highest_epoch, durable_epoch);
    auto&& recovery_set = logger_.GetRecoverySetFromLogs(durable_epoch);
    for (auto& entry : recovery_set) {
      highest_epoch = std::max(
          highest_epoch, entry.data_item_copy.transaction_id.load().epoch);

      index_.Put(entry.key, std::move(entry.data_item_copy));
    }
    epoch_framework_.MakeMeOffline();

    SPDLOG_DEBUG("  Global epoch is resumed from {0}", highest_epoch);
    epoch_framework_.SetGlobalEpoch(highest_epoch);
    SPDLOG_INFO("Finish recovery process");
  }

 private:
  Config config_;
  ThreadPool thread_pool_;
  Recovery::Logger logger_;
  Callback::CallbackManager callback_manager_;
  EpochFramework epoch_framework_;
  std::unordered_map<std::string, Table> tables_;
  Index::ConcurrentTable index_;
  Recovery::CPRManager checkpoint_manager_;
};

// Database::Impl* Database::Impl::CurrentDBInstance = nullptr;

}  // namespace LineairDB
#endif /** LINEAIRDB_DATABASE_IMPL_H **/
