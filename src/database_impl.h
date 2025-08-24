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
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <functional>
#include <shared_mutex>
#include <tuple>
#include <utility>

#include "callback/callback_manager.h"
#include "index/concurrent_table.h"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.h"
#include "table/table.h"
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
        tables_(),
        checkpoint_manager_(config_, tables_, epoch_framework_) {
    if (Database::Impl::CurrentDBInstance == nullptr) {
      Database::Impl::CurrentDBInstance = this;
      SPDLOG_INFO("LineairDB instance has been constructed.");
    } else {
      SPDLOG_ERROR(
          "It is prohibited to allocate two LineairDB::Database instance at "
          "the same time.");
      exit(1);
    }
    if (!config_.anonymous_table_name.empty()) {
      CreateTable(config_.anonymous_table_name);
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

  /**
   * Ensures that all pending operations are completed and all callbacks have
   * been executed.
   *
   * Note: Due to the dependency on the implementation of
   * moodycamel::concurrentqueue, callbacks are executed **after**
   * try_dequeue(). This means the queue size can become zero even if some
   * callbacks have not yet been executed. As a result, the current
   * `WaitForAllCallbacksToBeExecuted()` does not strictly behave as its name
   * suggests (since it only checks if the queue length is zero).
   *
   * To address this problem, an atomic variable `latest_callbacked_epoch_` is
   * used as a workaround to ensure proper waiting.
   */
  void Fence() {
    const auto current_epoch = epoch_framework_.GetGlobalEpoch();
    epoch_framework_.Sync();
    thread_pool_.WaitForQueuesToBecomeEmpty();
    callback_manager_.WaitForAllCallbacksToBeExecuted();
    // Spin-wait with yield for better performance in the critical path
    while (latest_callbacked_epoch_.load() < current_epoch) {
      std::this_thread::yield();
    }
  }
  const Config& GetConfig() const { return config_; }

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
      thread_pool_.EnqueueForAllThreads([&, old_epoch]() {
        callback_manager_.ExecuteCallbacks(old_epoch);
        latest_callbacked_epoch_.store(old_epoch);
      });

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

  bool CreateTable(const std::string_view table_name) {
    std::unique_lock<std::shared_mutex> lk(schema_mutex_);
    if (tables_.find(std::string(table_name)) != tables_.end()) {
      return false;
    }
    auto [it, inserted] =
        tables_.emplace(std::piecewise_construct,
                        std::forward_as_tuple(std::string(table_name)),
                        std::forward_as_tuple(epoch_framework_, config_,
                                              std::string(table_name)));
    return inserted;
  }

  std::optional<Table*> GetTable(const std::string_view table_name) {
    std::shared_lock lk(schema_mutex_);
    if (auto it = tables_.find(std::string(table_name)); it != tables_.end()) {
      return &it->second;
    }
    return std::nullopt;
  }

 private:
  void Recovery() {
    SPDLOG_INFO("Start recovery process");
    // Start recovery from logfiles
    EpochNumber highest_epoch = 1;
    const auto durable_epoch = logger_.GetDurableEpochFromLog();
    SPDLOG_DEBUG("  Durable epoch is resumed from {0}", highest_epoch);
    logger_.SetDurableEpoch(durable_epoch);

    thread_pool_.WaitForQueuesToBecomeEmpty();

    epoch_framework_.MakeMeOnline();
    auto& local_epoch = epoch_framework_.GetMyThreadLocalEpoch();
    local_epoch = durable_epoch;

    highest_epoch = std::max(highest_epoch, durable_epoch);
    auto&& recovery_sets = logger_.GetRecoverySetFromLogs(durable_epoch);

    for (auto& recovery_set : recovery_sets) {
      CreateTable(recovery_set.table_name);
      auto table = GetTable(recovery_set.table_name);
      if (!table.has_value()) {
        SPDLOG_ERROR("Table {0} not found", recovery_set.table_name);
        continue;
      }

      highest_epoch =
          std::max(highest_epoch,
                   recovery_set.data_item_copy.transaction_id.load().epoch);
      table.value()->GetPrimaryIndex().Put(
          recovery_set.key, std::move(recovery_set.data_item_copy));
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
  std::atomic<EpochNumber> latest_callbacked_epoch_{1};
  Recovery::CPRManager checkpoint_manager_;
  std::shared_mutex schema_mutex_;
};

}  // namespace LineairDB
#endif /** LINEAIRDB_DATABASE_IMPL_H **/
