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

#include "callback/callback_manager.h"
#include "index/concurrent_table.h"
#include "recovery/logger.h"
#include "thread_pool/thread_pool.h"
#include "transaction_impl.h"
#include "util/epoch_framework.hpp"
#include "util/logger.hpp"

namespace LineairDB {
class Database::Impl {
 public:
  inline static Database::Impl* CurrentDBInstance;

  Impl(const Config& c = Config())
      : config_(c),
        thread_pool_(c.max_thread),
        logger_(c),
        callback_manager_(c),
        point_index_(c),
        epoch_framework_(c.epoch_duration_ms, EventsOnEpochIsUpdated()) {
    if (Database::Impl::CurrentDBInstance == nullptr) {
      Database::Impl::CurrentDBInstance = this;
    } else {
      SPDLOG_ERROR(
          "It is prohibited to allocate two LineairDB::Database instance at "
          "the same time.");
      exit(1);
    }
    if (config_.enable_recovery) { Recovery(); }
    epoch_framework_.Start();
  };

  ~Impl() {
    thread_pool_.StopAcceptingTransactions();
    epoch_framework_.Sync();
    epoch_framework_.Stop();
    while (!thread_pool_.IsEmpty()) { std::this_thread::yield(); }
    thread_pool_.Shutdown();
    SPDLOG_DEBUG(
        "Epoch number and Durable epoch number are ended at {0}, and {1}, "
        "respectively.",
        epoch_framework_.GetGlobalEpoch(), logger_.GetDurableEpoch());
    assert(Database::Impl::CurrentDBInstance == this);
    Database::Impl::CurrentDBInstance = nullptr;
  };

  void ExecuteTransaction(ProcedureType proc, CallbackType clbk) {
    for (;;) {
      bool success = thread_pool_.Enqueue([&, transaction_procedure = proc,
                                           callback = clbk]() {
        epoch_framework_.MakeMeOnline();
        Transaction tx(this);

        transaction_procedure(tx);
        if (tx.tx_pimpl_->user_aborted_) {
          callback(LineairDB::TxStatus::Aborted);
          epoch_framework_.MakeMeOffline();
          return;
        }

        bool committed = tx.Precommit();
        if (committed) {
          if (!config_.enable_logging) { tx.tx_pimpl_->write_set_.clear(); }
          const auto current_epoch = epoch_framework_.GetMyThreadLocalEpoch();
          logger_.Enqueue(tx.tx_pimpl_->write_set_, current_epoch);
          callback_manager_.Enqueue(std::move(callback), current_epoch);
        } else {
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
    if (tx.tx_pimpl_->user_aborted_) {
      clbk(LineairDB::TxStatus::Aborted);
      epoch_framework_.MakeMeOffline();
      delete &tx;
      return false;
    }

    bool committed = tx.Precommit();
    if (committed) {
      if (!config_.enable_logging) { tx.tx_pimpl_->write_set_.clear(); }
      const auto current_epoch = epoch_framework_.GetMyThreadLocalEpoch();
      logger_.Enqueue(tx.tx_pimpl_->write_set_, current_epoch);
      callback_manager_.Enqueue(std::move(clbk), current_epoch, true);
    } else {
      clbk(LineairDB::TxStatus::Aborted);
    }
    epoch_framework_.MakeMeOffline();

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
  Index::ConcurrentTable& GetPointIndex() { return point_index_; }

  // NOTE: Called by a special thread managed by EpochFramework.
  std::function<void(EpochNumber)> EventsOnEpochIsUpdated() {
    return [&](EpochNumber old_epoch) {
      // Logging
      if (config_.enable_logging) {
        EpochNumber durable_epoch = logger_.FlushDurableEpoch();
        thread_pool_.EnqueueForAllThreads(
            [&, old_epoch]() { logger_.FlushLogs(old_epoch); });
        callback_manager_.ExecuteCallbacks(durable_epoch);
      }
      // Execute Callbacks
      thread_pool_.EnqueueForAllThreads(
          [&, old_epoch]() { callback_manager_.ExecuteCallbacks(old_epoch); });
      callback_manager_.ExecuteCallbacks(old_epoch);
    };
  }

 private:
  void Recovery() {
    SPDLOG_INFO("Start recovery process");
    // Start recovery from logfiles
    EpochNumber highest_epoch = 1;
    const auto durable_epoch  = Recovery::Logger::GetDurableEpochFromLog();
    SPDLOG_DEBUG("  Durable epoch is resumed from {0}", highest_epoch);
    logger_.SetDurableEpoch(durable_epoch);
    [[maybe_unused]] auto enqueued = thread_pool_.EnqueueForAllThreads(
        [&]() { logger_.RememberMe(durable_epoch); });
    assert(enqueued);

    thread_pool_.WaitForQueuesToBecomeEmpty();

    highest_epoch = std::max(highest_epoch, durable_epoch);
    auto&& recovery_set =
        Recovery::Logger::GetRecoverySetFromLogs(durable_epoch);
    for (auto& entry : recovery_set) {
      highest_epoch = std::max(
          highest_epoch, entry.data_item_copy.transaction_id.load().epoch);

      point_index_.Put(entry.key, entry.data_item_copy);
    }
    SPDLOG_DEBUG("  Global epoch is resumed from {0}", highest_epoch);
    epoch_framework_.SetGlobalEpoch(highest_epoch);
    SPDLOG_INFO("Finish recovery process");
  }

 private:
  Config config_;
  ThreadPool thread_pool_;
  Recovery::Logger logger_;
  Callback::CallbackManager callback_manager_;
  Index::ConcurrentTable point_index_;
  EpochFramework epoch_framework_;

};  // namespace LineairDB

// Database::Impl* Database::Impl::CurrentDBInstance = nullptr;

}  // namespace LineairDB
#endif /** LINEAIRDB_DATABASE_IMPL_H **/
