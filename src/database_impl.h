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
#include <table/table.h>

#include <functional>
#include <shared_mutex>
#include <tuple>
#include <utility>

#include "callback/callback_manager.h"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.h"
#include "table/table.h"
#include "table/table_dictionary.hpp"
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
        checkpoint_manager_(config_, table_dictionary_, epoch_framework_) {
    if (Database::Impl::CurrentDBInstance == nullptr) {
      Database::Impl::CurrentDBInstance = this;
      SPDLOG_INFO("LineairDB instance has been constructed.");
    } else {
      SPDLOG_ERROR(
          "It is prohibited to allocate two LineairDB::Database instance at "
          "the same time.");
      exit(EXIT_FAILURE);
    }
    if (!config_.anonymous_table_name.empty()) {
      CreateTable(config_.anonymous_table_name);
    } else {
      SPDLOG_ERROR("Anonymous table name is not set.");
      exit(EXIT_FAILURE);
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
   * Ensures that (1) all pending operations are completed, (2) all callbacks
   * have been executed, and (3) all index updates have been fully applied and
   * are visible to subsequent operations.
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
    // Wait for all index updates to be linearizable
    // This ensures that all insertions/deletions are visible in the index
    table_dictionary_.ForEachTable(
        [](Table& table) { table.WaitForIndexIsLinearizable(); });
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
    return table_dictionary_.CreateTable(table_name, epoch_framework_, config_);
  }

  bool CreateSecondaryIndex(const std::string_view table_name,
                            const std::string_view index_name,
                            const uint index_type) {
    std::shared_lock<std::shared_mutex> lk(schema_mutex_);
    auto it = GetTable(table_name);
    if (!it.has_value()) {
      return false;
    }
    return it.value()->CreateSecondaryIndex(index_name, index_type);
  }

  std::optional<Table*> GetTable(const std::string_view table_name) {
    return table_dictionary_.GetTable(table_name);
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
    auto&& recovery_sets = logger_.GetRecoverySetFromLogs(durable_epoch);

    for (auto& recovery_set : recovery_sets) {
      // Skip deleted entries (tombstones with size=0)
      if (!recovery_set.data_item_copy.IsInitialized()) continue;
      CreateTable(recovery_set.table_name);
      auto table = GetTable(recovery_set.table_name);
      if (!table.has_value()) {
        SPDLOG_CRITICAL(
            "Recovery failed: Table {0} could not be found or created.",
            recovery_set.table_name);
        exit(EXIT_FAILURE);
      }

      highest_epoch =
          std::max(highest_epoch,
                   recovery_set.data_item_copy.transaction_id.load().epoch);

      if (!recovery_set.index_name.empty()) {
        // Temporary: skip recovery for secondary index entries.
        continue;
      }
      // Primary Index recovery
      table.value()->GetPrimaryIndex().Put(
          recovery_set.key, std::move(recovery_set.data_item_copy));
#if 0
      // Secondary Index recovery (disabled temporarily)
      Index::SecondaryIndex* idx = nullptr;
      table.value()->GetOrCreateSecondaryIndex(recovery_set.index_name, &idx);
      if (idx != nullptr) {
        idx->Put(recovery_set.key, std::move(recovery_set.data_item_copy));
        SPDLOG_DEBUG(
            "  Recovery: Secondary index '{0}' restored key '{1}' with {2} "
            "primary keys",
            recovery_set.index_name, recovery_set.key,
            recovery_set.data_item_copy.primary_keys.size());
      } else {
        SPDLOG_ERROR(
            "Recovery failed: Could not create secondary index {0} for "
            "table {1}",
            recovery_set.index_name, recovery_set.table_name);
      }
#endif
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
  TableDictionary table_dictionary_;
  std::atomic<EpochNumber> latest_callbacked_epoch_{1};
  Recovery::CPRManager checkpoint_manager_;
  mutable std::shared_mutex schema_mutex_;
};

}  // namespace LineairDB
#endif /** LINEAIRDB_DATABASE_IMPL_H **/
