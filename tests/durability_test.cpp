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

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <atomic>
#include <chrono>
#include <experimental/filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "test_helper.hpp"
#include "util/logger.hpp"

class DurabilityTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::experimental::filesystem::remove_all("lineairdb_logs");
    config_.max_thread           = 4;
    config_.enable_logging       = true;
    config_.enable_recovery      = true;
    config_.enable_checkpointing = true;
    config_.checkpoint_period    = 1;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(DurabilityTest, Recovery) {
  // We expect LineairDB enables recovery logging by default.
  const LineairDB::Config config = db_->GetConfig();
  ASSERT_TRUE(config.enable_logging);

  int initial_value = 1;
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                                           tx.Write<int>("alice",
                                                         initial_value);
                                         },
                                         [&](LineairDB::Transaction& tx) {
                                           tx.Write<int>("bob", initial_value);
                                         }});
  db_->Fence();

  // Expect that recovery procedure has idempotence
  for (size_t i = 0; i < 3; i++) {
    db_.reset(nullptr);
    db_ = std::make_unique<LineairDB::Database>(config);

    TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                                 auto alice = tx.Read<int>("alice");
                                 ASSERT_TRUE(alice.has_value());
                                 auto current_value = alice.value();
                                 ASSERT_EQ(initial_value, current_value);
                                 auto bob = tx.Read<int>("bob");
                                 ASSERT_TRUE(bob.has_value());
                                 current_value = bob.value();
                                 ASSERT_EQ(initial_value, current_value);
                               }});
  }
}

TEST_F(DurabilityTest, RecoveryInContendedWorkload) {
  // We expect LineairDB enables recovery logging by default.
  const LineairDB::Config config = db_->GetConfig();
  ASSERT_TRUE(config.enable_logging);

  TransactionProcedure Update([](LineairDB::Transaction& tx) {
    int value = 0xBEEF;
    tx.Write<int>("alice", value);
  });

  ASSERT_NO_THROW({
    TestHelper::DoTransactionsOnMultiThreads(db_.get(),
                                             {Update, Update, Update});
  });
  db_->Fence();

  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               auto alice = tx.Read<int>("alice");
                               ASSERT_TRUE(alice.has_value());
                               auto current_value = alice.value();
                               ASSERT_EQ(0xBEEF, current_value);
                             }});
}

TEST_F(DurabilityTest, RecoveryWithHandlerInterface) {
  const LineairDB::Config config = db_->GetConfig();
  ASSERT_TRUE(config.enable_logging);

  TransactionProcedure Update([](LineairDB::Transaction& tx) {
    int value = 0xBEEF;
    tx.Write<int>("alice", value);
  });

  ASSERT_NO_THROW({
    TestHelper::DoHandlerTransactionsOnMultiThreads(db_.get(),
                                                    {Update, Update, Update});
  });
  db_->Fence();

  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               auto alice = tx.Read<int>("alice");
                               ASSERT_TRUE(alice.has_value());
                               auto current_value = alice.value();
                               ASSERT_EQ(0xBEEF, current_value);
                             }});
}

size_t getLogDirectorySize() {
  namespace fs = std::experimental::filesystem;
  size_t size  = 0;
  for (const auto& entry : fs::directory_iterator("lineairdb_logs")) {
    if (entry.path().filename() == "durable_epoch_working.json") continue;
    size += fs::file_size(entry.path());
  }
  return size;
}

TEST_F(DurabilityTest, LogFileSizeIsBounded) {  // a.k.a., checkpointing
  const LineairDB::Config config = db_->GetConfig();
  ASSERT_TRUE(config.enable_logging);
  ASSERT_TRUE(config.enable_checkpointing);

  TransactionProcedure Update([](LineairDB::Transaction& tx) {
    int value = 0xBEEF;
    tx.Write<int>("alice", value);
  });

  std::experimental::filesystem::path log_path = "lineairdb_logs";
  size_t filesize                              = 0;
  ASSERT_EQ(filesize, getLogDirectorySize());
  bool filesize_is_monotonically_increasing = true;

  auto begin = std::chrono::high_resolution_clock::now();

  for (;;) {
    const size_t current_file_size = getLogDirectorySize();
    if (filesize <= current_file_size) {
      filesize = current_file_size;
    } else {
      filesize_is_monotonically_increasing = false;
      break;
    }
    ASSERT_NO_THROW({
      TestHelper::DoTransactions(db_.get(), {Update, Update, Update});
    });

    auto now = std::chrono::high_resolution_clock::now();
    assert(begin < now);
    size_t elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(now - begin).count();
    if (config.checkpoint_period * 2 < elapsed) break;
  }
  ASSERT_FALSE(filesize_is_monotonically_increasing);
}

TEST_F(DurabilityTest,
       LogFileSizeIsBoundedOnHandlerInterface) {  // a.k.a., checkpointing
  const LineairDB::Config config = db_->GetConfig();
  ASSERT_TRUE(config.enable_logging);
  ASSERT_TRUE(config.enable_checkpointing);

  TransactionProcedure Update([](LineairDB::Transaction& tx) {
    int value = 0xBEEF;
    tx.Write<int>("alice", value);
  });

  std::experimental::filesystem::path log_path = "lineairdb_logs";
  size_t filesize                              = 0;
  ASSERT_EQ(filesize, getLogDirectorySize());
  bool filesize_is_monotonically_increasing = true;

  auto begin = std::chrono::high_resolution_clock::now();

  std::atomic<bool> stop(false);
  std::thread worker_thread([&]() {
    for (;;) {
      auto& tx                    = db_->BeginTransaction();
      int value                   = 0xBEEF;
      [[maybe_unused]] auto alice = tx.Read<int>("alice");
      tx.Write<int>("alice", value);
      db_->EndTransaction(tx, [](auto) {});
      if (stop.load()) break;
    }
  });

  for (;;) {
    const size_t current_file_size = getLogDirectorySize();
    if (filesize <= current_file_size) {
      filesize = current_file_size;
    } else {
      filesize_is_monotonically_increasing = false;
      break;
    }

    auto now = std::chrono::high_resolution_clock::now();
    assert(begin < now);
    size_t elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(now - begin).count();
    if (config.checkpoint_period * 3 < elapsed) break;
  }
  stop.store(true);
  worker_thread.join();
  ASSERT_FALSE(filesize_is_monotonically_increasing);
}

TEST_F(DurabilityTest, CPRConsistency) {  // a.k.a., checkpointing
  /**
   * CPR Consistency:
   * Definition 1 (CPR Consistency). A database state is CPR consistent if and
   * only if, for every client $C$ , the state contains all its transactions
   * committed before a unique client-local time-point $tC$ , and none after.
   * Ref:
   * https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf
   *
   * i.e., There exists the guarantee of consistent snapshot at some time point.
   */

  LineairDB::Config config = db_->GetConfig();
  config.enable_logging    = false;
  config.checkpoint_period = 5;  // 5sec
  ASSERT_TRUE(config.enable_checkpointing);
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  TransactionProcedure Update([](LineairDB::Transaction& tx) {
    int value = 0;
    tx.Write<int>("alice", value);
  });

  TestHelper::DoTransactions(db_.get(), {Update});
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  // We assume that DB has been destructed within 5 seconds and there are no
  // consisntent snapshot
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               auto alice = tx.Read<int>("alice");
                               ASSERT_FALSE(alice.has_value());
                             }});

  TestHelper::DoTransactions(db_.get(), {Update});
  std::this_thread::sleep_for(
      std::chrono::seconds(config.checkpoint_period * 2));

  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               auto alice = tx.Read<int>("alice");
                               ASSERT_TRUE(alice.has_value());
                             }});
}

TEST_F(DurabilityTest,
       CPRConsistencyOnHandlerInterface) {  // a.k.a., checkpointing
  LineairDB::Config config = db_->GetConfig();
  config.enable_logging    = false;
  config.checkpoint_period = 5;  // 5sec
  ASSERT_TRUE(config.enable_checkpointing);
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  TransactionProcedure Update([](LineairDB::Transaction& tx) {
    int value = 0;
    tx.Write<int>("alice", value);
  });

  TestHelper::DoHandlerTransactionsOnMultiThreads(db_.get(), {Update});
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  // We assume that DB has been destructed within 5 seconds and there are no
  // consisntent snapshot
  TestHelper::DoHandlerTransactionsOnMultiThreads(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        auto alice = tx.Read<int>("alice");
        ASSERT_FALSE(alice.has_value());
      }});

  TestHelper::DoHandlerTransactionsOnMultiThreads(db_.get(), {Update});
  std::this_thread::sleep_for(
      std::chrono::seconds(config.checkpoint_period * 2));

  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);
  TestHelper::DoHandlerTransactionsOnMultiThreads(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        auto alice = tx.Read<int>("alice");
        ASSERT_TRUE(alice.has_value());
      }});
}