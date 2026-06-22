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
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "test_helper.hpp"
#include "util/logger.hpp"

class DurabilityTest
    : public ::testing::TestWithParam<LineairDB::Config::ConcurrencyControl> {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    config_.work_dir =
        "lineairdb_logs_" + std::to_string(reinterpret_cast<uintptr_t>(this));
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread = 4;
    config_.concurrency_control_protocol = GetParam();
    config_.enable_logging = true;
    config_.enable_recovery = true;
    config_.enable_checkpointing = false;
    config_.checkpoint_period = 10;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
  virtual void TearDown() {
    db_.reset(nullptr);
    std::filesystem::remove_all(config_.work_dir);
  }
};

TEST_P(DurabilityTest, Recovery) {
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

TEST_P(DurabilityTest, RecoveryKeepsDeletedKeysAbsent) {
  // We expect LineairDB enables recovery logging by default.
  const LineairDB::Config config = db_->GetConfig();
  ASSERT_TRUE(config.enable_logging);

  int initial_value = 1;
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               tx.Write<int>("alice", initial_value);
                             }});
  db_->Fence();

  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) { tx.Delete("alice"); }});
  db_->Fence();

  // Expect that recovery procedure has idempotence
  for (size_t i = 0; i < 3; i++) {
    db_.reset(nullptr);
    db_ = std::make_unique<LineairDB::Database>(config);

    TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                                 auto alice = tx.Read<int>("alice");
                                 ASSERT_FALSE(alice.has_value());
                               }});
  }
}

TEST_P(DurabilityTest, RecoveryKeepsDeletedKeysAbsentEvenWithCheckpoint) {
  LineairDB::Config config = db_->GetConfig();
  config.enable_checkpointing = true;
  config.checkpoint_period = 1;
  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<LineairDB::Database>(config);
  ASSERT_TRUE(config.enable_logging);

  int initial_value = 1;
  TestHelper::DoTransactions(
      db_.get(), {{[&](LineairDB::Transaction& tx) {
                    tx.Write<int>("alice", initial_value);
                  }},
                  {[&](LineairDB::Transaction& tx) { tx.Delete("alice"); }}});

  // Wait for checkpoint to be created.
  // The checkpoint algorithm should ignore deleted keys to be recovered.
  std::this_thread::sleep_for(
      std::chrono::seconds(config.checkpoint_period + 5));

  // Expect that recovery procedure has idempotence
  for (size_t i = 0; i < 3; i++) {
    db_.reset(nullptr);
    db_ = std::make_unique<LineairDB::Database>(config);

    TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                                 auto alice = tx.Read<int>("alice");
                                 ASSERT_FALSE(alice.has_value());
                               }});
  }
}

TEST_P(DurabilityTest, RecoveryLargeObject) {
  std::string initial_value(4096, 'a');
  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        tx.Write("alice", reinterpret_cast<std::byte*>(initial_value.data()),
                 initial_value.size());
      }});
  db_->Fence();

  for (size_t i = 0; i < 3; i++) {
    TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                                 auto alice = tx.Read("alice");
                                 ASSERT_TRUE(alice.first != nullptr);
                                 std::string current_value(
                                     reinterpret_cast<const char*>(alice.first),
                                     alice.second);
                                 ASSERT_EQ(initial_value, current_value);
                               }});
  }
}

TEST_P(DurabilityTest, RecoveryInContendedWorkload) {
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

TEST_P(DurabilityTest, RecoveryWithHandlerInterface) {
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

size_t getLogDirectorySize(const LineairDB::Config& conf) {
  namespace fs = std::filesystem;
  size_t size = 0;
  for (const auto& entry : fs::directory_iterator(conf.work_dir)) {
    if (entry.path().filename().generic_string().find("working") !=
        std::string::npos)
      continue;
    size += fs::file_size(entry.path());
  }
  return size;
}

TEST_P(DurabilityTest, LogFileSizeIsBounded) {  // a.k.a., checkpointing
  LineairDB::Config config = db_->GetConfig();
  config.enable_checkpointing = true;
  config.checkpoint_period = 1;
  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<LineairDB::Database>(config);
  ASSERT_TRUE(config.enable_logging);
  ASSERT_TRUE(config.enable_checkpointing);

  TransactionProcedure Update([](LineairDB::Transaction& tx) {
    int value = 0xBEEF;
    tx.Write<int>("alice", value);
  });

  size_t filesize = 0;
  ASSERT_EQ(filesize, getLogDirectorySize(config));
  bool filesize_is_monotonically_increasing = true;

  auto begin = std::chrono::high_resolution_clock::now();

  for (;;) {
    const size_t current_file_size = getLogDirectorySize(config);
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
    if (config.checkpoint_period * 10 < elapsed) break;
  }
  ASSERT_FALSE(filesize_is_monotonically_increasing);
}

TEST_P(DurabilityTest,
       LogFileSizeIsBoundedOnHandlerInterface) {  // a.k.a., checkpointing
  LineairDB::Config config = db_->GetConfig();
  config.enable_checkpointing = true;
  config.checkpoint_period = 1;
  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<LineairDB::Database>(config);
  ASSERT_TRUE(config.enable_logging);
  ASSERT_TRUE(config.enable_checkpointing);

  TransactionProcedure Update([](LineairDB::Transaction& tx) {
    int value = 0xBEEF;
    tx.Write<int>("alice", value);
  });

  size_t filesize = 0;
  ASSERT_EQ(filesize, getLogDirectorySize(config));
  bool filesize_is_monotonically_increasing = true;

  auto begin = std::chrono::high_resolution_clock::now();

  std::atomic<bool> stop(false);
  std::thread worker_thread([&]() {
    for (;;) {
      auto& tx = db_->BeginTransaction();
      [[maybe_unused]] auto alice = tx.Read<int>("alice");
      int value = 0xBEEF;
      tx.Write<int>("alice", value);
      db_->EndTransaction(tx, [](auto) {});
      if (stop.load()) return;
      std::this_thread::sleep_for(std::chrono::nanoseconds(1));
    }
  });

  for (;;) {
    const size_t current_file_size = getLogDirectorySize(config);
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
    if (config.checkpoint_period * 10 < elapsed) break;
  }
  stop.store(true);
  worker_thread.join();
  ASSERT_FALSE(filesize_is_monotonically_increasing);
}

TEST_P(DurabilityTest, CPRConsistency) {  // a.k.a., checkpointing
  /**
   * CPR Consistency:
   * > Definition 1 (CPR Consistency). A database state is CPR consistent if and
   * > only if, for every client $C$ , the state contains all its transactions
   * > committed before a unique client-local time-point $tC$ , and none after.
   * Ref:
   * https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf
   *
   * i.e., There exists the guarantee of consistent snapshot at some time point.
   */

  LineairDB::Config config = db_->GetConfig();
  config.enable_logging = false;
  config.enable_checkpointing = true;
  config.checkpoint_period =
      2;  // 2sec (avoid premature checkpointing before destruction)
  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<LineairDB::Database>(config);

  TransactionProcedure Update([](LineairDB::Transaction& tx) {
    int value = 0;
    tx.Write<int>("alice", value);
  });

  std::atomic<bool> precommitted(false);
  db_->ExecuteTransaction(
      Update, [](const auto) {},
      [&](const auto status) {
        if (status == LineairDB::TxStatus::Committed) {
          precommitted.store(true);
        }
      });
  while (!precommitted.load()) {
    std::this_thread::yield();
  }

  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  // We assume that DB has been destructed within 5 seconds and there are no
  // consistent snapshots.
  {
    std::atomic<bool> read_done(false);
    std::optional<int> alice_val;
    db_->ExecuteTransaction(
        [&](LineairDB::Transaction& tx) { alice_val = tx.Read<int>("alice"); },
        [](const auto) {}, [&](const auto) { read_done.store(true); });
    while (!read_done.load()) {
      std::this_thread::yield();
    }
    ASSERT_FALSE(alice_val.has_value());
  }

  precommitted.store(false);
  db_->ExecuteTransaction(
      Update, [](const auto) {},
      [&](const auto status) {
        if (status == LineairDB::TxStatus::Committed) {
          precommitted.store(true);
        }
      });
  while (!precommitted.load()) {
    std::this_thread::yield();
  }
  db_->WaitForCheckpoint();

  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  {
    std::atomic<bool> read_done(false);
    std::optional<int> alice_val;
    db_->ExecuteTransaction(
        [&](LineairDB::Transaction& tx) { alice_val = tx.Read<int>("alice"); },
        [](const auto) {}, [&](const auto) { read_done.store(true); });
    while (!read_done.load()) {
      std::this_thread::yield();
    }
    ASSERT_TRUE(alice_val.has_value());
  }
}

TEST_P(DurabilityTest,
       CPRConsistencyOnHandlerInterface) {  // a.k.a., checkpointing
  LineairDB::Config config = db_->GetConfig();
  config.enable_logging = false;
  config.enable_checkpointing = true;
  config.checkpoint_period =
      2;  // 2sec (avoid premature checkpointing before destruction)
  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<LineairDB::Database>(config);

  TransactionProcedure Update([](LineairDB::Transaction& tx) {
    int value = 0;
    tx.Write<int>("alice", value);
  });

  {
    auto& tx = db_->BeginTransaction();
    Update(tx);
    bool committed = db_->EndTransaction(tx, [](const auto) {});
    ASSERT_TRUE(committed);
  }
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  // We assume that DB has been destructed within 5 seconds and there are no
  // consistent snapshots.
  {
    auto& tx = db_->BeginTransaction();
    auto alice = tx.Read<int>("alice");
    ASSERT_FALSE(alice.has_value());
    db_->EndTransaction(tx, [](const auto) {});
  }

  {
    auto& tx = db_->BeginTransaction();
    Update(tx);
    bool committed = db_->EndTransaction(tx, [](const auto) {});
    ASSERT_TRUE(committed);
  }
  db_->WaitForCheckpoint();

  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);
  {
    auto& tx = db_->BeginTransaction();
    auto alice = tx.Read<int>("alice");
    ASSERT_TRUE(alice.has_value());
    db_->EndTransaction(tx, [](const auto) {});
  }
}

TEST_P(DurabilityTest, RecoveryWithNamedTable) {
  const LineairDB::Config config = db_->GetConfig();
  const std::string table_name = "users";
  const std::string key = "user1";
  const int value = 12345;

  // 1. Create a table and write to it
  db_->CreateTable(table_name);
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               bool success = tx.SetTable(table_name);
                               ASSERT_TRUE(success);
                               tx.Write<int>(key, value);
                             }});
  db_->Fence();

  // 2. Restart DB to trigger recovery
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  // 3. Verify data is recovered in the correct table
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               bool success = tx.SetTable(table_name);
                               ASSERT_TRUE(success);
                               auto data = tx.Read<int>(key);
                               ASSERT_TRUE(data.has_value());
                               ASSERT_EQ(data.value(), value);
                             }});

  // 4. Verify data is NOT in the anonymous table
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               // Note: Not calling SetTable, so this reads from
                               // the anonymous table
                               auto data = tx.Read<int>(key);
                               ASSERT_FALSE(data.has_value());
                             }});
}

TEST_P(DurabilityTest, TwoPhaseLockingDestructorGracefulShutdown) {
  LineairDB::Config config = db_->GetConfig();
  config.concurrency_control_protocol =
      LineairDB::Config::ConcurrencyControl::TwoPhaseLocking;
  config.enable_checkpointing = true;
  config.checkpoint_period = 1;  // 1sec
  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<LineairDB::Database>(config);

  // Execute continuous write transactions to the same key from a background
  // thread to ensure concurrent checkpointing conflicts with 2PL lock updates.
  std::atomic<bool> stop(false);
  std::thread writer([&]() {
    int i = 0;
    while (!stop.load()) {
      auto& tx = db_->BeginTransaction();
      tx.Write<int>("key", i++);
      db_->EndTransaction(tx, [](auto) {});
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  stop.store(true);
  writer.join();

  // Destruct the database. If there's a deadlock/hang, the test will hang here.
  db_.reset(nullptr);
}

TEST_P(DurabilityTest, ShortLivedThreadsDoNotBlockEpochAdvancement) {
  // Spawn a transaction in a short-lived thread which then exits.
  // We use handler interface directly on t1 to register t1 in thread-local
  // storage of ThreadLocalLogger.
  std::thread t1([&]() {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("key_from_t1", 42);
    db_->EndTransaction(tx, [](auto) {});
  });
  t1.join();

  // Execute a transaction on the main thread.
  auto& tx = db_->BeginTransaction();
  tx.Write<int>("key_from_main", 100);
  db_->EndTransaction(tx, [](auto) {});

  // Call Fence. If the exited thread's local storage is not offline,
  // the min durable epoch will be stuck at E_t1, causing Fence() to
  // hang/deadlock.
  db_->Fence();
}

INSTANTIATE_TEST_SUITE_P(
    ConcurrencyControlProtocols, DurabilityTest,
    ::testing::Values(LineairDB::Config::ConcurrencyControl::SiloNWR,
                      LineairDB::Config::ConcurrencyControl::TwoPhaseLocking));
