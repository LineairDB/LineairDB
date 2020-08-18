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

class DurabilityTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::experimental::filesystem::remove_all("lineairdb_logs");
    config_.max_thread = 4;
    db_                = std::make_unique<LineairDB::Database>(config_);
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