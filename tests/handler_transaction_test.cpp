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

class HandlerTransactionTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::experimental::filesystem::remove_all("lineairdb_logs");
    config_.max_thread = 4;
    db_                = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(HandlerTransactionTest, ExecuteTransaction) {
  int value_of_alice = 1;
  auto* db           = db_.get();
  {
    auto& tx = db->BeginTransaction();
    tx.Write("alice", reinterpret_cast<std::byte*>(&value_of_alice),
             sizeof(int));
    db->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(LineairDB::TxStatus::Committed, status);
    });
  }
  db->Fence();
  {
    auto& tx   = db->BeginTransaction();
    auto alice = tx.Read("alice");
    ASSERT_NE(alice.first, nullptr);
    ASSERT_EQ(value_of_alice, *reinterpret_cast<const int*>(alice.first));

    db->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(LineairDB::TxStatus::Committed, status);
    });
  }
}

TEST_F(HandlerTransactionTest, ExecuteTransactionWithTemplates) {
  int value_of_alice = 1;
  auto* db           = db_.get();
  {
    auto& tx = db->BeginTransaction();
    tx.Write<int>("alice", value_of_alice);
    db->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(LineairDB::TxStatus::Committed, status);
    });
  }
  db->Fence();
  {
    auto& tx   = db->BeginTransaction();
    auto alice = tx.Read<int>("alice");
    ASSERT_TRUE(alice.has_value());
    ASSERT_EQ(value_of_alice, alice.value());
    db->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(LineairDB::TxStatus::Committed, status);
    });
  }
}

TEST_F(HandlerTransactionTest, UserAbort) {
  auto* db = db_.get();
  {
    auto& tx = db->BeginTransaction();
    tx.Abort();
    db->EndTransaction(tx, [&](auto status) {
      ASSERT_EQ(LineairDB::TxStatus::Aborted, status);
    });
  }
}
