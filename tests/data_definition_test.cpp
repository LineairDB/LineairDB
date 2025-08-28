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

#include <array>
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/transaction.h"
#include "lineairdb/tx_status.h"
#include "test_helper.hpp"

class DataDefinitionTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread = 4;
    config_.checkpoint_period = 1;
    config_.epoch_duration_ms = 100;
    db_.reset(nullptr);
    db_ = std::make_unique<LineairDB::Database>();
  }
};

TEST_F(DataDefinitionTest, CreateTable) {
  bool success = db_->CreateTable("users");
  ASSERT_TRUE(success);
  bool duplicated = db_->CreateTable("users");
  ASSERT_FALSE(duplicated);
}

TEST_F(DataDefinitionTest, SetTable) {
  ASSERT_TRUE(db_->CreateTable("users"));
  {
    auto& tx = db_->BeginTransaction();
    bool table_exists = tx.SetTable("users");
    ASSERT_TRUE(table_exists);

    bool non_existent_table = tx.SetTable("non_existent");
    ASSERT_FALSE(non_existent_table);
    db_->EndTransaction(tx, [](auto status) {
      // Table not found is not a fatal error, so we expect the transaction to
      // commit
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(DataDefinitionTest, ReadWrite) {
  db_->CreateTable("users");

  {
    auto& tx = db_->BeginTransaction();
    bool table_exists = tx.SetTable("users");
    ASSERT_TRUE(table_exists);
    tx.Write<int>("user1", 42);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
  db_->Fence();

  {
    auto& tx = db_->BeginTransaction();
    bool table_exists = tx.SetTable("users");
    ASSERT_TRUE(table_exists);
    auto data = tx.Read<int>("user1");
    ASSERT_TRUE(data.has_value());
    ASSERT_EQ(data.value(), 42);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
  db_->Fence();
}

TEST_F(DataDefinitionTest, ConcurrencyControlBetweenMultipleTables) {
  db_->CreateTable("users");
  db_->CreateTable("accounts");

  std::atomic<bool> tx1_ready = false;
  std::atomic<bool> tx2_ready = false;

  std::thread thread1([&]() {
    auto& tx1 = db_->BeginTransaction();
    ASSERT_TRUE(tx1.SetTable("users"));

    tx1.Write<int>("user1", 42);
    tx1_ready = true;
    while (!tx2_ready) std::this_thread::yield();  // Wait for tx2 to be ready
    db_->EndTransaction(tx1, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  });

  std::thread thread2([&]() {
    auto& tx2 = db_->BeginTransaction();
    ASSERT_TRUE(tx2.SetTable("accounts"));

    tx2.Write<int>("user1", 100);
    tx2_ready = true;
    while (!tx1_ready) std::this_thread::yield();  // Wait for tx1 to be ready
    db_->EndTransaction(tx2, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  });

  thread1.join();
  thread2.join();

  db_->Fence();

  // Check Results
  {
    auto& tx = db_->BeginTransaction();
    ASSERT_TRUE(tx.SetTable("users"));
    auto data = tx.Read<int>("user1");
    ASSERT_TRUE(data.has_value());
    ASSERT_EQ(data.value(), 42);

    ASSERT_TRUE(tx.SetTable("accounts"));
    data = tx.Read<int>("user1");
    ASSERT_TRUE(data.has_value());
    ASSERT_EQ(data.value(), 100);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(DataDefinitionTest, SetTableAfterWrite) {
  db_->CreateTable("users");
  db_->CreateTable("accounts");

  {
    auto& tx = db_->BeginTransaction();
    ASSERT_TRUE(tx.SetTable("users"));
    tx.Write<int>("user1", 42);

    // Now switch to another table
    ASSERT_TRUE(tx.SetTable("accounts"));
    tx.Write<int>("user1", 100);

    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
  db_->Fence();

  // Check Results
  {
    auto& tx = db_->BeginTransaction();
    ASSERT_TRUE(tx.SetTable("users"));
    auto data = tx.Read<int>("user1");
    ASSERT_TRUE(data.has_value());
    ASSERT_EQ(data.value(), 42);

    ASSERT_TRUE(tx.SetTable("accounts"));
    data = tx.Read<int>("user1");
    ASSERT_TRUE(data.has_value());
    ASSERT_EQ(data.value(), 100);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}