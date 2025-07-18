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
class DatabaseTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread = 4;
    config_.checkpoint_period = 1;
    config_.epoch_duration_ms = 100;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(DatabaseTest, CreateTable) {
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>();
  bool success = db_->CreateTable("users");
  ASSERT_TRUE(success);
  bool duplicated = db_->CreateTable("users");
  ASSERT_FALSE(duplicated);
}

// CreateSecondaryIndex("table_name", "index_name", "UNIQUE")
// For now, we only support UNIQUE constraint, but maybe we will support more
// constraints in the future, so we will use enum class Constraint.
using Constraint = LineairDB::SecondaryIndexOption::Constraint;

TEST_F(DatabaseTest, CreateSecondaryIndexWithoutConstraints) {
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "nickname"));
}

TEST_F(DatabaseTest, CreateSecondaryIndexWithoutConstraintsTimeKey) {
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::time_t>("users", "created_at"));
}

TEST_F(DatabaseTest, CreateSecondaryIndexWithoutConstraintsStringKey) {
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "name"));
}

// UNIQUE only
TEST_F(DatabaseTest, CreateSecondaryIndexUniqueOnlyIntKey) {
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex<int>("users", "age", Constraint::UNIQUE));
}

TEST_F(DatabaseTest, CreateSecondaryIndexUniqueOnlyStringKey) {
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "name",
                                                     Constraint::UNIQUE));
}

TEST_F(DatabaseTest, CreateSecondaryIndexUniqueOnlyTimeKey) {
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::time_t>("users", "created_at",
                                                     Constraint::UNIQUE));
}

// ---------------- Variant key type insertion tests ----------------

TEST_F(DatabaseTest, InsertSecondaryIndexStringKey) {
  db_ = std::make_unique<LineairDB::Database>();
  db_->CreateTable("users");
  db_->CreateSecondaryIndex<std::string>("users", "email");

  // Write & Verify string secondary key

  auto& tx = db_->BeginTransaction();
  int age = 42;
  tx.WritePrimaryIndex<int>("users", "user#1", age);
  tx.WriteSecondaryIndex<std::string>("users", "email", "alice@example.com",
                                      "user#1");
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
}

TEST_F(DatabaseTest, InsertSecondaryIndexStringKeyWithTypeMismatch) {
  db_ = std::make_unique<LineairDB::Database>();
  db_->CreateTable("users");
  db_->CreateSecondaryIndex<std::string>("users", "email");

  // Write & Verify string secondary key

  auto& tx = db_->BeginTransaction();
  int age = 42;
  tx.WritePrimaryIndex<int>("users", "user#1", age);
  tx.WriteSecondaryIndex<std::string>("users", "email", 42,
                                      "user#1");
  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}


TEST_F(DatabaseTest, InsertSecondaryIndexIntKey) {
  db_ = std::make_unique<LineairDB::Database>();
  db_->CreateTable("users");
  db_->CreateSecondaryIndex<int>("users", "age");

  // Write & Verify int secondary key

  auto& tx = db_->BeginTransaction();
  int age = 24;
  tx.WritePrimaryIndex<int>("users", "user#2", age);
  tx.WriteSecondaryIndex<int>("users", "age", 20, "user#2");
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
}

TEST_F(DatabaseTest, InsertSecondaryIndexIntKeyWithTypeMismatch) {
  db_ = std::make_unique<LineairDB::Database>();
  db_->CreateTable("users");
  db_->CreateSecondaryIndex<int>("users", "age");

  // Write & Verify int secondary key

  auto& tx = db_->BeginTransaction();
  tx.WriteSecondaryIndex<int>("users", "age", "alice@example.com", "user#2");
  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));

}

TEST_F(DatabaseTest, InsertSecondaryIndexTimeKey) {
  db_ = std::make_unique<LineairDB::Database>();
  db_->CreateTable("users");
  db_->CreateSecondaryIndex<std::time_t>("users", "created_at");

  auto ts = std::time(nullptr);

  // Write & Verify time_t secondary key

  auto& tx = db_->BeginTransaction();
  tx.WriteSecondaryIndex<std::time_t>("users", "created_at", ts, "user#3");
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
}

TEST_F(DatabaseTest, InsertSecondaryIndexTimeKeyWithTypeMismatch) {

  db_ = std::make_unique<LineairDB::Database>();
  db_->CreateTable("users");
  db_->CreateSecondaryIndex<std::time_t>("users", "created_at");

  auto ts = std::time(nullptr);

  // Write & Verify time_t secondary key

  auto& tx = db_->BeginTransaction();
  tx.WriteSecondaryIndex<std::time_t>("users", "created_at", "alice@example.com", "user#3");
  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}

// ---------------- Variant key type read tests ----------------
TEST_F(DatabaseTest, ReadSecondaryIndexStringKey) {
  db_ = std::make_unique<LineairDB::Database>();
  db_->CreateTable("users");
  db_->CreateSecondaryIndex<std::string>("users", "email");

  // Write & Verify string secondary key
  auto& tx = db_->BeginTransaction();
  int age = 42;
  tx.WritePrimaryIndex<int>("users", "user#1", age);
  tx.WriteSecondaryIndex<std::string>("users", "email", "alice@example.com",
                                      "user#1");

  auto& rtx = db_->BeginTransaction();
  auto pk = rtx.ReadSecondaryIndex<std::string>("users", "email",
                                                "alice@example.com");
  auto val = rtx.ReadPrimaryIndex<int>("users", pk.value());
  ASSERT_EQ(val.value(), 42);
  db_->EndTransaction(rtx, [](auto) {});
}

TEST_F(DatabaseTest, ReadSecondaryIndexIntKey) {
  db_ = std::make_unique<LineairDB::Database>();
  db_->CreateTable("users");
  db_->CreateSecondaryIndex<int>("users", "age");

  // Write & Verify int secondary key

  auto& tx = db_->BeginTransaction();
  int age = 24;
  tx.WritePrimaryIndex<int>("users", "user#2", age);
  tx.WriteSecondaryIndex<int>("users", "age", 20, "user#2");

  auto& rtx = db_->BeginTransaction();
  auto pk = rtx.ReadSecondaryIndex<int>("users", "age", 20);
  auto val = rtx.ReadPrimaryIndex<int>("users", pk.value());
  ASSERT_EQ(val.value(), 24);
  db_->EndTransaction(rtx, [](auto) {});
}

TEST_F(DatabaseTest, ReadSecondaryIndexTimeKey) {
  db_ = std::make_unique<LineairDB::Database>();
  db_->CreateTable("users");
  db_->CreateSecondaryIndex<std::time_t>("users", "created_at");

  auto ts = std::time(nullptr);

  // Write & Verify time_t secondary key

  auto& tx = db_->BeginTransaction();
  int age = 30;
  tx.WritePrimaryIndex<int>("users", "user#3", age);
  tx.WriteSecondaryIndex<std::time_t>("users", "created_at", ts, "user#3");

  auto& rtx = db_->BeginTransaction();
  auto pk = rtx.ReadSecondaryIndex<std::time_t>("users", "created_at", ts);
  auto val = rtx.ReadPrimaryIndex<int>("users", pk.value());
  ASSERT_EQ(val.value(), 30);
  db_->EndTransaction(rtx, [](auto) {});
}

// [Secondary Index Constraint Enforcement Tests] ----------------------------
// UNIQUE constraint: insertion of a duplicate value should abort the txn
TEST_F(DatabaseTest, InsertDuplicateSecondaryKeyViolatesUnique) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "email",
                                                     Constraint::UNIQUE));

  // 1st row – this should commit.
  {
    auto& tx = db_->BeginTransaction();
    int age = 42;
    tx.WritePrimaryIndex<int>("users", "user#1", age);
    tx.WriteSecondaryIndex<std::string>("users", "email", "bob@example.com",
                                        "user#1");
    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  // 2nd row with the same email – should abort.
  {
    auto& tx = db_->BeginTransaction();
    int age = 24;
    tx.WritePrimaryIndex<int>("users", "user#2", age);
    tx.WriteSecondaryIndex<std::string>("users", "email", "bob@example.com",
                                        "user#2");  // duplicate key
    ASSERT_FALSE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
  }
}

// WriteSecondaryIndex to unregistered index should abort
TEST_F(DatabaseTest, WriteSecondaryIndexToUnregisteredIndexShouldAbort) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));

  auto& tx = db_->BeginTransaction();

  int age = 22;
  tx.WritePrimaryIndex<int>("users", "user#1", age);
  tx.WriteSecondaryIndex<std::string>("users", "email", "alice@example.com",
                                      "user#1");
  db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); });
}

// ReadSecondaryIndex to unregistered index should abort
TEST_F(DatabaseTest, ReadSecondaryIndexToUnregisteredIndexShouldAbort) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));

  auto& tx = db_->BeginTransaction();
  tx.ReadSecondaryIndex<std::string>("users", "email", "alice@example.com");
  db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); });
}

// WriteSecondaryIndex with non-existent primary key should abort
TEST_F(DatabaseTest, InsertSKWithNonExistentPKShouldAbort) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));

  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "email",
                                                     Constraint::UNIQUE));

  auto& tx = db_->BeginTransaction();
  // PK "ghost#1" does not exist yet
  tx.WriteSecondaryIndex<std::string>("users", "email", "ghost@example.com",
                                      "ghost#1");

  // Expect Abort due to reference integrity violation
  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}

// Test scan order for integer secondary keys
TEST_F(DatabaseTest, ScanSecondaryIndex_IntegerOrder) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("items"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<int>("items", "priority"));

  // Insert items with integer secondary keys: 1, 9, 10
  {
    auto& tx = db_->BeginTransaction();
    tx.WritePrimaryIndex<std::string>("items", "item#1", "low");
    tx.WritePrimaryIndex<std::string>("items", "item#2", "high");
    tx.WritePrimaryIndex<std::string>("items", "item#3", "medium");

    tx.WriteSecondaryIndex<int>("items", "priority", 1, "item#1");
    tx.WriteSecondaryIndex<int>("items", "priority", 9, "item#2");
    tx.WriteSecondaryIndex<int>("items", "priority", 10, "item#3");

    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  // Scan and verify order: should be 1, 9, 10 (NOT 1, 10, 9)
  {
    auto& rtx = db_->BeginTransaction();
    std::vector<int> scanned_priorities;
    auto result = rtx.ScanSecondaryIndex<int>(
        "items", "priority", 1, 10,
        [&](int priority, std::string_view primary_key) {
          scanned_priorities.push_back(priority);
          return false;  // continue scanning
        });

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(scanned_priorities, (std::vector<int>{1, 9, 10}));
    db_->EndTransaction(rtx, [](auto) {});
  }
}

// Test scan order for string secondary keys (lexicographical)
TEST_F(DatabaseTest, ScanSecondaryIndex_StringOrder) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "name"));

  // Insert users with string secondary keys
  {
    auto& tx = db_->BeginTransaction();
    tx.WritePrimaryIndex<int>("users", "user#1", 25);
    tx.WritePrimaryIndex<int>("users", "user#2", 30);
    tx.WritePrimaryIndex<int>("users", "user#3", 35);

    tx.WriteSecondaryIndex<std::string>("users", "name", "Alice", "user#1");
    tx.WriteSecondaryIndex<std::string>("users", "name", "Bob", "user#2");
    tx.WriteSecondaryIndex<std::string>("users", "name", "Charlie", "user#3");

    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  // Scan and verify lexicographical order
  {
    auto& rtx = db_->BeginTransaction();
    std::vector<std::string> scanned_names;

    auto result = rtx.ScanSecondaryIndex<std::string>(
        "users", "name", "Alice", "Charlie",
        [&](std::string_view name, std::string_view primary_key) {
          scanned_names.emplace_back(name);
          return false;  // continue scanning
        });

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(scanned_names,
              (std::vector<std::string>{"Alice", "Bob", "Charlie"}));

    db_->EndTransaction(rtx, [](auto) {});
  }
}

// Test mixed type constraint: ensure different types can't be mixed in same
// index
TEST_F(DatabaseTest, SecondaryIndex_TypeConsistency) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("mixed"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<int>("mixed", "value"));

  // First insert with int type
  {
    auto& tx = db_->BeginTransaction();
    tx.WritePrimaryIndex<std::string>("mixed", "item#1", "data1");
    tx.WriteSecondaryIndex<int>("mixed", "value", 42, "item#1");
    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  {
    auto& tx = db_->BeginTransaction();
    tx.WritePrimaryIndex<std::string>("mixed", "item#2", "data2");
    tx.WriteSecondaryIndex<std::string>("mixed", "value", "hello", "item#2");

    db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); });
  }
}
