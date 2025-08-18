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
#include "lineairdb/key_serializer.h"
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
    db_.reset(nullptr);
    db_ = std::make_unique<LineairDB::Database>(config_);
    db_->CreateTable("users");
  }
};
using Constraint = LineairDB::SecondaryIndexOption::Constraint;

// ---------------- Variant key type insertion tests ----------------

TEST_F(DatabaseTest, InsertSecondaryIndexStringKey) {
  db_->CreateSecondaryIndex<std::string>("users", "email");

  // Write & Verify string secondary key

  auto& tx = db_->BeginTransaction();
  int age = 42;
  tx.Write<int>("users", "user#1", age);
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"), "user#1");
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
}

TEST_F(DatabaseTest, InsertSecondaryIndexStringKeyWithTypeMismatch) {
  db_->CreateSecondaryIndex<std::string>("users", "email");

  // Write & Verify string secondary key

  auto& tx = db_->BeginTransaction();
  int age = 42;
  tx.Write<int>("users", "user#1", age);
  tx.WriteSecondaryIndex<std::string_view>("users", "email", 42, "user#1");
  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}

TEST_F(DatabaseTest, InsertSecondaryIndexIntKey) {
  db_->CreateSecondaryIndex<int>("users", "age");

  // Write & Verify int secondary key

  auto& tx = db_->BeginTransaction();
  int age = 24;
  tx.Write<int>("users", "user#2", age);
  tx.WriteSecondaryIndex<std::string_view>("users", "age", 20, "user#2");
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
}

TEST_F(DatabaseTest, InsertSecondaryIndexIntKeyWithTypeMismatch) {
  db_->CreateSecondaryIndex<int>("users", "age");

  // Write & Verify int secondary key

  auto& tx = db_->BeginTransaction();
  tx.Write<int>("users", "user#2", 20);
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "age", std::string("alice@example.com"), "user#2");
  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}

TEST_F(DatabaseTest, InsertSecondaryIndexTimeKey) {
  db_->CreateSecondaryIndex<std::time_t>("users", "created_at");

  auto ts = std::time(nullptr);

  // Write & Verify time_t secondary key

  auto& tx = db_->BeginTransaction();
  tx.Write<int>("users", "user#3", 30);
  tx.WriteSecondaryIndex<std::string_view>("users", "created_at", ts, "user#3");
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
}

TEST_F(DatabaseTest, InsertSecondaryIndexTimeKeyWithTypeMismatch) {
  db_->CreateSecondaryIndex<std::time_t>("users", "created_at");

  // Write & Verify time_t secondary key

  auto& tx = db_->BeginTransaction();
  tx.Write<int>("users", "user#3", 30);
  tx.WriteSecondaryIndex<std::string_view>("users", "created_at", 42, "user#3");
  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}

// ---------------- Variant key type read tests ----------------
TEST_F(DatabaseTest, ReadSecondaryIndexStringKey) {
  db_->CreateSecondaryIndex<std::string>("users", "email");

  // Write & Verify string secondary key
  auto& tx = db_->BeginTransaction();
  int age = 42;
  tx.Write<int>("users", "user#1", age);
  // Passing std::string explicitly for the third argument avoids type mismatch
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"), "user#1");

  // Commit the writing transaction first
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  auto& rtx = db_->BeginTransaction();
  // Passing std::string explicitly for the third argument avoids type mismatch
  auto pk = rtx.ReadSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"));
  auto val = rtx.Read<int>("users", pk.at(0));
  ASSERT_EQ(val.value(), 42);
  db_->EndTransaction(rtx, [](auto) {});
}

TEST_F(DatabaseTest, ReadSecondaryIndexMultipleKeysStringKey) {
  db_->CreateSecondaryIndex<std::string>("users", "email");

  // Write & Verify string secondary key
  auto& tx = db_->BeginTransaction();
  int age = 42;
  int age2 = 24;
  tx.Write<int>("users", "user#1", age);
  tx.Write<int>("users", "user#2", age2);
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"), "user#1");
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"), "user#2");

  // Commit the writing transaction first
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  auto& rtx = db_->BeginTransaction();
  auto pk = rtx.ReadSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"));
  auto val = rtx.Read<int>("users", pk.at(0));
  ASSERT_EQ(val.value(), 42);
  auto val2 = rtx.Read<int>("users", pk.at(1));
  ASSERT_EQ(val2.value(), 24);
  db_->EndTransaction(rtx, [](auto) {});
}

TEST_F(DatabaseTest, ReadSecondaryIndexIntKey) {
  db_->CreateSecondaryIndex<int>("users", "age");

  // Write & Verify int secondary key

  auto& tx = db_->BeginTransaction();
  int age = 24;
  tx.Write<int>("users", "user#2", age);
  tx.WriteSecondaryIndex<std::string_view>("users", "age", 20, "user#2");

  // Commit the writing transaction
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));

  auto& rtx = db_->BeginTransaction();
  auto pk = rtx.ReadSecondaryIndex<int>("users", "age", 20);
  auto val = rtx.Read<int>("users", pk.at(0));
  ASSERT_EQ(val.value(), 24);
  db_->EndTransaction(rtx, [](auto) {});
}

TEST_F(DatabaseTest, ReadSecondaryIndexMultipleKeysIntKey) {
  db_->CreateSecondaryIndex<int>("users", "age");

  auto& tx = db_->BeginTransaction();
  int age = 24;
  int age2 = 30;
  tx.Write<int>("users", "user#2", age);
  tx.Write<int>("users", "user#3", age2);
  tx.WriteSecondaryIndex<std::string_view>("users", "age", 20, "user#2");
  tx.WriteSecondaryIndex<std::string_view>("users", "age", 20, "user#3");

  // Commit the writing transaction
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));

  auto& rtx = db_->BeginTransaction();
  auto pk = rtx.ReadSecondaryIndex<int>("users", "age", 20);
  auto val = rtx.Read<int>("users", pk.at(0));
  ASSERT_EQ(val.value(), 24);
  auto val2 = rtx.Read<int>("users", pk.at(1));
  ASSERT_EQ(val2.value(), 30);
  db_->EndTransaction(rtx, [](auto) {});
}

TEST_F(DatabaseTest, ReadSecondaryIndexTimeKey) {
  db_->CreateSecondaryIndex<std::time_t>("users", "created_at");

  auto ts = std::time(nullptr);

  // Write & Verify time_t secondary key

  auto& tx = db_->BeginTransaction();
  int age = 30;
  tx.Write<int>("users", "user#3", age);
  tx.WriteSecondaryIndex<std::string_view>("users", "created_at", ts, "user#3");

  // Commit the writing transaction
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));

  auto& rtx = db_->BeginTransaction();
  auto pk = rtx.ReadSecondaryIndex<std::time_t>("users", "created_at", ts);
  auto val = rtx.Read<int>("users", pk.at(0));
  ASSERT_EQ(val.value(), 30);
  db_->EndTransaction(rtx, [](auto) {});
}

TEST_F(DatabaseTest, ReadSecondaryIndexMultipleKeysTimeKey) {
  db_->CreateSecondaryIndex<std::time_t>("users", "created_at");

  auto ts = std::time(nullptr);

  auto& tx = db_->BeginTransaction();
  int age = 30;
  int age2 = 35;
  tx.Write<int>("users", "user#3", age);
  tx.Write<int>("users", "user#4", age2);
  tx.WriteSecondaryIndex<std::string_view>("users", "created_at", ts, "user#3");
  tx.WriteSecondaryIndex<std::string_view>("users", "created_at", ts, "user#4");

  // Commit the writing transaction
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));

  auto& rtx = db_->BeginTransaction();
  auto pk = rtx.ReadSecondaryIndex<std::time_t>("users", "created_at", ts);
  auto val = rtx.Read<int>("users", pk.at(0));
  ASSERT_EQ(val.value(), 30);
  auto val2 = rtx.Read<int>("users", pk.at(1));
  ASSERT_EQ(val2.value(), 35);
  db_->EndTransaction(rtx, [](auto) {});
}

// [Secondary Index Constraint Enforcement Tests] ----------------------------
// UNIQUE constraint: insertion of a duplicate value should abort the txn
TEST_F(DatabaseTest, InsertDuplicateSecondaryKeyViolatesUnique) {
  db_->CreateSecondaryIndex<std::string>("users", "email", Constraint::UNIQUE);

  // 1st row – this should commit.
  {
    auto& tx = db_->BeginTransaction();
    int age = 42;
    tx.Write<int>("users", "user#1", age);
    tx.WriteSecondaryIndex<std::string_view>(
        "users", "email", std::string("bob@example.com"), "user#1");
    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  // 2nd row with the same email – should abort.
  {
    auto& tx = db_->BeginTransaction();
    int age = 24;
    tx.Write<int>("users", "user#2", age);
    tx.WriteSecondaryIndex<std::string_view>("users", "email",
                                             std::string("bob@example.com"),
                                             "user#2");  // duplicate key
    ASSERT_FALSE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
  }
}

// WriteSecondaryIndex to unregistered index should abort
TEST_F(DatabaseTest, WriteSecondaryIndexToUnregisteredIndexShouldAbort) {
  auto& tx = db_->BeginTransaction();
  int age = 22;
  tx.Write<int>("users", "user#1", age);
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"), "user#1");
  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}

// ReadSecondaryIndex to unregistered index should abort
TEST_F(DatabaseTest, ReadSecondaryIndexToUnregisteredIndexShouldAbort) {
  auto& tx = db_->BeginTransaction();
  // This side does not result in an error
  tx.ReadSecondaryIndex<std::string_view>("users", "email",
                                          "alice@example.com");
  db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); });
}

// WriteSecondaryIndex with non-existent primary key should abort
TEST_F(DatabaseTest, InsertSKWithNonExistentPKShouldAbort) {
  db_->CreateSecondaryIndex<std::string>("users", "email", Constraint::UNIQUE);

  auto& tx = db_->BeginTransaction();
  // PK "ghost#1" does not exist yet
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("ghost@example.com"), "ghost#1");

  // Expect Abort due to reference integrity violation
  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}

// UNIQUE: if tx tries to write same SK with different PK, abort
TEST_F(DatabaseTest, Unique_SameTransaction_SameSK_DifferentPK_ShouldAbort) {
  db_->CreateSecondaryIndex<std::string>("users", "email", Constraint::UNIQUE);

  auto& tx = db_->BeginTransaction();
  // insert 2 PKs in the same tx
  tx.Write<std::string_view>("users", "user#1", "Alice");
  tx.Write<std::string_view>("users", "user#2", "Bob");
  // write same SK (email) to user#1 → then try to write same SK to user#2
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("dup@example.com"), "user#1");
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("dup@example.com"), "user#2");

  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}

// UNIQUE: if tx writes same SK with same PK multiple times, it is idempotent
// and should commit
TEST_F(DatabaseTest,
       Unique_SameTransaction_SameSK_SamePK_Idempotent_ShouldCommit) {
  db_->CreateSecondaryIndex<std::string>("users", "email", Constraint::UNIQUE);

  auto& tx = db_->BeginTransaction();
  tx.Write<std::string_view>("users", "user#1", "Alice");
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("ok@example.com"), "user#1");
  // write same SK with same PK multiple times (idempotent)
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("ok@example.com"), "user#1");

  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
}

// Test scan order for integer secondary keys
TEST_F(DatabaseTest, ScanSecondaryIndex_IntegerOrder) {
  ASSERT_TRUE(db_->CreateTable("items"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<int>("items", "priority"));

  // Insert items with integer secondary keys: 1, 9, 10
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("items", "item#1", "low");
    tx.Write<std::string_view>("items", "item#2", "high");
    tx.Write<std::string_view>("items", "item#3", "medium");

    tx.WriteSecondaryIndex<std::string_view>("items", "priority", 1, "item#1");
    tx.WriteSecondaryIndex<std::string_view>("items", "priority", 9, "item#2");
    tx.WriteSecondaryIndex<std::string_view>("items", "priority", 10, "item#3");

    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  // Scan and verify order: should be 1, 9, 10 (NOT 1, 10, 9)
  {
    auto& rtx = db_->BeginTransaction();
    auto count = rtx.ScanSecondaryIndex<std::string_view>(
        "items", "priority", 1, 10,
        [&](auto priority_key, std::vector<std::string_view> primary_keys) {
          if (priority_key == LineairDB::Util::SerializeKey(1)) {
            EXPECT_EQ(primary_keys.at(0), "item#1");
          }
          if (priority_key == LineairDB::Util::SerializeKey(9)) {
            EXPECT_EQ(primary_keys.at(1), "item#2");
          }
          if (priority_key == LineairDB::Util::SerializeKey(10)) {
            EXPECT_EQ(primary_keys.at(2), "item#3");
          }
          return false;  // continue scanning
        });

    if (count.has_value()) {
      ASSERT_EQ(count.value(), 3);
    }
    db_->EndTransaction(rtx, [](auto) {});
  }
}

// Test scan order for string secondary keys (lexicographical)
TEST_F(DatabaseTest, ScanSecondaryIndex_StringOrder) {
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "name"));

  // Insert users with string secondary keys
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("users", "user#1", "Alice");
    tx.Write<std::string_view>("users", "user#2", "Bob");
    tx.Write<std::string_view>("users", "user#3", "Charlie");

    tx.WriteSecondaryIndex<std::string_view>("users", "name",
                                             std::string("Alice"), "user#1");
    tx.WriteSecondaryIndex<std::string_view>("users", "name",
                                             std::string("Bob"), "user#2");
    tx.WriteSecondaryIndex<std::string_view>("users", "name",
                                             std::string("Charlie"), "user#3");

    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  // Scan and verify lexicographical order
  {
    auto& rtx = db_->BeginTransaction();
    auto count = rtx.ScanSecondaryIndex<std::string_view>(
        "users", "name", std::string("Alice"), std::string("Charlie"),
        [&](auto name_key, std::vector<std::string_view> primary_keys) {
          if (name_key == LineairDB::Util::SerializeKey(std::string("Alice"))) {
            EXPECT_EQ(primary_keys.at(0), "user#1");
          }
          if (name_key == LineairDB::Util::SerializeKey(std::string("Bob"))) {
            EXPECT_EQ(primary_keys.at(1), "user#2");
          }
          if (name_key ==
              LineairDB::Util::SerializeKey(std::string("Charlie"))) {
            EXPECT_EQ(primary_keys.at(2), "user#3");
          }
          return false;  // continue scanning
        });

    if (count.has_value()) {
      ASSERT_EQ(count.value(), 3);
    }
    db_->EndTransaction(rtx, [](auto) {});
  }
}

// Test scan order for time_t secondary keys (chronological)
TEST_F(DatabaseTest, ScanSecondaryIndex_TimeOrder) {
  ASSERT_TRUE(db_->CreateTable("events"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<time_t>("events", "timestamp"));

  // Insert events with time_t secondary keys
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("events", "event#1", "start");
    tx.Write<std::string_view>("events", "event#2", "middle");
    tx.Write<std::string_view>("events", "event#3", "end");

    time_t time1 = 1000000000;  // 2001-09-09 01:46:40 UTC
    time_t time2 = 1000000001;  // 2001-09-09 01:46:41 UTC
    time_t time3 = 1000000002;  // 2001-09-09 01:46:42 UTC

    tx.WriteSecondaryIndex<std::string_view>("events", "timestamp", time1,
                                             "event#1");
    tx.WriteSecondaryIndex<std::string_view>("events", "timestamp", time2,
                                             "event#2");
    tx.WriteSecondaryIndex<std::string_view>("events", "timestamp", time3,
                                             "event#3");

    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  // Scan and verify chronological order
  {
    auto& rtx = db_->BeginTransaction();
    auto count = rtx.ScanSecondaryIndex<std::string_view>(
        "events", "timestamp", time_t(1000000000), time_t(1000000002),
        [&](auto timestamp_key, std::vector<std::string_view> primary_keys) {
          if (timestamp_key ==
              LineairDB::Util::SerializeKey(time_t(1000000000))) {
            EXPECT_EQ(primary_keys.at(0), "event#1");
          }
          if (timestamp_key ==
              LineairDB::Util::SerializeKey(time_t(1000000001))) {
            EXPECT_EQ(primary_keys.at(1), "event#2");
          }
          if (timestamp_key ==
              LineairDB::Util::SerializeKey(time_t(1000000002))) {
            EXPECT_EQ(primary_keys.at(2), "event#3");
          }
          return false;  // continue scanning
        });

    if (count.has_value()) {
      ASSERT_EQ(count.value(), 3);
    }
    db_->EndTransaction(rtx, [](auto) {});
  }
}

// Test mixed type constraint: ensure different types can't be mixed in same
// index
TEST_F(DatabaseTest, SecondaryIndex_TypeConsistency) {
  ASSERT_TRUE(db_->CreateTable("mixed"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<int>("mixed", "value"));

  // First insert with int type
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("mixed", "item#1", "data1");
    tx.WriteSecondaryIndex<std::string_view>("mixed", "value", 42, "item#1");
    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("mixed", "item#2", "data2");
    tx.WriteSecondaryIndex<std::string_view>("mixed", "value", "hello",
                                             "item#2");

    db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); });
  }
}

// test for notnull
// if tx does not write all secondary indices after WritePrimary, abort
TEST_F(DatabaseTest, SecondaryIndex_NotNull) {
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "email"));

  auto& tx = db_->BeginTransaction();
  tx.Write<std::string_view>("users", "user#1", "Alice");
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"), "user#1");
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));

  auto& rtx = db_->BeginTransaction();
  rtx.Write<std::string_view>("users", "user#2", "Bob");
  ASSERT_FALSE(db_->EndTransaction(
      rtx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}

TEST_F(DatabaseTest, SecondaryIndex_NotNull_MultipleSecondaryIndices) {
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "email"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "name"));

  // Case 1: if tx does not write all secondary indices, abort
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("users", "user#1", "Alice");
    tx.WriteSecondaryIndex<std::string_view>(
        "users", "email", std::string("alice@example.com"), "user#1");
    ASSERT_FALSE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
  }

  // Case 2: if tx writes secondary index multiple times, abort
  // (verification of unique constraint per index)
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("users", "user#2", "Bob");
    tx.WriteSecondaryIndex<std::string_view>(
        "users", "email", std::string("bob@example.com"), "user#2");
    tx.WriteSecondaryIndex<std::string_view>(
        "users", "email", std::string("bob@example.com"), "user#2");
    ASSERT_FALSE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
  }

  // Case 3: if tx writes both email and name, commit
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("users", "user#3", "Charlie");
    tx.WriteSecondaryIndex<std::string_view>(
        "users", "email", std::string("charlie@example.com"), "user#3");
    tx.WriteSecondaryIndex<std::string_view>("users", "name",
                                             std::string("Charlie"), "user#3");
    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }
}

// test for update SecondaryIndex
TEST_F(DatabaseTest, SecondaryIndex_Update) {
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "email"));

  auto& tx = db_->BeginTransaction();
  tx.Write<std::string_view>("users", "user#1", "Alice");
  tx.WriteSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"), "user#1");
  tx.UpdateSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"),
      std::string("alice@example.com"), "user#1");
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
}

// UpdateSecondaryIndex: moving SK succeeds
TEST_F(DatabaseTest, SecondaryIndex_Update_MoveSK) {
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "email"));

  // Initial registration
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("users", "user#1", "Alice");
    tx.WriteSecondaryIndex<std::string_view>(
        "users", "email", std::string("alice@example.com"), "user#1");
    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  // Update SK: alice@example.com -> alice@new.com
  {
    auto& tx = db_->BeginTransaction();
    tx.UpdateSecondaryIndex<std::string_view>(
        "users", "email", std::string("alice@example.com"),
        std::string("alice@new.com"), "user#1");
    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  // Verification
  {
    auto& rtx = db_->BeginTransaction();
    auto old_pk = rtx.ReadSecondaryIndex<std::string_view>(
        "users", "email", std::string("alice@example.com"));
    auto new_pk = rtx.ReadSecondaryIndex<std::string_view>(
        "users", "email", std::string("alice@new.com"));
    ASSERT_TRUE(db_->EndTransaction(
        rtx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));

    ASSERT_TRUE(old_pk.empty());
    ASSERT_EQ(new_pk.size(), 1u);
    EXPECT_EQ(new_pk[0], std::string("user#1"));
  }
}

// UpdateSecondaryIndex: update to the same SK is a no-op
TEST_F(DatabaseTest, SecondaryIndex_Update_NoOp_SameKey) {
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "email"));

  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("users", "user#1", "Alice");
    tx.WriteSecondaryIndex<std::string_view>(
        "users", "email", std::string("alice@example.com"), "user#1");
    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  {
    auto& tx = db_->BeginTransaction();
    tx.UpdateSecondaryIndex<std::string_view>(
        "users", "email", std::string("alice@example.com"),
        std::string("alice@example.com"), "user#1");
    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  {
    auto& rtx = db_->BeginTransaction();
    auto pk = rtx.ReadSecondaryIndex<std::string_view>(
        "users", "email", std::string("alice@example.com"));
    ASSERT_TRUE(db_->EndTransaction(
        rtx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
    ASSERT_EQ(pk.size(), 1u);
    EXPECT_EQ(pk[0], std::string("user#1"));
  }
}

// UpdateSecondaryIndex: abort on UNIQUE constraint violation
TEST_F(DatabaseTest, SecondaryIndex_Update_UniqueViolation) {
  db_->CreateSecondaryIndex<std::string>("users", "email", Constraint::UNIQUE);

  // Register user#2 -> bob@example.com, user#1 -> alice@example.com
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("users", "user#2", "Bob");
    tx.WriteSecondaryIndex<std::string_view>(
        "users", "email", std::string("bob@example.com"), "user#2");
    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<std::string_view>("users", "user#1", "Alice");
    tx.WriteSecondaryIndex<std::string_view>(
        "users", "email", std::string("alice@example.com"), "user#1");
    ASSERT_TRUE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  // Updating user#1 to bob@example.com causes abort due to UNIQUE constraint
  {
    auto& tx = db_->BeginTransaction();
    tx.UpdateSecondaryIndex<std::string_view>(
        "users", "email", std::string("alice@example.com"),
        std::string("bob@example.com"), "user#1");
    ASSERT_FALSE(db_->EndTransaction(
        tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
  }

  // Verify: unchanged
  {
    auto& rtx = db_->BeginTransaction();
    auto pk_bob = rtx.ReadSecondaryIndex<std::string_view>(
        "users", "email", std::string("bob@example.com"));
    auto pk_alice = rtx.ReadSecondaryIndex<std::string_view>(
        "users", "email", std::string("alice@example.com"));
    ASSERT_TRUE(db_->EndTransaction(
        rtx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));

    ASSERT_EQ(pk_bob.size(), 1u);
    EXPECT_EQ(pk_bob[0], std::string("user#2"));
    ASSERT_EQ(pk_alice.size(), 1u);
    EXPECT_EQ(pk_alice[0], std::string("user#1"));
  }
}

// UpdateSecondaryIndex: abort when updating an unregistered index
TEST_F(DatabaseTest, SecondaryIndex_Update_UnregisteredIndexShouldAbort) {
  auto& tx = db_->BeginTransaction();
  tx.Write<std::string_view>("users", "user#1", "Alice");
  tx.UpdateSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"),
      std::string("alice@new.com"), "user#1");
  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}

// UpdateSecondaryIndex: abort when updating a non-existent PK
TEST_F(DatabaseTest, SecondaryIndex_Update_PrimaryKeyNotExistShouldAbort) {
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "email"));

  auto& tx = db_->BeginTransaction();
  tx.UpdateSecondaryIndex<std::string_view>(
      "users", "email", std::string("alice@example.com"),
      std::string("alice@new.com"), "user#999");
  ASSERT_FALSE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}