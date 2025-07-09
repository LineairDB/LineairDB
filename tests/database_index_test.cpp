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

#include "lineairdb/database.h"

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/transaction.h"
#include "lineairdb/tx_status.h"
#include "test_helper.hpp"
class DatabaseTest : public ::testing::Test
{
protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp()
  {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread = 4;
    config_.checkpoint_period = 1;
    config_.epoch_duration_ms = 100;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(DatabaseTest, CreateTable)
{
  db_ = std::make_unique<LineairDB::Database>();
  bool success = db_->CreateTable("users");
  ASSERT_TRUE(success);
  bool duplicated = db_->CreateTable("users");
  ASSERT_FALSE(duplicated);
}

// CreateSecondaryIndex("table_name", "index_name", "UNIQUE")
// For now, we only support UNIQUE constraint, but maybe we will support more constraints in the future, so we will use enum class Constraint.
using Constraint = LineairDB::SecondaryIndexOption::Constraint;

TEST_F(DatabaseTest, CreateSecondaryIndexWithoutConstraints)
{
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "nickname"));
}

// UNIQUE only
TEST_F(DatabaseTest, CreateSecondaryIndexUniqueOnly)
{
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "email", Constraint::UNIQUE));
}

TEST_F(DatabaseTest, InsertWithSecondaryIndex)
{
  db_ = std::make_unique<LineairDB::Database>();
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "email");

  auto &tx = db_->BeginTransaction();
  int age = 42;
  // Primary key registration
  // Write(table, primary_key, value)
  tx.WritePrimaryIndex<int>("users", "user#1", age);

  // Secondary key registration
  // Write(table, index_name, secondary_key, primary_key)
  tx.WriteSecondaryIndex<std::string>("users", "email", "alice@example.com", "user#1");
  db_->EndTransaction(tx, [](auto s)
                      { ASSERT_EQ(LineairDB::TxStatus::Committed, s); });

  auto &rtx = db_->BeginTransaction();
  // Secondary key search
  // Read(table, index_name, secondary_key)
  auto pk = rtx.ReadSecondaryIndex<std::string>("users", "email", "alice@example.com");
  auto val = rtx.ReadPrimaryIndex<int>("users", pk.value());
  ASSERT_EQ(age, val.value());
  db_->EndTransaction(rtx, [](auto) {});
}

// [Secondary Index Constraint Enforcement Tests] ----------------------------
// UNIQUE constraint: insertion of a duplicate value should abort the txn
TEST_F(DatabaseTest, InsertDuplicateSecondaryKeyViolatesUnique)
{
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "email", Constraint::UNIQUE));

  // 1st row – this should commit.
  {
    auto &tx = db_->BeginTransaction();
    int age = 42;
    tx.WritePrimaryIndex<int>("users", "user#1", age);
    tx.WriteSecondaryIndex<std::string>("users", "email", "bob@example.com", "user#1");
    ASSERT_TRUE(db_->EndTransaction(tx, [](auto s)
                                    { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
  }

  // 2nd row with the same email – should abort.
  {
    auto &tx = db_->BeginTransaction();
    int age = 24;
    tx.WritePrimaryIndex<int>("users", "user#2", age);
    tx.WriteSecondaryIndex<std::string>("users", "email", "bob@example.com", "user#2"); // duplicate key
    ASSERT_FALSE(db_->EndTransaction(tx, [](auto s)
                                     { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
  }
}

// WriteSecondaryIndex to unregistered index should abort
TEST_F(DatabaseTest, WriteSecondaryIndexToUnregisteredIndexShouldAbort)
{
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));

  auto &tx = db_->BeginTransaction();

  int age = 22;
  tx.WritePrimaryIndex<int>("users", "user#1", age);
  tx.WriteSecondaryIndex<std::string>("users", "email", "alice@example.com", "user#1");
  db_->EndTransaction(tx, [](auto s)
                      { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); });
}

// ReadSecondaryIndex to unregistered index should abort
TEST_F(DatabaseTest, ReadSecondaryIndexToUnregisteredIndexShouldAbort)
{
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));

  auto &tx = db_->BeginTransaction();
  tx.ReadSecondaryIndex<std::string>("users", "email", "alice@example.com");
  db_->EndTransaction(tx, [](auto s)
                      { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); });
}

// WriteSecondaryIndex with non-existent primary key should abort
TEST_F(DatabaseTest, InsertSKWithNonExistentPKShouldAbort)
{
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));

  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "email", Constraint::UNIQUE));

  auto &tx = db_->BeginTransaction();
  // PK "ghost#1" does not exist yet
  tx.WriteSecondaryIndex<std::string>("users", "email", "ghost@example.com", "ghost#1");

  // Expect Abort due to reference integrity violation
  ASSERT_FALSE(db_->EndTransaction(tx, [](auto s)
                                   { ASSERT_EQ(LineairDB::TxStatus::Aborted, s); }));
}
