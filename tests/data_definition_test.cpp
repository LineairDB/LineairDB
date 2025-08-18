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
    db_ = std::make_unique<LineairDB::Database>();
  }
};

TEST_F(DatabaseTest, CreateTable) {
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
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "nickname"));
}

TEST_F(DatabaseTest, CreateSecondaryIndexWithoutConstraintsTimeKey) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::time_t>("users", "created_at"));
}

TEST_F(DatabaseTest, CreateSecondaryIndexWithoutConstraintsStringKey) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "name"));
}

// UNIQUE only
TEST_F(DatabaseTest, CreateSecondaryIndexUniqueOnlyIntKey) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex<int>("users", "age", Constraint::UNIQUE));
}

TEST_F(DatabaseTest, CreateSecondaryIndexUniqueOnlyStringKey) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::string>("users", "name",
                                                     Constraint::UNIQUE));
}

TEST_F(DatabaseTest, CreateSecondaryIndexUniqueOnlyTimeKey) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex<std::time_t>("users", "created_at",
                                                     Constraint::UNIQUE));
}