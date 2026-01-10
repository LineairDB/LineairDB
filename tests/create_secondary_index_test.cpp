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

#include <filesystem>
#include <memory>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/transaction.h"
#include "lineairdb/tx_status.h"
#include "test_helper.hpp"

class CreateSecondaryIndexTest : public ::testing::Test {
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
  }
};

TEST_F(CreateSecondaryIndexTest, CreateSecondaryIndexWithIntKey) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));
}

TEST_F(CreateSecondaryIndexTest, CreateSecondaryIndexWithStringKey) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "name_index", 0));
}

TEST_F(CreateSecondaryIndexTest, CreateSecondaryIndexWithDateTimeKey) {
  ASSERT_TRUE(db_->CreateTable("events"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("events", "created_at_index", 0));
}

TEST_F(CreateSecondaryIndexTest, CreateMultipleSecondaryIndexes) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "name_index", 0));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "created_at_index", 0));
}

TEST_F(CreateSecondaryIndexTest, CreateDuplicateSecondaryIndex) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));
  ASSERT_FALSE(db_->CreateSecondaryIndex("users", "age_index", 0));
}

TEST_F(CreateSecondaryIndexTest, CreateSecondaryIndexOnNonExistentTable) {
  ASSERT_FALSE(db_->CreateSecondaryIndex("non_existent_table", "index", 0));
}
