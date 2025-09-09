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
  tx.SetTable("users");
  tx.SetSecondaryIndex("email");
  int age = 42;
  tx.Write<int>("user#1", age);
  tx.WriteSecondaryIndex<std::string_view>(std::string("alice@example.com"),
                                           "user#1");
  ASSERT_TRUE(db_->EndTransaction(
      tx, [](auto s) { ASSERT_EQ(LineairDB::TxStatus::Committed, s); }));
}
