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

#include <memory>
#include <optional>

#include "../test_helper.hpp"
#include "gtest/gtest.h"

class IndexTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    config_.enable_recovery = false;
    config_.enable_logging = false;
    config_.enable_checkpointing = false;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(IndexTest, InsertAndScanConflictWithinSameTransaction) {
  db_->CreateTable("users");
  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  constexpr int erin = 4;
  tx.Write<int>("erin", erin);

  auto count = tx.Scan("erin", std::nullopt, [&](auto, auto) { return false; });
  ASSERT_TRUE(count.has_value());

  db_->EndTransaction(tx, [](auto) {});
}