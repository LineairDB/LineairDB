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

    TestHelper::RetryTransactionUntilCommit(db_.get(), [&](auto& tx) {
      int alice = 1;
      int bob = 2;
      int carol = 3;
      tx.template Write<decltype(alice)>("alice", alice);
      tx.template Write<decltype(bob)>("bob", bob);
      tx.template Write<decltype(carol)>("carol", carol);
    });
    db_->Fence();
  }
};

TEST_F(IndexTest, Scan) {
  auto& tx = db_->BeginTransaction();
  auto count = tx.Scan("alice", "bob", [&](auto key, auto) {
    EXPECT_TRUE(key == "alice" || key == "bob");
    return false;
  });
  ASSERT_TRUE(count.has_value());
  ASSERT_EQ(2, count.value());  // #Scan is not inclusive
  db_->EndTransaction(tx, [](auto) {});
}

TEST_F(IndexTest, AlphabeticalOrdering) {
  auto& tx = db_->BeginTransaction();
  auto count = tx.Scan("carol", "alice", [&](auto, auto) { return false; });
  ASSERT_FALSE(count.has_value());

  count = tx.Scan("carol", "zzz", [&](auto, auto) { return false; });
  ASSERT_TRUE(count.has_value());
  ASSERT_EQ(1, count.value());
  db_->EndTransaction(tx, [](auto) {});
}

TEST_F(IndexTest, ScanViaTemplate) {
  auto& tx = db_->BeginTransaction();
  auto count = tx.Scan<int>("alice", "bob", [&](auto key, auto) {
    EXPECT_TRUE(key == "alice" || key == "bob");
    return false;
  });
  ASSERT_TRUE(count.has_value());
  ASSERT_EQ(2, count.value());
  db_->EndTransaction(tx, [](auto) {});
}

TEST_F(IndexTest, StopScanning) {
  auto& tx = db_->BeginTransaction();
  auto count = tx.Scan("alice", "carol", [&](auto key, auto) {
    EXPECT_TRUE(key == "alice");
    EXPECT_FALSE(key == "bob");
    return true;
  });
  ASSERT_TRUE(count.has_value());
  ASSERT_EQ(1, count.value());
  db_->EndTransaction(tx, [](auto) {});
}

TEST_F(IndexTest, ScanWithoutEnd) {
  auto& tx = db_->BeginTransaction();
  auto count = tx.Scan("alice", std::nullopt, [&](auto key, auto) {
    EXPECT_TRUE(key == "alice" || key == "bob" || key == "carol");
    return false;
  });
  ASSERT_TRUE(count.has_value());
  ASSERT_EQ(3, count.value());
  db_->EndTransaction(tx, [](auto) {});
}

TEST_F(IndexTest, ScanWithPhantomAvoidance) {
  int dave = 4;

  std::optional<size_t> first, second;
  const auto committed = TestHelper::DoHandlerTransactionsOnMultiThreads(
      db_.get(),
      {[&](LineairDB::Transaction& tx) { tx.Write<int>("dave", dave); },
       [&](LineairDB::Transaction& tx) {
         auto scan = [&]() {
           return tx.Scan("alice", "dave", [&](auto, auto) { return false; });
         };
         first = scan();
         std::this_thread::yield();
         second = scan();
       }});
  if (committed == 2 && first.has_value() && second.has_value()) {
    ASSERT_EQ(first, second);
  }
}
