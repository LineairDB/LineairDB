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
class DatabaseTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread        = 4;
    config_.checkpoint_period = 1;
    config_.epoch_duration_ms = 100;
    db_                       = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(DatabaseTest, Instantiate) {}

TEST_F(DatabaseTest, InstantiateWithConfig) {
  db_.reset(nullptr);
  LineairDB::Config conf;
  conf.checkpoint_period = 1;
  ASSERT_NO_THROW(db_ = std::make_unique<LineairDB::Database>(conf));
}
TEST_F(DatabaseTest, ExecuteTransaction) {
  int value_of_alice = 1;
  TestHelper::DoTransactions(
      db_.get(),
      {[&](LineairDB::Transaction& tx) {
         tx.Write("alice", reinterpret_cast<std::byte*>(&value_of_alice),
                  sizeof(int));
       },
       [&](LineairDB::Transaction& tx) {
         auto alice = tx.Read("alice");
         ASSERT_NE(alice.first, nullptr);
         ASSERT_EQ(value_of_alice, *reinterpret_cast<const int*>(alice.first));
         ASSERT_EQ(0, tx.Read("bob").second);
       }});
}

TEST_F(DatabaseTest, ExecuteTransactionWithTemplates) {
  int value_of_alice = 1;
  TestHelper::DoTransactions(db_.get(),
                             {[&](LineairDB::Transaction& tx) {
                                tx.Write<int>("alice", value_of_alice);
                              },
                              [&](LineairDB::Transaction& tx) {
                                auto alice = tx.Read<int>("alice");
                                ASSERT_EQ(value_of_alice, alice.value());
                                ASSERT_FALSE(tx.Read<int>("bob").has_value());
                              }});
}

TEST_F(DatabaseTest, LargeSizeBuffer) {
  constexpr size_t Size = 2048;
  LineairDB::Config conf;
  conf.checkpoint_period    = 1;
  conf.max_thread           = 1;
  conf.enable_checkpointing = false;

  std::array<std::byte, Size> alice;

  ::testing::FLAGS_gtest_death_test_style = "threadsafe";

  TestHelper::DoTransactions(
      db_.get(),
      {[&](LineairDB::Transaction& tx) { tx.Write("alice", &alice[0], Size); },
       [&](LineairDB::Transaction& tx) {
         ASSERT_TRUE(tx.Read<decltype(alice)>("alice").has_value());
       }});
}

TEST_F(DatabaseTest, Scan) {
  int alice = 1;
  int bob   = 2;
  int carol = 3;
  TestHelper::RetryTransactionUntilCommit(db_.get(), [&](auto& tx) {
    tx.template Write<decltype(alice)>("alice", alice);
    tx.template Write<decltype(bob)>("bob", bob);
    tx.template Write<decltype(carol)>("carol", carol);
  });
  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
                    // Scan
                    auto count = tx.Scan<decltype(alice)>(
                        "alice", "carol", [&](auto key, auto value) {
                          if (key == "alice") { EXPECT_EQ(alice, value); }
                          if (key == "bob") { EXPECT_EQ(bob, value); }
                          if (key == "carol") { EXPECT_EQ(carol, value); }
                          return false;
                        });
                    if (count.has_value()) { ASSERT_EQ(count.value(), 3); }
                  },
                  [&](LineairDB::Transaction& tx) {
                    // Cancel
                    auto count = tx.Scan<decltype(alice)>(
                        "alice", "carol", [&](auto key, auto value) {
                          if (key == "alice") { EXPECT_EQ(alice, value); }
                          return true;
                        });
                    if (count.has_value()) { ASSERT_EQ(count.value(), 1); };
                  }});
}

TEST_F(DatabaseTest, SaveAsString) {
  TestHelper::DoTransactions(db_.get(),
                             {[&](LineairDB::Transaction& tx) {
                                tx.Write<std::string_view>("alice", "value");
                              },
                              [&](LineairDB::Transaction& tx) {
                                auto alice = tx.Read<std::string_view>("alice");
                                ASSERT_EQ("value", alice.value());
                              }});
}

TEST_F(DatabaseTest, UserAbort) {
  TestHelper::DoTransactions(db_.get(),
                             {[&](LineairDB::Transaction& tx) {
                                int value_of_alice = 1;
                                tx.Write<int>("alice", value_of_alice);
                                tx.Abort();
                              },
                              [&](LineairDB::Transaction& tx) {
                                auto alice = tx.Read<int>("alice");
                                ASSERT_FALSE(alice.has_value());  // Opacity
                                tx.Abort();
                              }});
}

TEST_F(DatabaseTest, ReadYourOwnWrites) {
  int value_of_alice = 1;
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               tx.Write<int>("alice", value_of_alice);
                               auto alice = tx.Read<int>("alice");
                               ASSERT_EQ(value_of_alice, alice.value());
                             }});
}

TEST_F(DatabaseTest, ThreadSafetyInsertions) {
  TransactionProcedure insertTenTimes([](LineairDB::Transaction& tx) {
    int value = 0xBEEF;
    for (size_t idx = 0; idx <= 10; idx++) {
      tx.Write<int>("alice" + std::to_string(idx), value);
    }
  });

  ASSERT_NO_THROW({
    TestHelper::DoTransactionsOnMultiThreads(
        db_.get(),
        {insertTenTimes, insertTenTimes, insertTenTimes, insertTenTimes});
  });
  db_->Fence();

  TestHelper::DoTransactions(
      db_.get(), {[](LineairDB::Transaction& tx) {
        for (size_t idx = 0; idx <= 10; idx++) {
          auto alice = tx.Read<int>("alice" + std::to_string(idx));
          ASSERT_TRUE(alice.has_value());
          auto current_value = alice.value();
          ASSERT_EQ(0xBEEF, current_value);
        }
      }});
}

TEST_F(DatabaseTest, NoConfigTransaction) {
  // NOTE: this test will take default 5 seconds for checkpointing
  db_.reset(nullptr);
  db_                = std::make_unique<LineairDB::Database>();
  int value_of_alice = 1;
  TestHelper::DoTransactions(
      db_.get(),
      {[&](LineairDB::Transaction& tx) {
         tx.Write("alice", reinterpret_cast<std::byte*>(&value_of_alice),
                  sizeof(int));
       },
       [&](LineairDB::Transaction& tx) {
         auto alice = tx.Read("alice");
         ASSERT_NE(alice.first, nullptr);
         ASSERT_EQ(value_of_alice, *reinterpret_cast<const int*>(alice.first));
         ASSERT_EQ(0, tx.Read("bob").second);
       }});
}

// [Secondary Index TDD] ------------------------------------------------------
// 1. テーブル作成 API のテスト
//構造
//database -> Table -> ConcurrentTables(primary, secondaries)
TEST_F(DatabaseTest, CreateTable) {
  // テーブル名 "users" の生成が例外なく行えること
  ASSERT_NO_THROW({
    auto* users_table = db_->CreateTable("users");
    ASSERT_NE(users_table, nullptr);
  });
}

// 2. セカンダリインデックスを用いた基本操作のテスト
//    - プライマリキーでデータ登録
//    - セカンダリキーでプライマリキーを登録
//    - プライマリキー取得 → DataItem* が返る
//    - セカンダリキー取得 → 対応するプライマリキーが返る
TEST_F(DatabaseTest, SecondaryIndexBasic) {
  // 前準備: テーブルとインデックスの生成
  auto* users_table = db_->CreateTable("users");
  ASSERT_NE(users_table, nullptr);
  ASSERT_TRUE(users_table->CreateSecondaryIndex("email"));

  // データ登録
  const std::string primary_key   = "user#1";
  const std::string secondary_key = "alice@example.com";
  int user_value                  = 42;

  // プライマリ登録
  ASSERT_TRUE(users_table->Put<int>(primary_key, user_value));

  // セカンダリ登録 (index "email" に secondary -> primary を保持)
  ASSERT_TRUE(users_table->Put("email", secondary_key, primary_key));

  // --- 検索 ---
  auto age_opt = users_table->Get<int>(primary_key);
  ASSERT_TRUE(age_opt.has_value());
  ASSERT_EQ(user_value, age_opt.value());

  auto pk_opt = users_table->Get<std::string>(secondary_key);
  ASSERT_TRUE(pk_opt.has_value());
  ASSERT_EQ(primary_key, pk_opt.value());
}

// ------------
