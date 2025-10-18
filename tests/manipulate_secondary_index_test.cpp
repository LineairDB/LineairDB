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

class ManipulateSecondaryIndexTest : public ::testing::Test {
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

TEST_F(ManipulateSecondaryIndexTest, ReadWriteSecondaryIndex) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0, 0));

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    tx.Write("user1", "Alice");

    std::string primary_key = "user1";
    tx.WriteSecondaryIndex(
        "age_index", "10",
        reinterpret_cast<const std::byte*>(primary_key.data()),
        primary_key.size());

    auto result = tx.ReadSecondaryIndex("age_index", "10");

    printf("Found %zu primary keys for age=10:\n", result.size());
    for (const auto& [pk_ptr, pk_size] : result) {
      std::string pk_str(reinterpret_cast<const char*>(pk_ptr), pk_size);
      printf("  Primary Key: %s\n", pk_str.c_str());
    }

    EXPECT_EQ(result.size(), 1);

    if (!result.empty()) {
      std::string pk_str(reinterpret_cast<const char*>(result[0].first),
                         result[0].second);
      EXPECT_EQ(pk_str, "user1");
    }

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(ManipulateSecondaryIndexTest, ReadWriteMultipleSecondaryIndex) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0, 0));

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    tx.Write("user1", "Alice");
    tx.Write("user2", "Bob");
    tx.Write("user3", "Carol");

    std::string pk1 = "user1";
    tx.WriteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk1.data()),
                           pk1.size());
    std::string pk2 = "user2";
    tx.WriteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());
    std::string pk3 = "user3";
    tx.WriteSecondaryIndex("age_index", "30",
                           reinterpret_cast<const std::byte*>(pk3.data()),
                           pk3.size());
    tx.WriteSecondaryIndex("age_index", "30",
                           reinterpret_cast<const std::byte*>(pk3.data()),
                           pk3.size());

    auto result_25 = tx.ReadSecondaryIndex("age_index", "25");

    printf("Found %zu users with age=25:\n", result_25.size());
    for (const auto& [pk_ptr, pk_size] : result_25) {
      std::string pk_str(reinterpret_cast<const char*>(pk_ptr), pk_size);
      printf("  - %s\n", pk_str.c_str());
    }
    EXPECT_EQ(result_25.size(), 2);

    auto result_30 = tx.ReadSecondaryIndex("age_index", "30");
    EXPECT_EQ(result_30.size(), 1);

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(ManipulateSecondaryIndexTest, ReadDataViaSecondaryIndex) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0, 0));

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    // データ書き込み
    tx.Write("user1", "Alice");
    tx.Write("user2", "Bob");

    std::string pk1 = "user1";
    tx.WriteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk1.data()),
                           pk1.size());
    std::string pk2 = "user2";
    tx.WriteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());

    // セカンダリインデックスからプライマリキーを取得
    auto primary_keys = tx.ReadSecondaryIndex("age_index", "25");

    printf("Users with age=25:\n");

    // 各プライマリキーからデータを読み取り
    for (const auto& [pk_ptr, pk_size] : primary_keys) {
      std::string_view pk_key(reinterpret_cast<const char*>(pk_ptr), pk_size);

      // プライマリキーでデータを取得
      auto [data_ptr, data_size] = tx.Read(pk_key);

      if (data_ptr != nullptr) {
        std::string name(reinterpret_cast<const char*>(data_ptr), data_size);
        printf("  %.*s: %s\n", static_cast<int>(pk_size),
               reinterpret_cast<const char*>(pk_ptr), name.c_str());
      }
    }

    EXPECT_EQ(primary_keys.size(), 2);

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}
