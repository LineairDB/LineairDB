/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
/*
#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "test_helper.hpp"
#include "util/logger.hpp"

class SecondaryIndexRecoveryTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;

  virtual void SetUp() {
    std::filesystem::remove_all("lineairdb_logs");
    config_.max_thread = 4;
    config_.enable_logging = true;
    config_.enable_recovery = true;
    config_.enable_checkpointing = true;
    config_.checkpoint_period = 1;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }

  virtual void TearDown() {
    db_.reset(nullptr);
    std::filesystem::remove_all("lineairdb_logs");
  }
};

// 基本的なSecondary Indexのリカバリーテスト
TEST_F(SecondaryIndexRecoveryTest, BasicRecovery) {
  const std::string table_name = "users";
  const std::string index_name = "age_index";
  const std::string index_key = "age:30";
  const std::string primary_key = "user1";
  const std::string value = "Alice";

  db_->CreateTable(table_name);
  ASSERT_TRUE(db_->CreateSecondaryIndex(table_name, index_name, 0));

  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        ASSERT_TRUE(tx.SetTable(table_name));
        tx.Write(primary_key, reinterpret_cast<const std::byte*>(value.data()),
                 value.size());
        tx.WriteSecondaryIndex(
            index_name, index_key,
            reinterpret_cast<const std::byte*>(primary_key.data()),
            primary_key.size());
      }});
  db_->Fence();

  // DBを再起動してリカバリーを実行
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config_);

  // リカバリー後にデータが復元されているか確認
  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        ASSERT_TRUE(tx.SetTable(table_name));

        auto primary_keys = tx.ReadSecondaryIndex(index_name, index_key);
        ASSERT_EQ(primary_keys.size(), 1);
        const auto& [pk_ptr, pk_size] = primary_keys[0];
        std::string recovered_pk(reinterpret_cast<const char*>(pk_ptr),
                                 pk_size);
        ASSERT_EQ(recovered_pk, primary_key);

        auto [value_ptr, value_size] = tx.Read(primary_key);
        ASSERT_TRUE(value_ptr != nullptr);
        std::string recovered_value(reinterpret_cast<const char*>(value_ptr),
                                    value_size);
        ASSERT_EQ(recovered_value, value);
      }});
}

// 複数のprimary keysを持つSecondary Indexのリカバリーテスト
TEST_F(SecondaryIndexRecoveryTest, MultiplePrimaryKeysRecovery) {
  const std::string table_name = "users";
  const std::string index_name = "age_index";
  const std::string index_key = "age:25";
  const std::vector<std::string> primary_keys = {"user1", "user2", "user3"};
  const std::vector<std::string> values = {"Alice", "Bob", "Charlie"};

  db_->CreateTable(table_name);
  ASSERT_TRUE(db_->CreateSecondaryIndex(table_name, index_name, 0));

  // 同じインデックスキーに複数のprimary keyを登録
  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        ASSERT_TRUE(tx.SetTable(table_name));
        for (size_t i = 0; i < primary_keys.size(); i++) {
          tx.Write(primary_keys[i],
                   reinterpret_cast<const std::byte*>(values[i].data()),
                   values[i].size());
          tx.WriteSecondaryIndex(
              index_name, index_key,
              reinterpret_cast<const std::byte*>(primary_keys[i].data()),
              primary_keys[i].size());
        }
      }});
  db_->Fence();

  // DBを再起動
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config_);

  // リカバリー後に全てのprimary keysが復元されているか確認
  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        ASSERT_TRUE(tx.SetTable(table_name));

        auto recovered_pks = tx.ReadSecondaryIndex(index_name, index_key);
        ASSERT_EQ(recovered_pks.size(), primary_keys.size());

        // 全てのprimary keyが存在することを確認
        for (const auto& expected_pk : primary_keys) {
          bool found = false;
          for (const auto& [pk_ptr, pk_size] : recovered_pks) {
            std::string pk(reinterpret_cast<const char*>(pk_ptr), pk_size);
            if (pk == expected_pk) {
              found = true;
              break;
            }
          }
          ASSERT_TRUE(found) << "Primary key not found: " << expected_pk;
        }
      }});
}

// 複数のSecondary Indexのリカバリーテスト
TEST_F(SecondaryIndexRecoveryTest, MultipleSecondaryIndicesRecovery) {
  const std::string table_name = "users";
  const std::string age_index = "age_index";
  const std::string city_index = "city_index";
  const std::string primary_key = "user1";
  const std::string value = "Alice";
  const std::string age_key = "age:30";
  const std::string city_key = "city:Tokyo";

  db_->CreateTable(table_name);
  ASSERT_TRUE(db_->CreateSecondaryIndex(table_name, age_index, 0));
  ASSERT_TRUE(db_->CreateSecondaryIndex(table_name, city_index, 0));

  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        ASSERT_TRUE(tx.SetTable(table_name));
        tx.Write(primary_key, reinterpret_cast<const std::byte*>(value.data()),
                 value.size());
        tx.WriteSecondaryIndex(
            age_index, age_key,
            reinterpret_cast<const std::byte*>(primary_key.data()),
            primary_key.size());
        tx.WriteSecondaryIndex(
            city_index, city_key,
            reinterpret_cast<const std::byte*>(primary_key.data()),
            primary_key.size());
      }});
  db_->Fence();

  // DBを再起動
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config_);

  // 両方のSecondary Indexが復元されているか確認
  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        ASSERT_TRUE(tx.SetTable(table_name));

        // age_indexの確認
        auto age_pks = tx.ReadSecondaryIndex(age_index, age_key);
        ASSERT_EQ(age_pks.size(), 1);
        std::string age_pk(reinterpret_cast<const char*>(age_pks[0].first),
                           age_pks[0].second);
        ASSERT_EQ(age_pk, primary_key);

        // city_indexの確認
        auto city_pks = tx.ReadSecondaryIndex(city_index, city_key);
        ASSERT_EQ(city_pks.size(), 1);
        std::string city_pk(reinterpret_cast<const char*>(city_pks[0].first),
                            city_pks[0].second);
        ASSERT_EQ(city_pk, primary_key);
      }});
}

// リカバリーの冪等性テスト（複数回再起動しても同じ結果になる）
TEST_F(SecondaryIndexRecoveryTest, RecoveryIdempotence) {
  const std::string table_name = "users";
  const std::string index_name = "age_index";
  const std::string index_key = "age:40";
  const std::string primary_key = "user1";
  const std::string value = "David";

  db_->CreateTable(table_name);
  ASSERT_TRUE(db_->CreateSecondaryIndex(table_name, index_name, 0));

  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        ASSERT_TRUE(tx.SetTable(table_name));
        tx.Write(primary_key, reinterpret_cast<const std::byte*>(value.data()),
                 value.size());
        tx.WriteSecondaryIndex(
            index_name, index_key,
            reinterpret_cast<const std::byte*>(primary_key.data()),
            primary_key.size());
      }});
  db_->Fence();

  // 複数回再起動してリカバリーを実行
  for (int i = 0; i < 3; i++) {
    db_.reset(nullptr);
    db_ = std::make_unique<LineairDB::Database>(config_);

    TestHelper::DoTransactions(
        db_.get(), {[&](LineairDB::Transaction& tx) {
          ASSERT_TRUE(tx.SetTable(table_name));

          auto primary_keys = tx.ReadSecondaryIndex(index_name, index_key);
          ASSERT_EQ(primary_keys.size(), 1) << "Failed at iteration " << i;
          const auto& [pk_ptr, pk_size] = primary_keys[0];
          std::string recovered_pk(reinterpret_cast<const char*>(pk_ptr),
                                   pk_size);
          ASSERT_EQ(recovered_pk, primary_key) << "Failed at iteration " << i;
        }});
  }
}

size_t getLogDirectorySize(const LineairDB::Config& conf) {
  namespace fs = std::filesystem;
  size_t size = 0;
  for (const auto& entry : fs::directory_iterator(conf.work_dir)) {
    if (entry.path().filename().generic_string().find("working") !=
        std::string::npos)
      continue;
    size += fs::file_size(entry.path());
  }
  return size;
}

TEST_F(SecondaryIndexRecoveryTest, SecondaryIndexCheckpointing) {
  const LineairDB::Config config = db_->GetConfig();
  ASSERT_TRUE(config.enable_logging);
  ASSERT_TRUE(config.enable_checkpointing);

  const std::string table_name = "users";
  const std::string index_name = "age_index";
  ASSERT_TRUE(db_->CreateTable(table_name));
  ASSERT_TRUE(db_->CreateSecondaryIndex(table_name, index_name, 0));

  const std::string primary_key = "user1";
  const std::string value = "Alice";
  const std::string index_key = "age:30";

  size_t filesize = getLogDirectorySize(config);
  ASSERT_EQ(filesize, 0u);
  bool filesize_is_monotonically_increasing = true;
  auto begin = std::chrono::high_resolution_clock::now();

  for (;;) {
    TestHelper::DoTransactions(
        db_.get(), {[&](LineairDB::Transaction& tx) {
          ASSERT_TRUE(tx.SetTable(table_name));
          tx.Write(primary_key,
                   reinterpret_cast<const std::byte*>(value.data()),
                   value.size());
          tx.WriteSecondaryIndex(
              index_name, index_key,
              reinterpret_cast<const std::byte*>(primary_key.data()),
              primary_key.size());
        }});

    const size_t current_file_size = getLogDirectorySize(config);
    if (filesize <= current_file_size) {
      filesize = current_file_size;
    } else {
      filesize_is_monotonically_increasing = false;
      break;
    }

    auto now = std::chrono::high_resolution_clock::now();
    assert(begin < now);
    size_t elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(now - begin).count();
    if (config.checkpoint_period * 10 < elapsed) break;
  }
  ASSERT_FALSE(filesize_is_monotonically_increasing);
}
 */