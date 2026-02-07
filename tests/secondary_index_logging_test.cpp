/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.

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

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "msgpack.hpp"
#include "recovery/logger.h"
#include "spdlog/spdlog.h"
#include "test_helper.hpp"

namespace {

struct SecondaryLogStats {
  size_t record_count = 0;
  size_t primary_keys_count = 0;
  size_t primary_keys_bytes = 0;
};

size_t GetLogDirectorySize(const LineairDB::Config& conf) {
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

SecondaryLogStats GetSecondaryIndexLogStatsForLatestEpoch(
    const LineairDB::Config& conf) {
  namespace fs = std::filesystem;
  SecondaryLogStats stats{};
  if (!fs::exists(conf.work_dir)) return stats;

  LineairDB::EpochNumber max_epoch = 0;
  std::vector<LineairDB::Recovery::Logger::LogRecord> latest_records;

  for (const auto& entry : fs::directory_iterator(conf.work_dir)) {
    const auto filename = entry.path().filename().generic_string();
    if (filename.find("thread") != 0) continue;
    if (filename.find("working") != std::string::npos) continue;
    if (!entry.is_regular_file()) continue;

    std::ifstream file(entry.path(), std::ifstream::in | std::ifstream::binary);
    if (!file.good()) continue;

    std::string buffer((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());
    if (buffer.empty()) continue;

    size_t offset = 0;
    while (offset < buffer.size()) {
      auto oh = msgpack::unpack(buffer.data(), buffer.size(), offset);
      auto obj = oh.get();
      LineairDB::Recovery::Logger::LogRecords records;
      obj.convert(records);
      for (auto& record : records) {
        if (record.epoch < max_epoch) continue;
        if (record.epoch > max_epoch) {
          max_epoch = record.epoch;
          latest_records.clear();
        }
        latest_records.emplace_back(std::move(record));
      }
    }
  }

  for (const auto& record : latest_records) {
    for (const auto& kvp : record.key_value_pairs) {
      if (kvp.index_name.empty()) continue;
      stats.record_count++;
      stats.primary_keys_count += kvp.primary_keys.size();
      for (const auto& pk : kvp.primary_keys) {
        stats.primary_keys_bytes += pk.size();
      }
    }
  }

  return stats;
}

LineairDB::EpochNumber ReadDurableEpoch(const LineairDB::Config& conf) {
  namespace fs = std::filesystem;
  const auto path = fs::path(conf.work_dir) / "durable_epoch.json";
  std::ifstream file(path);
  LineairDB::EpochNumber epoch = 0;
  if (file.good()) {
    file >> epoch;
  }
  return epoch;
}

bool WaitForDurableEpochAtLeast(const LineairDB::Config& conf,
                                LineairDB::EpochNumber target,
                                std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (ReadDurableEpoch(conf) >= target) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

bool WaitForCheckpointFile(const LineairDB::Config& conf,
                           std::chrono::seconds timeout) {
  namespace fs = std::filesystem;
  const auto checkpoint_path = fs::path(conf.work_dir) / "checkpoint.log";
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (fs::exists(checkpoint_path) && fs::file_size(checkpoint_path) > 0) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return false;
}

std::string MakeFixedPrimaryKey(size_t index) {
  char buffer[17];
  std::snprintf(buffer, sizeof(buffer), "pk%014zu", index);
  return std::string(buffer, 16);
}

std::vector<std::string> MakePrimaryKeys(size_t count, size_t offset = 0) {
  std::vector<std::string> keys;
  keys.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    keys.emplace_back(MakeFixedPrimaryKey(offset + i));
  }
  return keys;
}

}  // namespace

class SecondaryIndexLoggingTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;

  void SetUp() override {
    spdlog::set_level(spdlog::level::info);
    std::filesystem::remove_all("lineairdb_logs");
    config_.max_thread = 4;
    config_.enable_logging = true;
    config_.enable_recovery = true;
    config_.enable_checkpointing = true;
    config_.checkpoint_period = 1;
    db_ = std::make_unique<LineairDB::Database>(config_);
    db_->CreateTable("users");
    spdlog::set_level(spdlog::level::info);
  }
};

TEST_F(SecondaryIndexLoggingTest,
       SecondaryIndexDeltaLoggingAvoidsFullPrimaryKeyList) {
  LineairDB::Config config = db_->GetConfig();
  config.enable_checkpointing = false;
  config.enable_logging = true;
  config.enable_recovery = false;

  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<LineairDB::Database>(config);

  const std::string table_name = "users";
  const std::string index_name = "age_index";
  const size_t secondary_keys = 9;
  const size_t primary_keys_per_secondary = 300;
  std::vector<std::string> index_keys;
  index_keys.reserve(secondary_keys);
  for (size_t i = 0; i < secondary_keys; ++i) {
    index_keys.emplace_back("age:" + std::to_string(30 + i));
  }

  db_->CreateTable(table_name);
  ASSERT_TRUE(db_->CreateSecondaryIndex(table_name, index_name, 0));

  // Preload 9 secondary keys, each with 300 primary keys (16 bytes each).
  {
    auto& tx = db_->BeginTransaction();
    ASSERT_TRUE(tx.SetTable(table_name));
    for (size_t s = 0; s < secondary_keys; ++s) {
      for (size_t i = 0; i < primary_keys_per_secondary; ++i) {
        const size_t pk_index = s * primary_keys_per_secondary + i;
        const std::string primary_key = MakeFixedPrimaryKey(pk_index);
        const std::string value = "value_" + primary_key;
        tx.Write(primary_key, reinterpret_cast<const std::byte*>(value.data()),
                 value.size());
        tx.WriteSecondaryIndex(
            index_name, index_keys[s],
            reinterpret_cast<const std::byte*>(primary_key.data()),
            primary_key.size());
      }
    }
    const bool committed = db_->EndTransaction(tx, [](auto) {});
    ASSERT_TRUE(committed);
  }

  // Trigger a single transaction that updates all 9 secondary keys.
  {
    auto& tx = db_->BeginTransaction();
    ASSERT_TRUE(tx.SetTable(table_name));
    for (size_t s = 0; s < secondary_keys; ++s) {
      const size_t pk_index = secondary_keys * primary_keys_per_secondary + s;
      const std::string primary_key = MakeFixedPrimaryKey(pk_index);
      const std::string value = "value_" + primary_key;
      tx.Write(primary_key, reinterpret_cast<const std::byte*>(value.data()),
               value.size());
      tx.WriteSecondaryIndex(
          index_name, index_keys[s],
          reinterpret_cast<const std::byte*>(primary_key.data()),
          primary_key.size());
    }
    const bool committed = db_->EndTransaction(tx, [](auto) {});
    ASSERT_TRUE(committed);
  }
  db_->Fence();

  const auto stats = GetSecondaryIndexLogStatsForLatestEpoch(config);
  ASSERT_GT(stats.record_count, 0u);
  EXPECT_EQ(stats.primary_keys_count, 0u);
  std::cout << "[SecondaryIndexLogStats] secondary_entries="
            << stats.record_count
            << " secondary_pk_count=" << stats.primary_keys_count
            << " secondary_pk_bytes=" << stats.primary_keys_bytes << std::endl;
}

TEST_F(SecondaryIndexLoggingTest, RecoveryWithSecondaryIndexWithoutCheckpoint) {
  LineairDB::Config config = db_->GetConfig();
  config.enable_checkpointing = false;
  config.enable_logging = true;
  config.enable_recovery = true;

  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<LineairDB::Database>(config);

  const std::string table_name = "users";
  const std::string index_name = "age_index";
  const std::string index_key = "age:30";
  const size_t primary_key_count = 300;
  const auto primary_keys = MakePrimaryKeys(primary_key_count);

  db_->CreateTable(table_name);
  ASSERT_TRUE(db_->CreateSecondaryIndex(table_name, index_name, 0));

  {
    auto& tx = db_->BeginTransaction();
    ASSERT_TRUE(tx.SetTable(table_name));
    for (const auto& primary_key : primary_keys) {
      const std::string value = "value_" + primary_key;
      tx.Write(primary_key, reinterpret_cast<const std::byte*>(value.data()),
               value.size());
      tx.WriteSecondaryIndex(
          index_name, index_key,
          reinterpret_cast<const std::byte*>(primary_key.data()),
          primary_key.size());
    }
    const bool committed = db_->EndTransaction(tx, [](auto) {});
    ASSERT_TRUE(committed);
  }
  db_->Fence();

  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        ASSERT_TRUE(tx.SetTable(table_name));
        auto results = tx.ReadSecondaryIndex(index_name, index_key);
        std::set<std::string> recovered;
        for (const auto& entry : results) {
          const auto* ptr = entry.first;
          const auto size = entry.second;
          recovered.emplace(reinterpret_cast<const char*>(ptr), size);
        }
        ASSERT_EQ(recovered.size(), primary_keys.size());
        for (const auto& expected : primary_keys) {
          ASSERT_TRUE(recovered.count(expected));
        }
      }});
}

TEST_F(SecondaryIndexLoggingTest,
       RecoveryWithSecondaryIndexWithCheckpointSameEpoch) {
  LineairDB::Config config = db_->GetConfig();
  config.enable_checkpointing = true;
  config.checkpoint_period = 1;
  config.enable_logging = true;
  config.enable_recovery = true;
  config.max_thread = 4;

  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<LineairDB::Database>(config);

  const std::string table_name = "users";
  const std::string index_name = "age_index";
  const std::string index_key = "age:30";
  const size_t primary_key_count = 300;
  const auto primary_keys_before = MakePrimaryKeys(primary_key_count);

  db_->CreateTable(table_name);
  ASSERT_TRUE(db_->CreateSecondaryIndex(table_name, index_name, 0));

  {
    auto& tx = db_->BeginTransaction();
    ASSERT_TRUE(tx.SetTable(table_name));
    for (const auto& primary_key : primary_keys_before) {
      const std::string value = "value_" + primary_key;
      tx.Write(primary_key, reinterpret_cast<const std::byte*>(value.data()),
               value.size());
      tx.WriteSecondaryIndex(
          index_name, index_key,
          reinterpret_cast<const std::byte*>(primary_key.data()),
          primary_key.size());
    }
    const bool committed = db_->EndTransaction(tx, [](auto) {});
    ASSERT_TRUE(committed);
  }
  db_->Fence();
  ASSERT_TRUE(WaitForCheckpointFile(
      config, std::chrono::seconds(config.checkpoint_period * 5)));

  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
        ASSERT_TRUE(tx.SetTable(table_name));
        auto results = tx.ReadSecondaryIndex(index_name, index_key);
        std::set<std::string> recovered;
        for (const auto& entry : results) {
          recovered.emplace(reinterpret_cast<const char*>(entry.first),
                            entry.second);
        }
        ASSERT_EQ(recovered.size(), primary_keys_before.size());
        for (const auto& expected : primary_keys_before) {
          ASSERT_TRUE(recovered.count(expected));
        }
      }});
}

TEST_F(SecondaryIndexLoggingTest, SecondaryIndexAddTimingRecorded) {
  LineairDB::Config config = db_->GetConfig();
  config.enable_logging = true;
  config.enable_checkpointing = false;
  config.enable_recovery = false;
  config.max_thread = 1;

  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<LineairDB::Database>(config);

  const std::string table_name = "users";
  const std::string index_name = "age_index";
  const std::string index_key = "age:30";
  const size_t initial_primary_keys = 300;
  const size_t iterations = 10;

  db_->CreateTable(table_name);
  ASSERT_TRUE(db_->CreateSecondaryIndex(table_name, index_name, 0));

  // Preload 300 primary keys for the same secondary key.
  {
    auto& tx = db_->BeginTransaction();
    ASSERT_TRUE(tx.SetTable(table_name));
    for (size_t i = 0; i < initial_primary_keys; ++i) {
      const std::string primary_key = MakeFixedPrimaryKey(i);
      const std::string value = "val_" + primary_key;
      tx.Write(primary_key, reinterpret_cast<const std::byte*>(value.data()),
               value.size());
      tx.WriteSecondaryIndex(
          index_name, index_key,
          reinterpret_cast<const std::byte*>(primary_key.data()),
          primary_key.size());
    }
    const bool committed = db_->EndTransaction(tx, [](auto) {});
    ASSERT_TRUE(committed);
  }

  std::vector<long long> durations_us;
  std::vector<size_t> log_delta_bytes;
  durations_us.reserve(iterations);
  log_delta_bytes.reserve(iterations);

  // Measure adding one primary key to the same secondary key.
  for (size_t i = 0; i < iterations; ++i) {
    const std::string primary_key =
        MakeFixedPrimaryKey(initial_primary_keys + i);
    const std::string value = "val_" + primary_key;
    const auto size_before = GetLogDirectorySize(config);

    auto& tx = db_->BeginTransaction();
    ASSERT_TRUE(tx.SetTable(table_name));
    tx.Write(primary_key, reinterpret_cast<const std::byte*>(value.data()),
             value.size());
    tx.WriteSecondaryIndex(
        index_name, index_key,
        reinterpret_cast<const std::byte*>(primary_key.data()),
        primary_key.size());

    const auto start = std::chrono::steady_clock::now();
    const bool committed = db_->EndTransaction(tx, [](auto) {});
    const auto end = std::chrono::steady_clock::now();
    ASSERT_TRUE(committed);

    const auto elapsed =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();
    durations_us.push_back(elapsed);

    const auto size_after = GetLogDirectorySize(config);
    log_delta_bytes.push_back(
        size_after >= size_before ? size_after - size_before : 0);

    // Cleanup to keep the secondary key size stable for the next iteration.
    auto& cleanup_tx = db_->BeginTransaction();
    ASSERT_TRUE(cleanup_tx.SetTable(table_name));
    cleanup_tx.DeleteSecondaryIndex(
        index_name, index_key,
        reinterpret_cast<const std::byte*>(primary_key.data()),
        primary_key.size());
    cleanup_tx.Delete(primary_key);
    const bool cleanup_committed = db_->EndTransaction(cleanup_tx, [](auto) {});
    ASSERT_TRUE(cleanup_committed);
  }

  std::vector<long long> sorted = durations_us;
  std::sort(sorted.begin(), sorted.end());
  const auto median = sorted[sorted.size() / 2];

  std::cout << "[SecondaryIndexTiming] iterations=" << iterations
            << " pk_bytes=16"
            << " initial_primary_keys=" << initial_primary_keys
            << " median_us=" << median << " samples_us=[";
  for (size_t i = 0; i < durations_us.size(); ++i) {
    if (i) std::cout << ",";
    std::cout << durations_us[i];
  }
  std::cout << "] log_delta_bytes=[";
  for (size_t i = 0; i < log_delta_bytes.size(); ++i) {
    if (i) std::cout << ",";
    std::cout << log_delta_bytes[i];
  }
  std::cout << "]" << std::endl;
}
