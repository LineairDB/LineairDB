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
#include "lineairdb/transaction.h"
#include "lineairdb/tx_status.h"
#include "test_helper.hpp"

class ConcurrentCreateTableTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread = 4;
    config_.checkpoint_period = 1;
    config_.epoch_duration_ms = 100;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(ConcurrentCreateTableTest, RepeatedlyCreateTable) {
  constexpr size_t kNumThreads = 4;
  constexpr size_t kNumTablesPerThread = 100;

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);

  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([this, i]() {
      for (size_t j = 0; j < kNumTablesPerThread; ++j) {
        std::string table_name =
            "table_" + std::to_string(i) + "_" + std::to_string(j);
        bool success = db_->CreateTable(table_name.c_str());
        ASSERT_TRUE(success);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(ConcurrentCreateTableTest, ConcurrentCreateTableAndCheckpoint) {
  constexpr size_t kMaxTables = 1000;
  std::thread worker([&]() {
    for (size_t i = 0; i < kMaxTables; ++i) {
      const std::string table_name = "table_" + std::to_string(i);
      (void)db_->CreateTable(table_name);
      if (i % 16 == 0) std::this_thread::yield();
    }
  });

  db_->WaitForCheckpoint();

  worker.join();
}
