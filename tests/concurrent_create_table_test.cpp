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
    config_.max_thread = 1;
    config_.checkpoint_period = 1;
    config_.epoch_duration_ms = 100;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(ConcurrentCreateTableTest, ConcurrentCreateTableAndCheckpoint) {
  constexpr size_t kNumWorkers = 4;
  constexpr size_t kTablesPerSec = 100;

  std::atomic<bool> stop{false};

  std::vector<std::thread> workers;
  for (size_t t = 0; t < kNumWorkers; ++t) {
    workers.emplace_back([&]() {
      size_t i = 0;
      while (!stop.load()) {
        const std::string table_name =
            "table_" + std::to_string(i % kTablesPerSec);
        const bool created = db_->CreateTable(table_name);
        if (created) {
        } else {
          EXPECT_FALSE(created);
        }
        ++i;
        if (i % 128 == 0) std::this_thread::yield();
      }
    });
  }

  // Wait for two checkpoints after all workers have started running
  db_->WaitForCheckpoint();
  db_->WaitForCheckpoint();

  stop.store(true);
  for (auto& w : workers) {
    w.join();
  }
}
