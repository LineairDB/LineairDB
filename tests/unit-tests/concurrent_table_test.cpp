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

#include "index/concurrent_table.h"

#include <thread>

#include "gtest/gtest.h"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"
#include "util/logger.hpp"

TEST(ConcurrentTableTest, Instantiate) {
  LineairDB::EpochFramework epoch;
  epoch.Start();
  ASSERT_NO_THROW(LineairDB::Index::ConcurrentTable table(epoch));
}

TEST(ConcurrentTableTest, Put) {
  LineairDB::EpochFramework epoch;
  epoch.Start();
  LineairDB::Index::ConcurrentTable table(epoch);
  table.Put("alice", LineairDB::DataItem{});
}

TEST(ConcurrentTableTest, Get) {
  LineairDB::EpochFramework epoch;
  epoch.Start();
  LineairDB::Index::ConcurrentTable table(epoch);
  ASSERT_EQ(nullptr, table.Get("alice"));
  table.Put("alice", {});
  ASSERT_NE(nullptr, table.Get("alice"));
}

TEST(ConcurrentTableTest, GetOrInsert) {
  LineairDB::EpochFramework epoch;
  epoch.Start();
  LineairDB::Index::ConcurrentTable table(epoch);
  ASSERT_NE(nullptr, table.GetOrInsert("alice"));
}

TEST(ConcurrentTableTest, ConcurrentInserting) {
  std::vector<std::thread> threads;
  LineairDB::EpochFramework epoch;
  epoch.Start();
  LineairDB::Index::ConcurrentTable table(epoch);

  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() { table.Put(std::to_string(i), {}); });
  }
  for (auto& thread : threads) { thread.join(); }
  for (size_t i = 0; i < 10; i++) {
    ASSERT_NE(nullptr, table.Get(std::to_string(i)));
  }
}

TEST(ConcurrentTableTest, ConcurrentAndConflictedInserting) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem> items(10);
  LineairDB::EpochFramework epoch;
  epoch.Start();
  LineairDB::Index::ConcurrentTable table(epoch);

  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&]() { table.Put("alice", {}); });
  }
  for (auto& thread : threads) { thread.join(); }
  bool some_item_were_inserted = false;
  auto* item                   = table.Get("alice");
  for (size_t i = 0; i < 10; i++) {
    if (item != nullptr) some_item_were_inserted = true;
  }

  ASSERT_TRUE(some_item_were_inserted);
}

TEST(ConcurrentTableTest, Scan) {
  LineairDB::Util::SetUpSPDLog();
  LineairDB::EpochFramework epoch;
  epoch.Start();
  LineairDB::Index::ConcurrentTable table(epoch);
  ASSERT_TRUE(table.Put("alice", {}));
  ASSERT_TRUE(table.Put("bob", {}));
  ASSERT_TRUE(table.Put("carol", {}));

  auto count = table.Scan("alice", "carol", [](auto) { return false; });
  if (count.has_value()) { ASSERT_EQ(3, count.value()); }
  epoch.Sync();
  epoch.Sync();
  auto count_synced = table.Scan("alice", "carol", [](auto) { return false; });

  if (count_synced.has_value()) { ASSERT_EQ(3, count_synced.value()); }

  auto count_canceled = table.Scan("alice", "carol", [](auto) { return true; });
  if (count_canceled.has_value()) { ASSERT_EQ(1, count_canceled.value()); }
}

TEST(ConcurrentTableTest, TremendousPut) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem*> items;
  LineairDB::EpochFramework epoch;
  epoch.Start();
  LineairDB::Index::ConcurrentTable table(epoch);

  constexpr size_t working_set_size = 8192;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = i * working_set_size; j < (i + 1) * working_set_size;
           j++) {
        table.Put(std::to_string(j), {});
      }
    });
  }
  for (auto& thread : threads) { thread.join(); }
}

TEST(ConcurrentTableTest, TremendousGetAndPut) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem*> items;
  LineairDB::EpochFramework epoch;
  epoch.Start();
  LineairDB::Index::ConcurrentTable table(epoch);

  constexpr size_t working_set_size = 8192;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = i * working_set_size; j < (i + 1) * working_set_size;
           j++) {
        table.Get(std::to_string(j - working_set_size));
        table.Put(std::to_string(j), {});
      }
    });
  }
  for (auto& thread : threads) { thread.join(); }
}

TEST(ConcurrentTableTest, ForEachIsSafeWithRehashing) {
  // Test scenario: #Rehash and #ForEach are concurrently executed.
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem*> items;
  LineairDB::EpochFramework epoch(1);
  LineairDB::Config config;
  config.rehash_threshold = 0.3;
  epoch.Start();
  LineairDB::Index::ConcurrentTable table(epoch);

  constexpr size_t working_set_size = 8192;
  for (size_t i = 0; i < 5; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = i * working_set_size; j < (i + 1) * working_set_size;
           j++) {
        table.Put(std::to_string(j), {});
      }
    });
  }
  for (size_t i = 0; i < 5; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = 0; j < 100; j++) {
        table.ForEach([](auto, auto) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          return true;
        });
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
