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
#include "types.h"

TEST(ConcurrentTableTest, Instantiate) {
  ASSERT_NO_THROW(LineairDB::Index::ConcurrentTable table);
}

TEST(ConcurrentTableTest, Put) {
  LineairDB::Index::ConcurrentTable table;
  table.Put("alice", new LineairDB::DataItem);
}

TEST(ConcurrentTableTest, Get) {
  LineairDB::Index::ConcurrentTable table;
  ASSERT_EQ(nullptr, table.Get("alice"));
  auto* item = new LineairDB::DataItem;
  table.Put("alice", item);
  ASSERT_EQ(item, table.Get("alice"));
}

TEST(ConcurrentTableTest, GetOrInsert) {
  LineairDB::Index::ConcurrentTable table;
  ASSERT_NE(nullptr, table.GetOrInsert("alice"));
}

TEST(ConcurrentTableTest, ConcurrentInserting) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem*> items;
  for (size_t i = 0; i < 10; i++) {
    items.emplace_back(new LineairDB::DataItem);
  }
  LineairDB::Index::ConcurrentTable table;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() { table.Put(std::to_string(i), items[i]); });
  }
  for (auto& thread : threads) { thread.join(); }
  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(items[i], table.Get(std::to_string(i)));
  }
}

TEST(ConcurrentTableTest, ConcurrentAndConflictedInserting) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem*> items;
  for (size_t i = 0; i < 10; i++) {
    items.emplace_back(new LineairDB::DataItem);
  }
  LineairDB::Index::ConcurrentTable table;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() { table.Put("alice", items[i]); });
  }
  for (auto& thread : threads) { thread.join(); }
  bool some_item_were_inserted = false;
  auto* item                   = table.Get("alice");
  for (size_t i = 0; i < 10; i++) {
    if (item == items[i]) some_item_were_inserted = true;
  }

  ASSERT_TRUE(some_item_were_inserted);
}

TEST(ConcurrentTableTest, TremendousPut) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem*> items;
  LineairDB::Index::ConcurrentTable table;
  constexpr size_t working_set_size = 8192;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = i * working_set_size; j < (i + 1) * working_set_size;
           j++) {
        table.Put(std::to_string(j), new LineairDB::DataItem);
      }
    });
  }
  for (auto& thread : threads) { thread.join(); }
}

TEST(ConcurrentTableTest, TremendousGetAndPut) {
  std::vector<std::thread> threads;
  std::vector<LineairDB::DataItem*> items;
  LineairDB::Index::ConcurrentTable table;
  constexpr size_t working_set_size = 8192;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = i * working_set_size; j < (i + 1) * working_set_size;
           j++) {
        table.Get(std::to_string(j - working_set_size));
        table.Put(std::to_string(j), new LineairDB::DataItem);
      }
    });
  }
  for (auto& thread : threads) { thread.join(); }
}
