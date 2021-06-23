/*
 *   Copyright (c) 2021 Nippon Telegraph and Telephone Corporation
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

#include "gtest/gtest.h"
#include "types/data_item.hpp"
#include "types/definitions.h"

TEST(IndexTest, Instantiate) {
  ASSERT_NO_THROW(LineairDB::Index::RangeIndex index);
}

TEST(IndexTest, Put) {
  LineairDB::Index::RangeIndex index;
  index.Put("alice", LineairDB::DataItem{});
}

TEST(IndexTest, Get) {
  LineairDB::Index::RangeIndex index;
  ASSERT_EQ(nullptr, index.Get("alice"));
  index.Put("alice", LineairDB::DataItem{});
  ASSERT_NE(nullptr, index.Get("alice"));
}

TEST(IndexTest, Scan) {
  LineairDB::Index::RangeIndex index;
  index.Put("alice", LineairDB::DataItem{});
  index.Put("bob", LineairDB::DataItem{});
  index.Put("carol", LineairDB::DataItem{});

  auto count = index.Scan("alice", "carol", [] {}());
  ASSERT_EQ(0, count);
  index.sync();

  auto count_synced = index.Scan("alice", "carol", []() {});
  ASSERT_EQ(2, count_synced);
}

TEST(IndexTest, ScanWithPhantomAvoidance) {
  LineairDB::Index::RangeIndex index;
  index.Put("alice", LineairDB::DataItem{});
  index.Put("bob", LineairDB::DataItem{});
  auto count = index.Scan("alice", "carol", []() {});
  ASSERT_EQ(0, count);

  index.sync();

  /** interleaving **/
  index.Put("dave", LineairDB::DataItem{});

  auto count_synced = index.Scan("alice", "carol", []() {});
  ASSERT_EQ(2, count_synced);
}