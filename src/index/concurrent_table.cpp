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

#include "concurrent_table.h"

#include <lineairdb/config.h>

#include <functional>

#include "impl/mpmc_concurrent_set_impl.h"
#include "types.h"

namespace LineairDB {
namespace Index {

ConcurrentTable::ConcurrentTable(Config config, WriteSetType recovery_set) {
  switch (config.concurrent_point_index) {
    case Config::ConcurrentPointIndex::MPMCConcurrentHashSet:
      container_ = std::make_unique<MPMCConcurrentSetImpl>();
      break;
    default:
      container_ = std::make_unique<MPMCConcurrentSetImpl>();
      break;
  }

  if (recovery_set.empty()) return;
  for (auto& entry : recovery_set) {
    container_->Put(entry.key, entry.index_cache);
  }
}

ConcurrentTable::~ConcurrentTable() {
  container_->ForAllWithExclusiveLock(
      [](const std::string_view, const DataItem* i) {
        assert(i != nullptr);
        delete i;
      });
  container_->Clear();
}

DataItem* ConcurrentTable::Get(const std::string_view key) {
  return container_->Get(key);
}

DataItem* ConcurrentTable::GetOrInsert(const std::string_view key) {
  auto* item = container_->Get(key);
  if (item == nullptr) { return InsertIfNotExist(key); }
  return item;
}

// return false if a corresponding entry already exists
bool ConcurrentTable::Put(const std::string_view key, const DataItem& rhs) {
  auto* value  = new DataItem(rhs);
  bool success = container_->Put(key, value);
  if (!success) delete value;
  return success;
}

DataItem* ConcurrentTable::InsertIfNotExist(const std::string_view key) {
  // NOTE We assume that derived table has set semantics;
  // i.e., concurrent #put operations always result in a single winner
  // and the other operations return false.
  Put(key, {nullptr, 0, 0});
  auto current = Get(key);
  assert(current != nullptr);
  return current;
}
}  // namespace Index
}  // namespace LineairDB
