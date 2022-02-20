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

#include <functional>

#include "lineairdb/config.h"
#include "point_index/impl/mpmc_concurrent_set_impl.h"
#include "range_index/impl/precision_locking.h"
#include "types/data_item.hpp"
#include "types/definitions.h"

namespace LineairDB {
namespace Index {

ConcurrentTable::ConcurrentTable(EpochFramework& epoch_framework, Config config,
                                 WriteSetType recovery_set)
    : epoch_manager_ref_(epoch_framework) {
  switch (config.concurrent_point_index) {
    case Config::ConcurrentPointIndex::MPMCConcurrentHashSet:
      point_index_ = std::make_unique<MPMCConcurrentSetImpl>();
      break;
    default:
      point_index_ = std::make_unique<MPMCConcurrentSetImpl>();
      break;
  }
  switch (config.range_index) {
    case Config::RangeIndex::PrecisionLockingIndex:
      range_index_ = std::make_unique<PrecisionLockingIndex>(epoch_manager_ref_);
      break;
    default:
      range_index_ = std::make_unique<PrecisionLockingIndex>(epoch_manager_ref_);
      break;
  }
  if (recovery_set.empty()) return;
  for (auto& entry : recovery_set) {
    point_index_->Put(entry.key, entry.index_cache);
  }
}

ConcurrentTable::~ConcurrentTable() {
  point_index_->ForAllWithExclusiveLock(
      [](const std::string_view, const DataItem* i) {
        assert(i != nullptr);
        delete i;
      });
  point_index_->Clear();
}

DataItem* ConcurrentTable::Get(const std::string_view key) {
  return point_index_->Get(key);
}

DataItem* ConcurrentTable::GetOrInsert(const std::string_view key) {
  auto* item = point_index_->Get(key);
  if (item == nullptr) { return InsertIfNotExist(key); }
  return item;
}

// return false if a corresponding entry already exists
bool ConcurrentTable::Put(const std::string_view key, const DataItem& rhs) {
  auto* value  = new DataItem(rhs);
  bool success = point_index_->Put(key, value);
  if (!success) delete value;
  return range_index_->Insert(key);
}

std::optional<size_t> ConcurrentTable::Scan(
    const std::string_view begin, const std::string_view end,
    std::function<bool(std::string_view)> operation) {
  return range_index_->Scan(begin, end, operation);
};

DataItem* ConcurrentTable::InsertIfNotExist(const std::string_view key) {
  // NOTE We assume that derived table has set semantics;
  // i.e., concurrent #put operations always result in a single winner
  // and the other operations return false.
  Put(key, {nullptr, 0, 0});
  auto current = Get(key);
  assert(current != nullptr);
  return current;
}

void ConcurrentTable::ForEach(
    std::function<bool(std::string_view, DataItem&)> f) {
  point_index_->ForEach(f);
};

}  // namespace Index
}  // namespace LineairDB
