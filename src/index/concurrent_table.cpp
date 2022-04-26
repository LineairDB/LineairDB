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

#include "index/precision_locking_index/index.hpp"
#include "lineairdb/config.h"
#include "types/data_item.hpp"
#include "types/definitions.h"

namespace LineairDB {
namespace Index {

ConcurrentTable::ConcurrentTable(EpochFramework& epoch_framework, Config config,
                                 WriteSetType recovery_set)
    : epoch_manager_ref_(epoch_framework) {
  switch (config.index_structure) {
    case Config::IndexStructure::HashTableWithPrecisionLockingIndex:
      index_ = std::make_unique<HashTableWithPrecisionLockingIndex<DataItem>>(
          epoch_manager_ref_);
      break;
    default:
      index_ = std::make_unique<HashTableWithPrecisionLockingIndex<DataItem>>(
          epoch_manager_ref_);
      break;
  }

  if (recovery_set.empty()) return;
  for (auto& entry : recovery_set) {
    index_->Put(entry.key, *entry.index_cache);
  }
}

DataItem* ConcurrentTable::Get(const std::string_view key) {
  return index_->Get(key);
}

DataItem* ConcurrentTable::GetOrInsert(const std::string_view key) {
  auto* item = index_->Get(key);
  if (item == nullptr) {
    index_->ForcePutBlankEntry(key);
    item = index_->Get(key);
    assert(item != nullptr);
  }
  return item;
}

// return false if a corresponding entry already exists
bool ConcurrentTable::Put(const std::string_view key, DataItem&& rhs) {
  return index_->Put(key, std::forward<decltype(rhs)>(rhs));
}

void ConcurrentTable::ForEach(
    std::function<bool(std::string_view, DataItem&)> f) {
  index_->ForEach(f);
};

std::optional<size_t> ConcurrentTable::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view)> operation) {
  return index_->Scan(begin, end, operation);
};

std::optional<size_t> ConcurrentTable::Scan(
    const std::string_view begin, const std::string_view end,
    std::function<bool(std::string_view, DataItem&)> operation) {
  return index_->Scan(begin, end, operation);
};

}  // namespace Index
}  // namespace LineairDB
