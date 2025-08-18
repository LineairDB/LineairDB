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

#ifndef LINEAIRDB_SECONDARY_INDEX_H
#define LINEAIRDB_SECONDARY_INDEX_H

#include <lineairdb/config.h>

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "index/precision_locking_index/index.hpp"
#include "index/secondary_index_interface.h"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "types/snapshot.hpp"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

template <typename K>
class SecondaryIndex : public SecondaryIndexInterface {
 public:
  using KeyType = K;

  SecondaryIndex(EpochFramework& epoch_framework, Config config = Config(),
                 bool is_unique = false,
                 [[maybe_unused]] WriteSetType recovery_set = WriteSetType())
      : secondary_index_(
            std::make_unique<HashTableWithPrecisionLockingIndex<DataItem>>(
                config, epoch_framework)),
        epoch_manager_ref_(epoch_framework),
        is_unique_(is_unique) {}

  const std::type_info& KeyTypeInfo() const override { return typeid(KeyType); }

  DataItem* Get(std::string_view serialized_key) override {
    return secondary_index_->Get(serialized_key);
  }

  DataItem* GetOrInsert(std::string_view serialized_key) override {
    auto* item = secondary_index_->Get(serialized_key);
    if (item == nullptr) {
      secondary_index_->ForcePutBlankEntry(serialized_key);
      item = secondary_index_->Get(serialized_key);
      assert(item != nullptr);
    }
    return item;
  }

  std::optional<size_t> Scan(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view)> operation) override {
    return secondary_index_->Scan(begin, end, operation);
  }

  bool IsUnique() const override { return is_unique_; }

  bool DeleteKey(std::string_view serialized_key) override {
    // Range-only deletion to hide the key from scans.
    // Point index does not support erase currently.
    // Implement as a method on HashTableWithPrecisionLockingIndex when
    // available.
    return secondary_index_->EraseRangeOnly(serialized_key);
  }

 private:
  std::unique_ptr<HashTableWithPrecisionLockingIndex<DataItem>>
      secondary_index_;
  LineairDB::EpochFramework& epoch_manager_ref_;
  bool is_unique_;
  std::mutex secondary_index_lock_;
};

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_SECONDARY_INDEX_H */
