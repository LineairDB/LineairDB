#pragma once

#include "index/concurrent_table.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

class SecondaryIndex {
 public:
  SecondaryIndex(EpochFramework& epoch_framework, Config config = Config(),
                 uint index_type = 0, uint data_type = 0,
                 [[maybe_unused]] WriteSetType recovery_set = WriteSetType())
      : index_type_(index_type), data_type_(data_type) {
    secondary_index_ =
        std::make_unique<HashTableWithPrecisionLockingIndex<DataItem>>(
            config, epoch_framework);
  }

  DataItem* Get(std::string_view key) { return secondary_index_->Get(key); }

  DataItem* GetOrInsert(std::string_view key) {
    auto* item = secondary_index_->Get(key);
    if (item == nullptr) {
      secondary_index_->ForcePutBlankEntry(key);
      item = secondary_index_->Get(key);
      assert(item != nullptr);
    }
    return item;
  }

  std::optional<size_t> Scan(std::string_view begin,
                             std::optional<std::string_view> end,
                             std::function<bool(std::string_view)> operation) {
    return secondary_index_->Scan(begin, end, operation);
  }

  bool DeleteKey(std::string_view key) {
    // Range-only deletion to hide the key from scans.
    // Point index does not support erase currently.
    // Implement as a method on HashTableWithPrecisionLockingIndex when
    // available.
    return secondary_index_->EraseRangeOnly(key);
  }

  bool IsUnique() { return index_type_ == 1; }

 private:
  uint index_type_;
  uint data_type_;
  std::unique_ptr<HashTableWithPrecisionLockingIndex<DataItem>>
      secondary_index_;

  std::string serialized_key_buffer_;
};
}  // namespace Index
}  // namespace LineairDB