#pragma once

#include "index/concurrent_table.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

class SecondaryIndex {
 public:
  SecondaryIndex(EpochFramework& epoch_framework, Config config = Config(),
                 uint index_type = 0,
                 [[maybe_unused]] WriteSetType recovery_set = WriteSetType())
      : index_type_(index_type) {
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

  bool Delete(std::string_view key) {
    // Range-only deletion to hide the key from scans.
    // Point index does not support erase currently.
    // Implement as a method on HashTableWithPrecisionLockingIndex when
    // available.
    return secondary_index_->Delete(key);
  }

  void ForEach(std::function<bool(std::string_view, DataItem&)> f) {
    secondary_index_->ForEach(f);
  }

  bool Put(const std::string_view key, DataItem&& value) {
    return secondary_index_->Put(key, std::forward<DataItem>(value));
  }

  bool IsUnique() { return index_type_ == 1; }
  void EnsureRangeEntryForExistingPoint(const std::string_view key) {
    if (!secondary_index_->HasPointEntry(key)) return;
    if (secondary_index_->HasRangeEntry(key)) return;
    secondary_index_->ForceInsertRange(key);
  }

  void WaitForIndexIsLinearizable() {
    secondary_index_->WaitForIndexIsLinearizable();
  }

 private:
  uint index_type_;
  std::unique_ptr<HashTableWithPrecisionLockingIndex<DataItem>>
      secondary_index_;
};
}  // namespace Index
}  // namespace LineairDB
