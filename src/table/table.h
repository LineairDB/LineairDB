#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "index/concurrent_table.h"
#include "index/secondary_index.h"
#include "lineairdb/config.h"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {

class Table {
 public:
  Table(EpochFramework& epoch_framework, const Config& config,
        std::string_view table_name);

  bool CreateSecondaryIndex(const std::string_view index_name,
                            [[maybe_unused]] const uint index_type) {
    std::unique_lock<std::shared_mutex> lk(table_lock_);
    if (secondary_indices_.count(std::string(index_name))) {
      return false;
    }
    auto new_index = std::make_unique<Index::SecondaryIndex>(
        epoch_framework_, config_, index_type);
    secondary_indices_[std::string(index_name)] = std::move(new_index);
    return true;
  }

  const std::string& GetTableName() const;

  Index::ConcurrentTable& GetPrimaryIndex();

  Index::SecondaryIndex* GetSecondaryIndex(const std::string_view index_name);

  size_t GetSecondaryIndexCount() const {
    std::shared_lock<std::shared_mutex> lk(table_lock_);
    return secondary_indices_.size();
  }

 private:
  EpochFramework& epoch_framework_;
  Config config_;
  Index::ConcurrentTable primary_index_;
  mutable std::shared_mutex table_lock_;
  std::unordered_map<std::string, std::unique_ptr<Index::SecondaryIndex>>
      secondary_indices_;
  std::string table_name_;
};
}  // namespace LineairDB