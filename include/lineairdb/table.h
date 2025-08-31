#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "config.h"
#include "index/concurrent_table.h"
#include "index/secondary_index.h"
#include "lineairdb/i_secondary_index.h"
#include "lineairdb/secondary_index_option.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {

class Table {
 public:
  Table(EpochFramework& epoch_framework, const Config& config);

  template <typename T>
  bool CreateSecondaryIndex(
      const std::string_view index_name,
      [[maybe_unused]] const SecondaryIndexOption::Constraint constraint) {
    std::unique_lock<std::shared_mutex> lk(table_lock_);
    if (secondary_indices_.count(std::string(index_name))) {
      return false;
    }
    bool is_unique = constraint == SecondaryIndexOption::Constraint::UNIQUE;
    auto new_index = std::make_unique<Index::SecondaryIndex<T>>(
        epoch_framework_, config_, is_unique);
    secondary_indices_[std::string(index_name)] = std::move(new_index);
    return true;
  }

  Index::ConcurrentTable& GetPrimaryIndex() { return primary_index_; }

  Index::ISecondaryIndex* GetSecondaryIndex(const std::string_view index_name);

  size_t GetSecondaryIndexCount() const {
    std::shared_lock<std::shared_mutex> lk(table_lock_);
    return secondary_indices_.size();
  }

 private:
  EpochFramework& epoch_framework_;
  Config config_;
  Index::ConcurrentTable primary_index_;
  mutable std::shared_mutex table_lock_;
  std::unordered_map<std::string, std::unique_ptr<Index::ISecondaryIndex>>
      secondary_indices_;
};
}  // namespace LineairDB