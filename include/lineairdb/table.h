#pragma once

#include <memory>
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
      const std::string& index_name,
      [[maybe_unused]] const SecondaryIndexOption::Constraint constraint) {
    if (secondary_indices_.count(index_name)) {
      return false;
    }
    bool is_unique = constraint == SecondaryIndexOption::Constraint::UNIQUE;
    auto new_index = std::make_unique<Index::SecondaryIndex<T>>(
        epoch_framework_, config_, is_unique);
    secondary_indices_[index_name] = std::move(new_index);
    return true;
  }

  Index::ISecondaryIndex* GetSecondaryIndex(const std::string& index_name);

 private:
  EpochFramework& epoch_framework_;
  Config config_;
  Index::ConcurrentTable primary_index_;

  std::unordered_map<std::string, std::unique_ptr<Index::ISecondaryIndex>>
      secondary_indices_;
};
}  // namespace LineairDB