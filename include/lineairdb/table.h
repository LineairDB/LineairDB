#pragma once

#include <unordered_map>

#include "config.h"
#include "index/concurrent_table.h"
#include "index/secondary_index.h"
#include "lineairdb/secondary_index_option.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
class Table {
 public:
  Table(EpochFramework& epoch_framework, const Config& config);
  bool CreateSecondaryIndex(const std::string index_name,
                            const SecondaryIndexOption::Constraint constraint
                            [[maybe_unused]]);
  
  Index::ConcurrentTable& GetPrimaryIndex();

  Index::SecondaryIndex& GetSecondaryIndex(const std::string& index_name);

 private:
  EpochFramework& epoch_framework_;
  Config config_;
  Index::ConcurrentTable primary_index_;
  std::unordered_map<std::string, Index::SecondaryIndex> secondary_indices_;
};
}  // namespace LineairDB