#include "table/table.h"

#include <shared_mutex>
#include <string_view>
#include <tuple>
#include <utility>

#include "index/concurrent_table.h"
#include "lineairdb/config.h"
#include "util/epoch_framework.hpp"
// #include "index/secondary_index.h"  // now included from table.h

namespace LineairDB {
Table::Table(EpochFramework& epoch_framework, const Config& config)
    : epoch_framework_(epoch_framework),
      config_(config),
      primary_index_(epoch_framework, config) {}

Index::SecondaryIndexInterface* Table::GetSecondaryIndex(
    const std::string_view index_name) {
  std::shared_lock<std::shared_mutex> lk(table_lock_);
  auto it = secondary_indices_.find(std::string(index_name));
  if (it == secondary_indices_.end()) {
    return nullptr;
  }
  return it->second.get();
}

const std::string& Table::GetTableName() const { return table_name_; }
Index::ConcurrentTable& Table::GetPrimaryIndex() { return primary_index_; }

}  // namespace LineairDB