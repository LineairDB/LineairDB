#include "lineairdb/table.h"

#include <tuple>
#include <utility>
// #include "index/secondary_index.h"  // now included from table.h

namespace LineairDB {
Table::Table(EpochFramework& epoch_framework, const Config& config)
    : epoch_framework_(epoch_framework),
      config_(config),
      primary_index_(epoch_framework, config) {}

Index::ISecondaryIndex* Table::GetSecondaryIndex(
    const std::string& index_name) {
  auto it = secondary_indices_.find(index_name);
  if (it == secondary_indices_.end()) {
    return nullptr;
  }
  return it->second.get();
}
}  // namespace LineairDB