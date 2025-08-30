#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "table/table.h"

namespace LineairDB {
class TableDictionary {
 public:
  TableDictionary() = default;
  ~TableDictionary() = default;

  bool CreateTable(std::string_view table_name, EpochFramework& epoch_framework,
                   const Config& config) {
    std::unique_lock<std::shared_mutex> lk(schema_mutex_);
    if (tables_.find(std::string(table_name)) != tables_.end()) {
      return false;
    }
    auto table = std::make_unique<Table>(epoch_framework, config,
                                         std::string(table_name));
    auto [it, inserted] =
        tables_.emplace(std::string(table_name), std::move(table));
    return inserted;
  }

  std::optional<Table*> GetTable(const std::string_view table_name) {
    std::shared_lock lk(schema_mutex_);
    auto it = tables_.find(std::string(table_name));
    if (it == tables_.end()) {
      return std::nullopt;  // Table not found
    }
    return it->second.get();
  }

  void ForEachTable(std::function<void(Table&)> f) {
    std::shared_lock lk(schema_mutex_);
    for (auto& [name, table] : tables_) {
      f(*table);
    }
  }

 private:
  std::unordered_map<std::string, std::unique_ptr<Table>> tables_;

  // @TODO  We should remove this mutex by using concurrent hash map (e.g.,
  // MPMCConcurrentSet in index/precision_locking_index/point_index directory)
  // It is very inefficient to acquire this lock for every read write
  // transactions only for GetTable().
  std::shared_mutex schema_mutex_;
};

}  // namespace LineairDB