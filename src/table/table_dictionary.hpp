#pragma once

#include <memory>
#include <string>
#include <utility>

#include "index/precision_locking_index/point_index/mpmc_concurrent_set_impl.hpp"
#include "table/table.h"

namespace LineairDB {
class TableDictionary {
 public:
  TableDictionary() = default;
  ~TableDictionary() = default;

  bool CreateTable(std::string_view table_name, EpochFramework& epoch_framework,
                   const Config& config) {
    if (tables_.Get(table_name) != nullptr) {
      return false;
    }
    auto* tbl = new Table(epoch_framework, config, std::string(table_name));
    auto inserted = tables_.Put(table_name, tbl);
    if (!inserted) delete tbl;
    return true;
  }

  std::optional<Table*> GetTable(const std::string_view table_name) {
    auto* table = tables_.Get(table_name);
    if (table == nullptr) {
      return std::nullopt;
    }
    return table;
  }

  void ForEachTable(std::function<void(Table&)> f) {
    tables_.ForEach([&](std::string_view, Table& table) {
      f(table);
      return true;
    });
  }

 private:
  LineairDB::Index::MPMCConcurrentSetImpl<Table> tables_;
};

}  // namespace LineairDB