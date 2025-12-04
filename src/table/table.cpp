#include "table/table.h"

#include <string_view>

#include "index/concurrent_table.h"
#include "lineairdb/config.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {

Table::Table(EpochFramework& epoch_framework, const Config& config,
             std::string_view table_name)
    : primary_index_(epoch_framework, config), table_name_(table_name) {}

const std::string& Table::GetTableName() const { return table_name_; }
Index::ConcurrentTable& Table::GetPrimaryIndex() { return primary_index_; }

void Table::WaitForIndexIsLinearizable() {
  primary_index_.WaitForIndexIsLinearizable();
}

}  // namespace LineairDB