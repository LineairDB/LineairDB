#pragma once

#include <memory>
#include <shared_mutex>
#include <string>

#include "index/concurrent_table.h"
#include "lineairdb/config.h"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {

class Table {
 public:
  Table(EpochFramework& epoch_framework, const Config& config,
        std::string_view table_name);

  const std::string& GetTableName() const;
  Index::ConcurrentTable& GetPrimaryIndex();

 private:
  Index::ConcurrentTable primary_index_;
  std::string table_name_;
};
}  // namespace LineairDB