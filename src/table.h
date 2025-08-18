#pragma once

#include <memory>
#include <shared_mutex>
#include <string>

#include "index/concurrent_table.h"
#include "lineairdb/config.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {

class Table {
 public:
  Table(EpochFramework& epoch_framework, const Config& config);

  Index::ConcurrentTable& GetPrimaryIndex() { return primary_index_; }

 private:
  // EpochFramework& epoch_framework_;
  // Config config_;
  Index::ConcurrentTable primary_index_;
  // mutable std::shared_mutex table_lock_;
};
}  // namespace LineairDB