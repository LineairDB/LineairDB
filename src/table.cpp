#include "table.h"

#include <shared_mutex>
#include <tuple>
#include <utility>
// #include "index/secondary_index.h"  // now included from table.h

namespace LineairDB {
Table::Table(EpochFramework& epoch_framework, const Config& config)
    : epoch_framework_(epoch_framework),
      config_(config),
      primary_index_(epoch_framework, config) {}
}  // namespace LineairDB