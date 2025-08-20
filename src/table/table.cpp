#include "table.h"

#include <shared_mutex>
#include <tuple>
#include <utility>

namespace LineairDB {
Table::Table(EpochFramework& epoch_framework, const Config& config)
    : primary_index_(epoch_framework, config) {}
}  // namespace LineairDB