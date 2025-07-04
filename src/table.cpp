#include "lineairdb/table.h"

namespace LineairDB {
    Table::Table(EpochFramework &epoch_framework, const Config &config)
        : primary_index_(epoch_framework, config),
          epoch_framework_(epoch_framework), 
          config_(config) {}
}  // namespace LineairDB