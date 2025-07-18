#include "lineairdb/table.h"
#include <tuple>
#include <utility>

namespace LineairDB
{
  Table::Table(EpochFramework &epoch_framework, const Config &config)
      : epoch_framework_(epoch_framework),
        config_(config),
        primary_index_(epoch_framework, config) {}

  bool Table::CreateSecondaryIndex(const std::string index_name, const SecondaryIndexOption::Constraint /*constraint*/)
  {
    if (secondary_indices_.find(index_name) != secondary_indices_.end())
    {
      return false;
    }

    auto [it, inserted] = secondary_indices_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(index_name),
        std::forward_as_tuple(epoch_framework_, config_));
    return inserted;
  }
} // namespace LineairDB