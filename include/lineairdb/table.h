#pragma once

#include <unordered_map>

#include "index/concurrent_table.h"
#include "epoch_framework.h"
#include "config.h"

namespace LineairDB
{
    class Table
    {
    public:
        Table(EpochFramework &epoch_framework, const Config &config);

    private:
        EpochFramework& epoch_framework_;
        Config config_;
        Index::ConcurrentTable primary_index_;
        std::unordered_map<std::string, Index::ConcurrentTable> secondary_indices_;
};
}  // namespace LineairDB