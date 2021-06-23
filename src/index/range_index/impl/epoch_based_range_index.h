/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef LINEAIRDB_INDEX_EPOCH_BASED_RANGE_INDEX_H
#define LINEAIRDB_INDEX_EPOCH_BASED_RANGE_INDEX_H

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <string_view>
#include <vector>

#include "index/range_index/range_index_base.h"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

/**
 * @brief
 * Multi-Producer Multi-Consumer (MPMC) hash-table,
 * based on the open addressing & linear-probing strategy.
 * @note We focus on the performance for reads (gets), not for writes (puts).
 * In other words, we provide lock-free #Get and (maybe locking) #Put.
 * This is because LineairDB requires that point-indexes have to
 * hold only indirection pointer to each data item; once an indirection is
 * created and stored into the index, it will not be changed by #puts.
 */
class EpochBasedRangeIndex final : public RangeIndexBase {
 public:
  ~EpochBasedRangeIndex() final override;
  size_t Scan(const std::string_view begin, const std::string_view end,
              std::function<void(std::string_view, std::pair<void*, size_t>)>
                  operation) final override;

 private:
};
}  // namespace Index
}  // namespace LineairDB

#endif /*  LINEAIRDB_INDEX_EPOCH_BASED_RANGE_INDEX_H*/
