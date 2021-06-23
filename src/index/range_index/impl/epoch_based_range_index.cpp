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

#include "epoch_based_range_index.h"

#include <atomic>
#include <cassert>
#include <functional>
#include <mutex>
#include <string_view>

#include "types/data_item.hpp"
#include "types/definitions.h"

namespace LineairDB {
namespace Index {

EpochBasedRangeIndex::~EpochBasedRangeIndex(){};

size_t EpochBasedRangeIndex::Scan(
    const std::string_view begin, const std::string_view end,
    std::function<void(std::string_view, std::pair<void*, size_t>)> operation) {
  return 0;
};

}  // namespace Index
}  // namespace LineairDB
