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

#ifndef LINEAIRDB_TYPES_H
#define LINEAIRDB_TYPES_H

#include <cstddef>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>

#include "concurrency_control/pivot_object.hpp"
#include "util/logger.hpp"

namespace LineairDB {

using EpochNumber = uint32_t;

// TODO set this parameter by configuration
constexpr size_t ValueBufferSize = 512;

struct DataItem {
  std::atomic<uint64_t> transaction_id;
  std::byte value[ValueBufferSize];
  size_t size;
  enum class CCTag { NWR } cc_tag;
  union {
    std::atomic<NWRPivotObject> pivot_object;
  };

  DataItem() : transaction_id(0), size(0), cc_tag(CCTag::NWR) {}
  DataItem(const std::byte* v, size_t s, uint64_t tid = 0)
      : transaction_id(tid), size(0), cc_tag(CCTag::NWR) {
    Reset(v, s);
  }
  DataItem(const DataItem& rhs)
      : transaction_id(rhs.transaction_id.load()), cc_tag(CCTag::NWR) {
    Reset(rhs.value, rhs.size);
  }
  DataItem& operator=(const DataItem& rhs) {
    transaction_id.store(rhs.transaction_id.load());
    Reset(rhs.value, rhs.size);
    return *this;
  }

  void Reset(const std::byte* v, size_t s, uint64_t tid = 0) {
    if (ValueBufferSize < s) {
      SPDLOG_ERROR("write buffer overflow. expected: {0}, capacity: {1}", s,
                   ValueBufferSize);
      exit(EXIT_FAILURE);
    }
    size = s;
    std::memcpy(value, v, s);
    if (tid != 0) transaction_id.store(tid);
  }

  std::atomic<NWRPivotObject>& GetNWRPivotObjectRef() {
    if (cc_tag != CCTag::NWR) { pivot_object.store({}); }
    return pivot_object;
  }
};

struct Snapshot {
  std::string key;
  DataItem data_item_copy;
  DataItem* index_cache;
  bool is_read_modify_write;

  Snapshot(const std::string_view k, const std::byte v[], const size_t s,
           DataItem* const i, const uint64_t ver = 0)
      : key(k), index_cache(i), is_read_modify_write(false) {
    if (v != nullptr) data_item_copy.Reset(v, s, ver);
  }
  Snapshot(const Snapshot& rhs) = default;

  static bool Compare(Snapshot& left, Snapshot& right) {
    return left.key < right.key;
  }
};

using ReadSetType  = std::vector<Snapshot>;
using WriteSetType = std::vector<Snapshot>;

}  // namespace LineairDB

#endif /* LINEAIRDB_TYPES_H */
