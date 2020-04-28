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

typedef uint32_t EpochNumber;

// TODO set this parameter by configuration
constexpr size_t ValueBufferSize = 512;

struct DataItem {
  std::atomic<uint64_t> transaction_id;
  std::byte value[ValueBufferSize];
  size_t size;
  std::atomic<NWRPivotObject>
      pivot_object;  // Used by only NWR-extended protocols

  DataItem() : transaction_id(0), size(0), pivot_object() {}
  DataItem(const std::byte* v, size_t s, uint64_t tid = 0)
      : transaction_id(tid), size(0), pivot_object() {
    Reset(v, s);
  }

  void Reset(const std::byte* v, size_t s) {
    if (ValueBufferSize < s) {
      SPDLOG_ERROR("write buffer overflow. expected: {0}, capacity: {1}", s,
                   ValueBufferSize);
      exit(EXIT_FAILURE);
    }
    size = s;
    std::memcpy(value, v, s);
  }
};

struct Snapshot {
  std::string key;
  std::byte value_copy[ValueBufferSize];
  size_t size;
  DataItem* index_cache;
  uint64_t version_in_epoch;
  bool is_read_modify_write;

  Snapshot(const std::string_view k, const std::byte v[], const size_t s,
           DataItem* const i, const uint64_t ver = 0)
      : key(k),
        size(s),
        index_cache(i),
        version_in_epoch(ver),
        is_read_modify_write(false) {
    if (v != nullptr) Reset(v, s);
  }

  static bool Compare(Snapshot& left, Snapshot& right) {
    return left.key < right.key;
  }

  void Reset(const std::byte* v, size_t s) {
    if (ValueBufferSize < s) {
      SPDLOG_ERROR("write buffer overflow. expected: {0}, capacity: {1}", s,
                   ValueBufferSize);
      exit(EXIT_FAILURE);
    }
    size = s;
    std::memcpy(value_copy, v, s);
  }
};

typedef std::vector<Snapshot> ReadSetType;
typedef std::vector<Snapshot> WriteSetType;

}  // namespace LineairDB

#endif /* LINEAIRDB_TYPES_H */
