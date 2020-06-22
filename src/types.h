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
#include <msgpack.hpp>
#include <string>
#include <type_traits>
#include <vector>

#include "concurrency_control/pivot_object.hpp"
#include "lock/impl/readers_writers_lock.hpp"
#include "util/logger.hpp"

namespace LineairDB {

using EpochNumber = uint32_t;

// TODO set this parameter by configuration
constexpr size_t ValueBufferSize = 512;

struct TransactionId {
  EpochNumber epoch;
  uint32_t tid;

  TransactionId() noexcept : epoch(0), tid(0) {}
  TransactionId(const EpochNumber e, uint32_t t) : epoch(e), tid(t) {}
  // TransactionId(const TransactionId& rhs) : epoch(rhs.epoch), tid(rhs.tid) {}
  TransactionId(const TransactionId& rhs) = default;
  bool IsEmpty() { return (epoch == 0 && tid == 0); }
  TransactionId(uint64_t n) : epoch(n >> 32), tid(n & ~1llu >> 32) {}
  bool operator==(const TransactionId& rhs) {
    return (epoch == rhs.epoch && tid == rhs.tid);
  }
  bool operator!=(const TransactionId& rhs) { return !(*this == rhs); }
  bool operator<(const TransactionId& rhs) {
    if (epoch == rhs.epoch) {
      return tid < rhs.tid;
    } else {
      return epoch < rhs.epoch;
    }
  }
  MSGPACK_DEFINE(epoch, tid);
};

struct DataItem {
  std::atomic<TransactionId> transaction_id;
  std::byte value[ValueBufferSize];
  size_t size;
  std::atomic<NWRPivotObject> pivot_object;  // for NWR
  enum class CCTag { NotInitialized, RW_LOCK } cc_tag;
  union {
    Lock::ReadersWritersLockBO readers_writers_lock;  // for 2PL
  };

  DataItem() : size(0), cc_tag(CCTag::NotInitialized) {}
  DataItem(const std::byte* v, size_t s, TransactionId tid)
      : transaction_id(tid), size(0), cc_tag(CCTag::NotInitialized) {
    Reset(v, s);
  }
  DataItem(const DataItem& rhs)
      : transaction_id(rhs.transaction_id.load()),
        cc_tag(CCTag::NotInitialized) {
    Reset(rhs.value, rhs.size);
  }
  DataItem& operator=(const DataItem& rhs) {
    transaction_id.store(rhs.transaction_id.load());
    Reset(rhs.value, rhs.size);
    return *this;
  }

  void Reset(const std::byte* v, const size_t s, TransactionId tid = 0) {
    if (ValueBufferSize < s) {
      SPDLOG_ERROR("write buffer overflow. expected: {0}, capacity: {1}", s,
                   ValueBufferSize);
      exit(EXIT_FAILURE);
    }
    size = s;
    std::memcpy(value, v, s);
    if (!tid.IsEmpty()) transaction_id.store(tid);
  }

  decltype(readers_writers_lock)& GetRWLockRef() {
    if (cc_tag != CCTag::RW_LOCK) {
      new (&readers_writers_lock) decltype(readers_writers_lock);
      cc_tag = CCTag::RW_LOCK;
    }
    return readers_writers_lock;
  }
};

struct Snapshot {
  std::string key;
  DataItem data_item_copy;
  DataItem* index_cache;
  bool is_read_modify_write;

  Snapshot(const std::string_view k, const std::byte v[], const size_t s,
           DataItem* const i, const TransactionId ver = 0)
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
