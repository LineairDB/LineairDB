/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.

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

#ifndef LINEAIRDB_DATA_ITEM_HPP
#define LINEAIRDB_DATA_ITEM_HPP

#include <atomic>
#include <cstddef>
#include <cstring>
#include <msgpack.hpp>
#include <string>
#include <type_traits>
#include <vector>

#include "concurrency_control/pivot_object.hpp"
#include "data_buffer.hpp"
#include "lock/impl/readers_writers_lock.hpp"
#include "types/transaction_id.hpp"
#include "util/logger.hpp"

namespace LineairDB {

struct DataItem {
  std::atomic<TransactionId> transaction_id;
  DataBuffer buffer;
  std::atomic<NWRPivotObject> pivot_object;  // for NWR
  enum class CCTag { NotInitialized, RW_LOCK } cc_tag;
  union {
    Lock::ReadersWritersLockBO readers_writers_lock;  // for 2PL
  };

  std::byte* value() { return &buffer.value[0]; }
  const std::byte* value() const { return &buffer.value[0]; }
  size_t size() const { return buffer.size; }

  DataItem() : pivot_object(NWRPivotObject()), cc_tag(CCTag::NotInitialized) {}
  DataItem(const std::byte* v, size_t s, TransactionId tid = 0)
      : transaction_id(tid),
        pivot_object(NWRPivotObject()),
        cc_tag(CCTag::NotInitialized) {
    Reset(v, s);
  }
  DataItem(const DataItem& rhs)
      : transaction_id(rhs.transaction_id.load()),
        pivot_object(NWRPivotObject()),
        cc_tag(CCTag::NotInitialized) {
    buffer.Reset(rhs.buffer);
  }
  DataItem& operator=(const DataItem& rhs) {
    transaction_id.store(rhs.transaction_id.load());
    buffer.Reset(rhs.buffer);
    return *this;
  }

  void Reset(const std::byte* v, const size_t s, TransactionId tid = 0) {
    buffer.Reset(v, s);
    if (!tid.IsEmpty()) transaction_id.store(tid);
  }

  auto& GetRWLockRef() {
    if (cc_tag != CCTag::RW_LOCK) {
      new (&readers_writers_lock) decltype(readers_writers_lock);
      cc_tag = CCTag::RW_LOCK;
    }
    return readers_writers_lock;
  }
};
}  // namespace LineairDB
#endif /* LINEAIRDB_DATA_ITEM_HPP */
