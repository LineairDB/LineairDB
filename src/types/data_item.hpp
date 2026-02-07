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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <memory>
#include <msgpack.hpp>
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
  bool initialized;
  DataBuffer buffer;
  // std::stringのvectorを保持する
  // lineairdvkeyみたいなエイリアス
  std::vector<std::string> primary_keys;
  std::vector<std::string> checkpoint_primary_keys;
  bool checkpoint_primary_keys_captured;
  /* std::unique_ptr<std::vector<DataBuffer>> sec_idx_buffers; */
  DataBuffer checkpoint_buffer;                     // a.k.a. stable version
  std::atomic<NWRPivotObject> pivot_object;         // for NWR
  Lock::ReadersWritersLockBO readers_writers_lock;  // for 2PL

  std::byte* value() { return &buffer.value[0]; }
  const std::byte* value() const { return &buffer.value[0]; }
  size_t size() const { return buffer.size; }
  bool IsInitialized() const { return initialized; }

  DataItem()
      : transaction_id(0),
        initialized(false),
        checkpoint_primary_keys_captured(false),
        pivot_object(NWRPivotObject()) {}
  DataItem(const std::byte* v, size_t s, TransactionId tid = 0)
      : transaction_id(tid),
        initialized(true),
        checkpoint_primary_keys_captured(false),
        pivot_object(NWRPivotObject()) {
    Reset(v, s);
  }
  DataItem(const DataItem& rhs)
      : transaction_id(rhs.transaction_id.load()),
        initialized(rhs.initialized),
        checkpoint_primary_keys_captured(false),
        pivot_object(NWRPivotObject()) {
    buffer.Reset(rhs.buffer);
    /* if (rhs.sec_idx_buffers) {
      sec_idx_buffers =
          std::make_unique<std::vector<DataBuffer>>(*rhs.sec_idx_buffers);
    } */
    primary_keys = rhs.primary_keys;
  }

  DataItem& operator=(const DataItem& rhs) {
    transaction_id.store(rhs.transaction_id.load());
    initialized = rhs.initialized;
    if (initialized) {
      buffer.Reset(rhs.buffer);
    }

    /* if (rhs.sec_idx_buffers) {
      sec_idx_buffers =
          std::make_unique<std::vector<DataBuffer>>(*rhs.sec_idx_buffers);
    } else {
      sec_idx_buffers = nullptr;
    } */
    primary_keys = rhs.primary_keys;
    return *this;
  }

  void Reset(const std::byte* v, const size_t s, TransactionId tid = 0) {
    buffer.Reset(v, s);
    if (!tid.IsEmpty()) transaction_id.store(tid);
    initialized = (v != nullptr && s != 0) || !primary_keys.empty();
  }

  void AddSecondaryIndexValue(const std::byte* v, size_t s) {
    std::string new_key(reinterpret_cast<const char*>(v), s);
    auto it =
        std::lower_bound(primary_keys.begin(), primary_keys.end(), new_key);
    if (it != primary_keys.end() && *it == new_key) return;
    primary_keys.insert(it, std::move(new_key));
    initialized = buffer.size != 0 || !primary_keys.empty();
  }

  void RemoveSecondaryIndexValue(const std::byte* v, size_t s) {
    const std::string target(reinterpret_cast<const char*>(v), s);
    auto it =
        std::lower_bound(primary_keys.begin(), primary_keys.end(), target);
    if (it == primary_keys.end() || *it != target) return;
    primary_keys.erase(it);
    initialized = buffer.size != 0 || !primary_keys.empty();
  }

  void CopyLiveVersionToStableVersion() {
    // There is an assumption that this thread can `exclusively` access this
    // data item.
    if (checkpoint_buffer.IsEmpty()) {
      checkpoint_buffer.Reset(buffer);
    }
    if (!checkpoint_primary_keys_captured) {
      checkpoint_primary_keys = primary_keys;
      checkpoint_primary_keys_captured = true;
    }
  }

  bool HasCheckpointPrimaryKeys() const {
    return checkpoint_primary_keys_captured;
  }

  const std::vector<std::string>& GetCheckpointPrimaryKeys() const {
    return checkpoint_primary_keys;
  }

  void ClearCheckpointPrimaryKeys() {
    checkpoint_primary_keys.clear();
    checkpoint_primary_keys_captured = false;
  }

  void ExclusiveLock() {
    // Acquire exclusive locking for all protocols:

    {
      // for Silo, Silo+NWR. they uses transaction_id as the lock
      for (;;) {
        auto tid = transaction_id.load();
        if (tid.tid & 1llu) {
          std::this_thread::yield();
          continue;
        }
        auto new_tid = tid;
        new_tid.tid += 1llu;
        if (transaction_id.compare_exchange_weak(tid, new_tid)) break;
      }
    }

    // for TwoPhaseLocking. it uses rw_lock.
    { GetRWLockRef().Lock(); }
  }

  void ExclusiveUnlock() {
    // Release exclusive locking for all protocols:

    // for Silo, Silo+NWR. they uses transaction_id as the lock
    {
      auto tid = transaction_id.load();
      tid.tid -= 1llu;
      transaction_id.store(tid);
    }
    // for TwoPhaseLocking. it uses rw_lock.
    { GetRWLockRef().UnLock(); }
  }

  decltype(readers_writers_lock)& GetRWLockRef() {
    return readers_writers_lock;
  };
};
}  // namespace LineairDB
#endif /* LINEAIRDB_DATA_ITEM_HPP */
