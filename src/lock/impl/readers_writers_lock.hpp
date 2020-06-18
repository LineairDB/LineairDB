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

#ifndef LINEAIRDB_READERS_WRITERS_LOCK_HPP
#define LINEAIRDB_READERS_WRITERS_LOCK_HPP

#include <atomic>
#include <cassert>
#include <chrono>
#include <thread>

#include "lock/lock.h"

namespace LineairDB {

namespace Lock {

template <bool EnableBackoff = false, bool EnableCohort = false>
class ReadersWritersLockImpl
    : LockBase<ReadersWritersLockImpl<EnableBackoff, EnableCohort>> {
 public:
  enum class LockType { Exclusive, Shared, Upgrade };
  ReadersWritersLockImpl() : lock_bit_(UnLocked) {}
  void Lock(LockType type = LockType::Exclusive) {
    [[maybe_unused]] size_t sleep_ns = 100;
    while (!TryLock(type)) {
      if constexpr (EnableBackoff) {
        std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_ns));
        sleep_ns *= 2;  // Exponential Backoff
      } else {
        std::this_thread::yield();
      }
    }
  }

  bool TryLock(LockType type = LockType::Exclusive) {
    if (type == LockType::Shared) {
      /** Acquire Readers (shared) Lock **/
      auto current = lock_bit_.load();
      if (IsExclusivelyLocked(current) || !IsThereRoomForNewReaders(current)) {
        return false;
      }
      auto desired = AddReader(current);
      return lock_bit_.compare_exchange_weak(current, desired);
    } else {
      /** Acquire Writer (exclusive) Lock **/
      if (lock_bit_.load() != UnLocked) return false;

      auto unlocked = UnLocked;
      return lock_bit_.compare_exchange_weak(unlocked, ExclusivelyLocked);
    }
  }

  void UnLock() {
    auto current = lock_bit_.load();
    if (IsExclusivelyLocked(current)) {
      lock_bit_.store(UnLocked);
      return;
    }

    [[maybe_unused]] size_t sleep_ns = 100;
    for (;;) {
      current = lock_bit_.load();
      assert(IsThereAnyReader(current));
      auto desired = SubReader(current);
      if (lock_bit_.compare_exchange_weak(current, desired)) return;
      if constexpr (EnableBackoff) {
        std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_ns));
        sleep_ns *= 2;  // Exponential Backoff
      } else {
        std::this_thread::yield();
      }
    }
  }

  constexpr static bool IsStarvationFreeAlgorithm() { return false; }
  constexpr static bool IsReadersWritersLockingAlgorithm() { return true; }

 private:
  std::atomic<uint64_t> lock_bit_;
  [[maybe_unused]] char cacheline_padding_[63];
  static_assert(decltype(lock_bit_)::is_always_lock_free);

  constexpr static uint64_t ExclusivelyLocked = 1llu;
  constexpr static uint64_t UnLocked          = 0llu;
  constexpr static uint64_t Reader            = 1llu << 1;
  constexpr static uint64_t ReadersFull       = ~1llu;

  inline static bool IsExclusivelyLocked(const uint64_t n) {
    return n == ExclusivelyLocked;
  }
  inline static bool IsThereAnyReader(const uint64_t n) {
    return (Reader * 1) <= n;
  }
  inline static bool IsThereRoomForNewReaders(const uint64_t n) {
    return n < ReadersFull;
  }
  inline static uint64_t AddReader(const uint64_t n) { return n + Reader; }
  inline static uint64_t SubReader(const uint64_t n) { return n - Reader; }
};
using ReadersWritersLock     = ReadersWritersLockImpl<false, false>;
using ReadersWritersLockBO   = ReadersWritersLockImpl<true, false>;
using ReadersWritersLockCO   = ReadersWritersLockImpl<false, true>;
using ReadersWritersLockBOCO = ReadersWritersLockImpl<true, true>;
using ReadersWritersLockCOBO = ReadersWritersLockBOCO;

}  // namespace Lock
}  // namespace LineairDB

#endif /* LINEAIRDB_READERS_WRITERS_LOCK_HPP */
