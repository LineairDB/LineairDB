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
#include "util/backoff.hpp"

namespace LineairDB {

namespace Lock {

template <bool EnableBackoff = false, bool EnableCohort = false>
class ReadersWritersLockImpl
    : LockBase<ReadersWritersLockImpl<EnableBackoff, EnableCohort>> {
 public:
  enum class LockType { Exclusive, Shared, Upgrade };
  ReadersWritersLockImpl() : lock_bit_(UnLocked) {}
  void Lock(LockType type = LockType::Exclusive) {
    if constexpr (EnableBackoff) {
      Util::RetryWithExponentialBackoff([&]() { return TryLock(type); });
    } else {
      for (;;) {
        if (TryLock(type)) break;
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
    } else if (type == LockType::Exclusive) {
      /** Acquire Writer (exclusive) Lock **/
      if (lock_bit_.load() != UnLocked) return false;

      auto unlocked = UnLocked;
      return lock_bit_.compare_exchange_weak(unlocked, ExclusivelyLocked);
    } else {
      assert(type == LockType::Upgrade);
      auto current = lock_bit_.load();
      assert(IsThereAnyReader(current));

      /** Upgrade to Writer (exclusive) Lock **/
      if (GetNumberOfReaders(current) == 1) {
        // it seems that I am the only reader
        return lock_bit_.compare_exchange_weak(current, ExclusivelyLocked);
      } else {
        return false;
      }
    }
  }

  void UnLock() {
    auto current = lock_bit_.load();
    assert(!IsUnlocked(current));
    if (IsExclusivelyLocked(current)) {
      lock_bit_.store(UnLocked);
      return;
    }

    if constexpr (EnableBackoff) {
      Util::RetryWithExponentialBackoff([&]() {
        assert(IsThereAnyReader(current));
        auto desired = SubReader(current);
        return lock_bit_.compare_exchange_weak(current, desired);
      });
    } else {
      for (;;) {
        current = lock_bit_.load();
        assert(IsThereAnyReader(current));
        auto desired = SubReader(current);
        if (lock_bit_.compare_exchange_weak(current, desired)) break;
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

  inline static bool IsUnlocked(const uint64_t n) { return n == UnLocked; }
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
  inline static uint64_t GetNumberOfReaders(const uint64_t n) {
    return n >> ExclusivelyLocked;
  }
};
using ReadersWritersLock     = ReadersWritersLockImpl<false, false>;
using ReadersWritersLockBO   = ReadersWritersLockImpl<true, false>;
using ReadersWritersLockCO   = ReadersWritersLockImpl<false, true>;
using ReadersWritersLockBOCO = ReadersWritersLockImpl<true, true>;
using ReadersWritersLockCOBO = ReadersWritersLockBOCO;

}  // namespace Lock
}  // namespace LineairDB

#endif /* LINEAIRDB_READERS_WRITERS_LOCK_HPP */
