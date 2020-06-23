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

#ifndef LINEAIRDB_TTAS_LOCK_HPP
#define LINEAIRDB_TTAS_LOCK_HPP

#include <atomic>
#include <cassert>

#include "lock/lock.h"

namespace LineairDB {

namespace Lock {

template <bool EnableBackoff = false, bool EnableCohort = false>
class TTASLockImpl : LockBase<TTASLockImpl<EnableBackoff, EnableCohort>> {
 public:
  enum class LockType { Exclusive };
  TTASLockImpl() : lock_bit_(UnLocked) {}
  void Lock(LockType = LockType::Exclusive) {
    while (!TryLock()) {
      if constexpr (EnableBackoff) {
        size_t sleep_ns = 100;
        std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_ns));
        sleep_ns *= 2;  // Exponential Backoff
      } else {
        std::this_thread::yield();
      }
    }
  }
  bool TryLock(LockType = LockType::Exclusive) {
    if (lock_bit_.load() == Locked) return false;

    auto unlocked = UnLocked;
    return lock_bit_.compare_exchange_weak(unlocked, Locked);
  }

  void UnLock() {
    assert(lock_bit_.load() == Locked);
    lock_bit_.store(UnLocked);
  }

  constexpr static bool IsStarvationFreeAlgorithm() { return false; }
  constexpr static bool IsReadersWritersLockingAlgorithm() { return false; }

 private:
  std::atomic<uint64_t> lock_bit_;
  [[maybe_unused]] char cacheline_padding_[63];
  static_assert(decltype(lock_bit_)::is_always_lock_free);
  constexpr static uint64_t Locked   = 1llu;
  constexpr static uint64_t UnLocked = 0llu;
};
using TTASLock     = TTASLockImpl<false, false>;
using TTASLockBO   = TTASLockImpl<true, false>;
using TTASLockCO   = TTASLockImpl<false, true>;
using TTASLockBOCO = TTASLockImpl<true, true>;
using TTASLockCOBO = TTASLockBOCO;

}  // namespace Lock
}  // namespace LineairDB

#endif /* LINEAIRDB_TTAS_LOCK_HPP */
