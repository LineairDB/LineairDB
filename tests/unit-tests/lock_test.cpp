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

#include "lock/lock.h"

#include <chrono>
#include <future>
#include <thread>
#include <utility>

#include "gtest/gtest.h"
#include "lock/impl/readers_writers_lock.hpp"
#include "lock/impl/ttas_lock.hpp"
#include "util/logger.hpp"

template <typename T>
class LockTest : public ::testing::Test {};

using namespace LineairDB::Lock;
typedef ::testing::Types<TTASLock, TTASLockBO, TTASLockCO, TTASLockBOCO,
                         ReadersWritersLock, ReadersWritersLockBO,
                         ReadersWritersLockCO, ReadersWritersLockBOCO>
    LockTypes;
TYPED_TEST_SUITE(LockTest, LockTypes);

TYPED_TEST(LockTest, Lock) {
  TypeParam lock;
  lock.Lock();
  ASSERT_FALSE(lock.TryLock());
}

TYPED_TEST(LockTest, UnLock) {
  TypeParam lock;
  lock.Lock();
  lock.UnLock();
  ASSERT_TRUE(lock.TryLock());
}

TYPED_TEST(LockTest, MultiThreaded) {
  TypeParam lock;
  std::atomic<bool> barrier(true);

  lock.Lock();
  auto the_other_thread = std::async(std::launch::async, [&]() {
    ASSERT_FALSE(lock.TryLock());
    while (barrier.load()) std::this_thread::yield();
    ASSERT_TRUE(lock.TryLock());
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  lock.UnLock();
  barrier.store(false);
  the_other_thread.wait();
}

TYPED_TEST(LockTest, StarvationFreeProperty) {
  if (TypeParam::IsStarvationFreeAlgorithm() == false) return;
  // TODO impl this testcase.
}

TYPED_TEST(LockTest, IsReadersWritersLockingAlgorithm) {
  if constexpr (TypeParam::IsReadersWritersLockingAlgorithm()) {
    TypeParam lock;
    ASSERT_TRUE(lock.TryLock(TypeParam::LockType::Exclusive));
    ASSERT_FALSE(lock.TryLock(TypeParam::LockType::Exclusive));
    ASSERT_FALSE(lock.TryLock(TypeParam::LockType::Shared));
    lock.UnLock();

    // Shared Locking
    for (size_t i = 0; i < 10; i++) {
      ASSERT_TRUE(lock.TryLock(TypeParam::LockType::Shared));
    }
    ASSERT_FALSE(lock.TryLock(TypeParam::LockType::Exclusive));

    // Lock upgrade
    ASSERT_FALSE(lock.TryLock(TypeParam::LockType::Upgrade));
    for (size_t i = 0; i < 9; i++) { lock.UnLock(); }
    ASSERT_TRUE(lock.TryLock(TypeParam::LockType::Upgrade));
  }
}
