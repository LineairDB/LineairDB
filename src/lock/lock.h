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

#ifndef LINEAIRDB_LOCK_H
#define LINEAIRDB_LOCK_H

namespace LineairDB {
namespace Lock {

template <class Derived>
class LockBase {
 public:
  enum class LockType { Exclusive, Shared };
  void Lock(LockType type = LockType::Exclusive) {
    if (IsReadersWritersLockingAlgorithm()) {
      return static_cast<Derived*>(this)->Lock(type);
    } else {
      return static_cast<Derived*>(this)->Lock();
    }
  }
  bool TryLock(LockType type = LockType::Exclusive) {
    if (IsReadersWritersLockingAlgorithm()) {
      return static_cast<Derived*>(this)->TryLock(type);
    } else {
      return static_cast<Derived*>(this)->Lock();
    }
  }
  void UnLock() { return static_cast<Derived*>(this)->UnLock(); }

  static bool IsStarvationFreeAlgorithm() {
    return Derived::IsStarvationFreeAlgorithm();
  }

  static bool IsReadersWritersLockingAlgorithm() {
    return Derived::IsReadersWritersLockingAlgorithm();
  }
};
}  // namespace Lock
}  // namespace LineairDB

#endif /* LINEAIRDB_LOCK_H */
