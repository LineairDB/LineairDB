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

#ifndef LINEAIRDB_TWO_PHASE_LOCKING_NWR_H
#define LINEAIRDB_TWO_PHASE_LOCKING_NWR_H

#include <lineairdb/tx_status.h>

#include <atomic>
#include <cstddef>
#include <cstring>
#include <set>
#include <vector>

#include "concurrency_control/concurrency_control_base.h"
#include "concurrency_control/pivot_object.hpp"
#include "index/concurrent_table.h"
#include "types.h"

namespace LineairDB {

namespace ConcurrencyControl {

enum class DeadLockAvoidanceType { NoWait, WaitDie, WoundWait };

template <DeadLockAvoidanceType deadlock_avoidance_type =
              DeadLockAvoidanceType::NoWait>
class TwoPhaseLockingImpl final : public ConcurrencyControlBase {
 public:
  TwoPhaseLockingImpl(TransactionReferences&& tx)
      : ConcurrencyControlBase(std::forward<TransactionReferences&&>(tx)),
        is_aborted_(false) {}

  ~TwoPhaseLockingImpl() final override{};

  const DataItem Read(const std::string_view,
                      DataItem* index_leaf) final override {
    assert(index_leaf != nullptr);
    if (is_aborted_)
      return {};  // NOTE: it means that 2PL does not ensure opacity.
    auto& rw_lock = index_leaf->GetRWLockRef();

    auto lock_acquired = rw_lock.TryLock(
        std::remove_reference<decltype(rw_lock)>::type::LockType::Shared);

    if (!lock_acquired) {
      if constexpr (deadlock_avoidance_type == DeadLockAvoidanceType::NoWait) {
        is_aborted_ = true;
        return {};
      } else {
        SPDLOG_ERROR(
            "Selected deadlock-avoidance algorithm is not implemented.");
        exit(EXIT_FAILURE);
      }
    }
    read_lock_set_.emplace(index_leaf);
    DataItem snapshot_item = *index_leaf;

    return snapshot_item;
  };
  void Write(const std::string_view key, const std::byte* const value,
             const size_t size, DataItem* index_leaf) final override {
    assert(index_leaf != nullptr);
    if (is_aborted_) return;

    auto& rw_lock             = index_leaf->GetRWLockRef();
    bool is_read_modify_write = false;
    for (auto& item : tx_ref_.read_set_ref_) {
      if (item.key == key) {
        is_read_modify_write = true;
        break;
      }
    }

    bool lock_acquired = false;
    if (is_read_modify_write) {
      // it has already been acquired shared lock. request upgrade.
      assert(read_lock_set_.find(index_leaf) != read_lock_set_.end());
      lock_acquired = rw_lock.TryLock(
          std::remove_reference<decltype(rw_lock)>::type::LockType::Upgrade);
      if (lock_acquired) read_lock_set_.erase(index_leaf);
    } else {
      lock_acquired = rw_lock.TryLock(
          std::remove_reference<decltype(rw_lock)>::type::LockType::Exclusive);
    }
    if (!lock_acquired) {
      if constexpr (deadlock_avoidance_type == DeadLockAvoidanceType::NoWait) {
        is_aborted_ = true;
        return;
      } else {
        SPDLOG_ERROR(
            "Selected deadlock-avoidance algorithm is not implemented.");
        exit(EXIT_FAILURE);
      }
    }

    auto copy_for_undo = *index_leaf;
    undo_set_.emplace_back(std::make_pair(index_leaf, copy_for_undo));

    index_leaf->Reset(value, size);
  };

  void Abort() final override {
    is_aborted_ = true;
    Undo();
  };
  bool Precommit() final override { return !is_aborted_; };

  void PostProcessing(TxStatus) final override { UnlockAll(); }

 private:
  void Undo() {
    for (auto& item : undo_set_) {
      item.first->Reset(item.second.value, item.second.size);
    }
  }
  void UnlockAll() {
    for (auto* item : read_lock_set_) { item->GetRWLockRef().UnLock(); }
    for (auto& item : undo_set_) { item.first->GetRWLockRef().UnLock(); }
  }

 private:
  std::vector<std::pair<DataItem*, DataItem>> undo_set_;
  std::set<DataItem*> read_lock_set_;
  bool is_aborted_;
};

using TwoPhaseLocking = TwoPhaseLockingImpl<DeadLockAvoidanceType::NoWait>;

}  // namespace ConcurrencyControl
}  // namespace LineairDB
#endif /* LINEAIRDB_TWO_PHASE_LOCKING_NWR_H */
