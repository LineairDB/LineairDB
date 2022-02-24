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

#include "precision_locking.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <mutex>
#include <string_view>
#include <vector>

#include "types/data_item.hpp"
#include "types/definitions.h"

namespace LineairDB {
namespace Index {

PrecisionLockingIndex::PrecisionLockingIndex(LineairDB::EpochFramework& e)
    : manager_stop_flag_(false),
      epoch_manager_ref_(e),
      manager_([&]() {
        while (manager_stop_flag_.load() != true) {
          epoch_manager_ref_.Sync();
          const auto global       = epoch_manager_ref_.GetGlobalEpoch();
          const auto stable_epoch = global - 2;

          {
            std::lock_guard<decltype(plock_)> p_guard(plock_);
            std::lock_guard<decltype(ulock_)> u_guard(ulock_);
            {
              // Clear predicate list
              auto it = predicate_list_.begin();
              if (it->first <= stable_epoch) {
                const auto beg = it;
                while (it != predicate_list_.end() &&
                       it->first <= stable_epoch) {
                  it++;
                }
                predicate_list_.erase(beg, it);
              }
            }
            {
              // Clear insert_or_delete_keys
              auto it = insert_or_delete_key_set_.begin();
              if (it->first <= stable_epoch) {
                const auto beg = it;
                while (it != insert_or_delete_key_set_.end() &&
                       it->first <= stable_epoch) {
                  it++;
                }
                const auto end = it;

                // Before deleting the set of insert_or_delete_keys, we update
                // the index container to apply such outdated (already
                // committed) insertions and deletions.
                for (it = beg; it != end; it++) {
                  for (const auto& event : it->second) {
                    container_[event.key].is_deleted = event.is_delete_event;
                  }
                }
                insert_or_delete_key_set_.erase(beg, end);
              }
            }
          }
        }
      }){};

PrecisionLockingIndex::~PrecisionLockingIndex() {
  manager_stop_flag_.store(true);
  manager_.join();
};

std::optional<size_t> PrecisionLockingIndex::Scan(
    const std::string_view b, const std::string_view e,
    std::function<bool(std::string_view)> operation) {
  size_t hit       = 0;
  const auto begin = std::string(b);
  const auto end   = std::string(e);
  if (end < begin) return std::nullopt;

  std::lock_guard<decltype(plock_)> p_guard(plock_);
  std::shared_lock<decltype(ulock_)> u_guard(ulock_);
  if (IsOverlapWithInsertOrDelete(b, e)) { return std::nullopt; }

  {
    auto it     = container_.lower_bound(begin);
    auto it_end = container_.upper_bound(end);
    for (; it != it_end; it++) {
      if (it->second.is_deleted) continue;
      hit++;
      auto cancel = operation(it->first);
      if (cancel) break;
    }
  }

  const auto epoch = epoch_manager_ref_.GetMyThreadLocalEpoch();

  predicate_list_[epoch].emplace_back(b, e);

  return hit;
};
bool PrecisionLockingIndex::Insert(const std::string_view key) {
  std::shared_lock<decltype(plock_)> p_guard(plock_);
  if (IsInPredicateSet(key)) { return false; }

  const auto epoch = epoch_manager_ref_.GetMyThreadLocalEpoch();
  std::lock_guard<decltype(ulock_)> u_guard(ulock_);
  insert_or_delete_key_set_[epoch].emplace_back(key, false);

  return true;
};

bool PrecisionLockingIndex::Delete(const std::string_view key) {
  std::shared_lock<decltype(plock_)> p_guard(plock_);
  if (IsInPredicateSet(key)) { return false; }
  const auto epoch = epoch_manager_ref_.GetMyThreadLocalEpoch();
  std::lock_guard<decltype(ulock_)> u_guard(ulock_);
  insert_or_delete_key_set_[epoch].emplace_back(key, true);

  return true;
};

bool PrecisionLockingIndex::IsInPredicateSet(const std::string_view key) {
  for (auto it = predicate_list_.begin(); it != predicate_list_.end(); it++) {
    for (const auto& predicate : it->second) {
      if (predicate.begin <= key && key <= predicate.end) return true;
    }
  }
  return false;
}

bool PrecisionLockingIndex::IsOverlapWithInsertOrDelete(
    const std::string_view begin, const std::string_view end) {
  for (auto it = insert_or_delete_key_set_.begin();
       it != insert_or_delete_key_set_.end(); it++) {
    for (const auto& event : it->second) {
      if (begin <= event.key && event.key <= end) return true;
    }
  }
  return false;
}

}  // namespace Index
}  // namespace LineairDB
