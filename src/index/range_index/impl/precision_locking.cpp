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
    : RangeIndexBase(e), manager_stop_flag_(false), manager_([&]() {
        while (manager_stop_flag_.load() != true) {
          epoch_manager_ref_.Sync();
          const auto global = epoch_manager_ref_.GetGlobalEpoch();

          {
            std::lock_guard<decltype(lock_)> guard(lock_);
            {
              // Clear predicate list
              auto it = remove_if(predicate_list_.begin(),
                                  predicate_list_.end(), [&](const auto& pred) {
                                    const auto del = 2 <= global - pred.epoch;
                                    return del;
                                  });
              predicate_list_.erase(it, predicate_list_.end());
            }
            {
              // Clear insert_or_delete_keys
              auto it = remove_if(insert_or_delete_key_set_.begin(),
                                  insert_or_delete_key_set_.end(),
                                  [&](const auto& pred) {
                                    const auto del = 2 <= global - pred.epoch;
                                    return del;
                                  });

              // Before deleting the set of insert_or_delete_keys, we update the
              // index container to contain such outdated (already committed)
              // insertions and deletions.
              auto outdated_start = it;

              for (; it != insert_or_delete_key_set_.end(); it++) {
                if (it->is_delete_event) {
                  assert(0 < container_.count(it->key));
                  container_.at(it->key).is_deleted = true;
                } else {
                  if (0 < container_.count(it->key)) {
                    auto& entry      = container_.at(it->key);
                    entry.is_deleted = false;
                  } else {
                    container_.emplace(it->key, IndexItem{false});
                  }
                }
              }

              insert_or_delete_key_set_.erase(outdated_start,
                                              insert_or_delete_key_set_.end());
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

  // TODO: we can optimize to avoid locking for read-only transactions.
  std::lock_guard<decltype(lock_)> guard(lock_);

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
  predicate_list_.emplace_back(Predicate{begin, end, epoch});

  return hit;
};
bool PrecisionLockingIndex::Insert(const std::string_view key) {
  std::lock_guard<decltype(lock_)> guard(lock_);
  if (IsInPredicateSet(key)) { return false; }

  const auto epoch = epoch_manager_ref_.GetMyThreadLocalEpoch();
  insert_or_delete_key_set_.push_back(
      InsertOrDeleteEvent{std::string(key), false, epoch});

  return true;
};

bool PrecisionLockingIndex::Delete(const std::string_view key) {
  std::lock_guard<decltype(lock_)> guard(lock_);
  if (IsInPredicateSet(key)) { return false; }
  const auto epoch = epoch_manager_ref_.GetMyThreadLocalEpoch();
  insert_or_delete_key_set_.push_back(
      InsertOrDeleteEvent{std::string(key), true, epoch});

  return true;
};

bool PrecisionLockingIndex::IsInPredicateSet(const std::string_view key) {
  for (auto it = predicate_list_.begin(); it != predicate_list_.end(); it++) {
    if (it->begin <= key && key <= it->end) return true;
  }
  return false;
}

bool PrecisionLockingIndex::IsOverlapWithInsertOrDelete(
    const std::string_view begin, const std::string_view end) {
  for (auto it = insert_or_delete_key_set_.begin();
       it != insert_or_delete_key_set_.end(); it++) {
    if (begin <= it->key && it->key <= end) return true;
  }
  return false;
}

}  // namespace Index
}  // namespace LineairDB
