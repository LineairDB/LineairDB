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

#include "mpmc_concurrent_set_impl.h"

#include <cassert>
#include <functional>
#include <mutex>
#include <string_view>

#include "types.h"

namespace LineairDB {
namespace Index {

MPMCConcurrentSetImpl::~MPMCConcurrentSetImpl() {
  Clear();
  delete table_.load();
}

// WANTFIX
// Replace linear-probing with hopscotch-hashing or cuckoo-hashing to reduce the
// computational costs of find operation.
DataItem* MPMCConcurrentSetImpl::Get(const std::string_view key) {
  epoch_framework_.MakeMeOnline();
  auto* table = table_.load(std::memory_order::memory_order_relaxed);
  __builtin_prefetch(table, 0, 3);
  size_t hash    = Hash(key, table);
  auto* bucket_p = (*table)[hash].load(std::memory_order::memory_order_relaxed);
  DataItem* return_value_p = nullptr;

  // lineair probing
  for (;;) {
    // redirected
    if (__builtin_expect(IsRedirectedPtr(bucket_p), false)) {
      table    = table_.load();
      hash     = Hash(key, table);
      bucket_p = (*table)[hash].load(std::memory_order::memory_order_relaxed);
      __builtin_prefetch(bucket_p, 0, 3);
      continue;
    }

    if (__builtin_expect(bucket_p == nullptr, false)) { break; }

    // Optimization: we assume that cmp of uint64_T is faster than strcmp.
    if (bucket_p->key_8b_prefix == string_to_uint64_t(key)) {
      if (bucket_p->key == key) {
        return_value_p = const_cast<DataItem*>(bucket_p->value);
        break;
      }
    }

    hash++;
    if (__builtin_expect(hash == table->size(), false)) { hash = 0; }
    bucket_p = (*table)[hash].load(std::memory_order::memory_order_relaxed);
  }

  epoch_framework_.MakeMeOffline();
  return return_value_p;
}

bool MPMCConcurrentSetImpl::Put(const std::string_view key,
                                const DataItem* const value_p) {
  epoch_framework_.MakeMeOnline();
  auto* table    = table_.load(std::memory_order::memory_order_seq_cst);
  size_t hash    = Hash(key, table);
  auto* new_node = new TableNode(key, value_p);

  // lineair probing
  for (;;) {
    auto& bucket_atm = (*table)[hash];
    auto* node       = bucket_atm.load(std::memory_order::memory_order_relaxed);

    // redirected
    if (__builtin_expect(IsRedirectedPtr(node), false)) {
      table = table_.load(std::memory_order::memory_order_seq_cst);
      hash  = Hash(key, table);
      node  = (*table)[hash].load(std::memory_order::memory_order_relaxed);
      continue;
    }

    // empty bucket has found. insert
    if (node == nullptr) {
      bool succ = bucket_atm.compare_exchange_weak(node, new_node);
      if (succ) {
        const size_t current_stored = populated_count_.fetch_add(1);
        const double current_fill_rate =
            (current_stored / static_cast<double>(table->size()));
        epoch_framework_.MakeMeOffline();
        if (RehashThreshold < current_fill_rate) { Rehash(); }
        return true;
      } else {
        continue;
      }
    }

    // Optimization: we assume that cmp of uint64_T is faster than strcmp.
    if (node->key_8b_prefix == string_to_uint64_t(key)) {
      if (node->key == key) {
        delete new_node;
        epoch_framework_.MakeMeOffline();
        return false;
      }
    }

    hash++;
    if (__builtin_expect(hash == table->size(), false)) { hash = 0; }
  }
}

// FYI: https://preshing.com/20160222/a-resizable-concurrent-map/
bool MPMCConcurrentSetImpl::Rehash() {
  std::lock_guard<std::mutex> lock(table_lock_);
  auto* table = table_.load(std::memory_order::memory_order_seq_cst);
  if ((populated_count_.load() / static_cast<double>(table->size())) <
      RehashThreshold) {
    // someone else has been rehashed the table.
    return false;
  }

  // NOTE changing the table size also changes the results of #Hash,
  // since it is used as the salt.
  TableType* new_table = new TableType(table->size() * 2);

  // copy and rehashing all nodes
  for (auto& bucket_atm : *table) {
    auto* node = bucket_atm.load(std::memory_order::memory_order_relaxed);

    if (node == nullptr) {
      if (bucket_atm.compare_exchange_strong(node, GetRedirectedPtr())) {
        continue;
      } else {
        node = bucket_atm.load(std::memory_order::memory_order_relaxed);
      }
    }

    size_t rehashed = Hash(node->key, new_table);

    // lineair probing
    for (;;) {
      auto& target_bucket = (*new_table)[rehashed];
      if (target_bucket.load(std::memory_order::memory_order_relaxed) ==
          nullptr) {
        target_bucket.store(node);
        break;
      }
      rehashed++;
      if (rehashed == new_table->size()) rehashed = 0;
    }

    [[maybe_unused]] bool exchanged =
        bucket_atm.compare_exchange_strong(node, GetRedirectedPtr());
    assert(exchanged);  // NOTE: This class provides concurrent `set` of
                        // `pointer`; we assume that pointer entries are never
                        // be deleted and updated.
  }

  [[maybe_unused]] auto table_exchanged =
      table_.compare_exchange_strong(table, new_table);
  assert(table_exchanged);

  // QSBR-based garbage collection
  epoch_framework_.Sync();
  delete table;
  return true;
}

void MPMCConcurrentSetImpl::ForAllWithExclusiveLock(
    std::function<void(const std::string_view, const DataItem*)> f) {
  std::lock_guard<std::mutex> lock(table_lock_);
  epoch_framework_.MakeMeOnline();
  for (auto& bucket_atm :
       *table_.load(std::memory_order::memory_order_relaxed)) {
    auto* node = bucket_atm.load();
    if (node == nullptr) continue;

    f(node->key, node->value);
  }
  epoch_framework_.MakeMeOffline();
}

inline size_t MPMCConcurrentSetImpl::Hash(std::string_view key,
                                          TableType* table) {
  auto capacity = table->size();
  auto hashed   = std::hash<std::string_view>()(key);
  hashed        = hashed ^ capacity;
  return hashed % capacity;
}

void MPMCConcurrentSetImpl::Clear() {
  std::lock_guard<std::mutex> lock(table_lock_);
  auto* table = table_.load(std::memory_order::memory_order_seq_cst);
  for (auto& bucket_atm : *table) {
    auto* node = bucket_atm.load(std::memory_order::memory_order_seq_cst);
    if (node == nullptr) continue;

    delete node;
  }
  table->clear();
}

}  // namespace Index
}  // namespace LineairDB
