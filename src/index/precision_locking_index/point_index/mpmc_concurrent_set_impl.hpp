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

#ifndef LINEAIRDB_MPMC_CONCURRENT_SET_IMPL_H
#define LINEAIRDB_MPMC_CONCURRENT_SET_IMPL_H

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <mutex>
#include <new>
#include <string_view>
#include <vector>

#include "types/data_item.hpp"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"

#ifndef PREFETCH_LOCALITY
#define PREFETCH_LOCALITY 3
#endif

namespace LineairDB {
namespace Index {

/**
 * @brief
 * Multi-Producer Multi-Consumer (MPMC) hash-table,
 * based on the open addressing & linear-probing strategy.
 * @note We focus on the performance for reads (gets), not for writes (puts).
 * In other words, we provide lock-free #Get and (maybe locking) #Put.
 * This is because LineairDB requires that point-indexes have to
 * hold only indirection pointer to each data item; once an indirection is
 * created and stored into the index, it will not be changed by #puts.
 */

template <typename T>
class MPMCConcurrentSetImpl {
  // TODO WANTFIX replace std::hardware_destructive_interference_size
  // TODO performance fix hashed prefix uint64_t key
  struct alignas(64) TableNode {
    std::string key;
    const T* value;
    uint64_t key_8b_prefix;
    TableNode() : value(nullptr) { assert(key.empty()); };
    TableNode(std::string_view k, const T* const v)
        : key(k), value(v), key_8b_prefix(string_to_uint64_t(k)) {}
  };
  // static_assert(sizeof(TableNode) ==
  //             std::hardware_destructive_interference_size);

  static constexpr size_t InitialTableSize = 4096;
  static constexpr uintptr_t RedirectedPtr = 0x4B1D;
  inline static bool IsRedirectedPtr(void* ptr) {
    return reinterpret_cast<uintptr_t>(ptr) == RedirectedPtr;
  }
  inline static TableNode* GetRedirectedPtr() {
    return reinterpret_cast<TableNode*>(RedirectedPtr);
  }
  inline static uint64_t string_to_uint64_t(std::string_view k) {
    uint64_t result = 0;
    const size_t copy_size = std::min(k.size(), sizeof(uint64_t));
    std::memcpy(&result, k.data(), copy_size);
    return result;
  }

  using TableType = std::vector<std::atomic<TableNode*>>;

 public:
  explicit MPMCConcurrentSetImpl(double r = 0.75)
      : rehash_threshold_(r),
        table_(new TableType(InitialTableSize)),
        populated_count_(0),
        rehash_thread_([&]() {
          while (!stop_flag_.load()) {
            std::unique_lock<std::mutex> lock(rehash_flag_);
            rehash_cv_.wait(lock, [&]() {
              const double current_fill_rate =
                  (populated_count_.load() /
                   static_cast<double>(table_.load()->size()));
              return (rehash_threshold_ <= current_fill_rate) ||
                     force_rehash_flag_.load() || stop_flag_.load();
            });

            if (stop_flag_.load()) return;
            const double current_fill_rate =
                (populated_count_.load() /
                 static_cast<double>(table_.load()->size()));
            if ((rehash_threshold_ <= current_fill_rate) ||
                force_rehash_flag_.load()) {
              // double checking for sprious wakeup
              force_rehash_flag_.store(false);
              Rehash();
            }
          }
        }) {
    epoch_framework_.Start();
  }
  ~MPMCConcurrentSetImpl() {
    stop_flag_.store(true);
    rehash_cv_.notify_all();
    rehash_thread_.join();
    Clear();
    delete table_.load();
  };
  T* Get(const std::string_view);
  bool Put(const std::string_view, const T* const);
  void Clear();  // thread-unsafe
  void ForEach(std::function<bool(std::string_view, T&)>);

 private:
  inline size_t Hash(std::string_view, TableType*);
  bool Rehash();

 private:
  const double rehash_threshold_;
  std::atomic<TableType*> table_;
  std::atomic<size_t> populated_count_;

  std::mutex table_lock_;
  std::atomic<bool> stop_flag_{false};

  std::mutex rehash_flag_;
  std::condition_variable rehash_cv_;
  std::thread rehash_thread_;
  std::atomic<bool> force_rehash_flag_{false};

  EpochFramework epoch_framework_;
};

/** the followings are implementation **/
template <typename T>
T* MPMCConcurrentSetImpl<T>::Get(const std::string_view key) {
get_start:
  epoch_framework_.MakeMeOnline();
  auto* table = table_.load(std::memory_order::memory_order_relaxed);
  __builtin_prefetch(table, 0, PREFETCH_LOCALITY);
  size_t hash = Hash(key, table);
  auto* bucket_p = (*table)[hash].load(std::memory_order::memory_order_relaxed);
  T* return_value_p = nullptr;

  size_t count = 0;

  // lineair probing
  for (;;) {
    // redirected
    if (__builtin_expect(IsRedirectedPtr(bucket_p), false)) {
      table = table_.load();
      hash = Hash(key, table);
      bucket_p = (*table)[hash].load(std::memory_order::memory_order_relaxed);
      __builtin_prefetch(bucket_p, 0, PREFETCH_LOCALITY);
      count = 0;
      continue;
    }

    if (__builtin_expect(bucket_p == nullptr, false)) {
      break;
    }

    // Optimization: we assume that cmp of uint64_T is faster than strcmp.
    if (bucket_p->key_8b_prefix == string_to_uint64_t(key)) {
      if (bucket_p->key == key) {
        return_value_p = const_cast<T*>(bucket_p->value);
        break;
      }
    }

    hash++;
    count++;
    if (__builtin_expect(hash == table->size(), false)) {
      hash = 0;
    }
    bucket_p = (*table)[hash].load(std::memory_order::memory_order_relaxed);
    if (count > 100) {
      epoch_framework_.MakeMeOffline();
      force_rehash_flag_.store(true);
      rehash_cv_.notify_all();  // rehash the table to reduce the probing length
      epoch_framework_.Sync();
      count = 0;
      goto get_start;
    }
  }

  epoch_framework_.MakeMeOffline();
  return return_value_p;
}

template <typename T>
bool MPMCConcurrentSetImpl<T>::Put(const std::string_view key,
                                   const T* const value_p) {
put_start:
  epoch_framework_.MakeMeOnline();
  auto* table = table_.load(std::memory_order::memory_order_seq_cst);
  size_t hash = Hash(key, table);
  auto* new_node = new TableNode(key, value_p);
  size_t count = 0;

  // TODO: WANTFIX
  // Replace linear-probing with hopscotch-hashing or cuckoo-hashing to reduce
  // the computational costs of find operation.
  for (;;) {
    auto& bucket_atm = (*table)[hash];
    auto* node = bucket_atm.load(std::memory_order::memory_order_relaxed);

    // redirected
    if (__builtin_expect(IsRedirectedPtr(node), false)) {
      table = table_.load(std::memory_order::memory_order_seq_cst);
      hash = Hash(key, table);
      node = (*table)[hash].load(std::memory_order::memory_order_relaxed);
      count = 0;
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
        if (rehash_threshold_ < current_fill_rate) {
          rehash_cv_.notify_one();
        }
        return true;
      } else {
        continue;
      }
    }

    // Optimization: we assume that cmp of uint64_t is faster than strcmp.
    if (node->key_8b_prefix == string_to_uint64_t(key)) {
      if (node->key == key) {
        delete new_node;
        epoch_framework_.MakeMeOffline();
        return false;
      }
    }

    hash++;
    count++;
    if (__builtin_expect(hash == table->size(), false)) {
      hash = 0;
    }

    if (count > 100) {
      epoch_framework_.MakeMeOffline();
      force_rehash_flag_.store(true);
      rehash_cv_.notify_all();  // rehash the table to reduce the probing length
      epoch_framework_.Sync();
      delete new_node;
      goto put_start;
    }
  }
}

// FYI: https://preshing.com/20160222/a-resizable-concurrent-map/
template <typename T>
bool MPMCConcurrentSetImpl<T>::Rehash() {
  std::lock_guard<std::mutex> lock(table_lock_);
  auto* table = table_.load(std::memory_order::memory_order_seq_cst);

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

template <typename T>
inline size_t MPMCConcurrentSetImpl<T>::Hash(std::string_view key,
                                             TableType* table) {
  auto capacity = table->size();
  auto hashed = std::hash<std::string_view>()(key);
  hashed = hashed ^ capacity;
  return hashed % capacity;
}

template <typename T>
void MPMCConcurrentSetImpl<T>::Clear() {
  std::lock_guard<std::mutex> lock(table_lock_);
  auto* table = table_.load(std::memory_order::memory_order_seq_cst);
  for (auto& bucket_atm : *table) {
    auto* node = bucket_atm.load(std::memory_order::memory_order_seq_cst);
    if (node == nullptr) continue;
    delete node->value;

    delete node;
  }
  table->clear();
}

template <typename T>
void MPMCConcurrentSetImpl<T>::ForEach(
    std::function<bool(std::string_view, T&)> f) {
  std::lock_guard<std::mutex> lock(table_lock_);
  epoch_framework_.MakeMeOnline();
  auto* table = table_.load(std::memory_order::memory_order_seq_cst);
  for (auto& bucket_atm : *table) {
    auto* node = bucket_atm.load(std::memory_order::memory_order_seq_cst);
    if (node == nullptr) continue;
    assert(!IsRedirectedPtr(node));
    auto is_success = f(node->key, *const_cast<T*>(node->value));
    if (!is_success) break;
  }
  epoch_framework_.MakeMeOffline();
}

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_MPMC_CONCURRENT_SET_IMPL_H */
