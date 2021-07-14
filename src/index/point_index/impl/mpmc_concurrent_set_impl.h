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

#include "index/point_index/concurrent_point_index_base.h"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"

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
class MPMCConcurrentSetImpl final : public ConcurrentPointIndexBase {
  // TODO WANTFIX replace std::hardware_destructive_interference_size
  // TODO performance fix hashed prefix uint64_t key
  struct alignas(64) TableNode {
    std::string key;
    const DataItem* value;
    uint64_t key_8b_prefix;
    TableNode() : value(nullptr) { assert(key.empty()); };
    TableNode(std::string_view k, const DataItem* const v)
        : key(k), value(v), key_8b_prefix(string_to_uint64_t(k)) {}
  };
  // static_assert(sizeof(TableNode) ==
  //             std::hardware_destructive_interference_size);

  static constexpr size_t InitialTableSize = 1024;
  static constexpr double RehashThreshold  = 0.75;
  static constexpr uintptr_t RedirectedPtr = 0x4B1D;
  inline static bool IsRedirectedPtr(void* ptr) {
    return reinterpret_cast<uintptr_t>(ptr) == RedirectedPtr;
  }
  inline static TableNode* GetRedirectedPtr() {
    return reinterpret_cast<TableNode*>(RedirectedPtr);
  }
  inline static uint64_t string_to_uint64_t(std::string_view k) {
    uint64_t result        = 0;
    const size_t copy_size = std::min(k.size(), sizeof(uint64_t));
    std::memcpy(&result, k.data(), copy_size);
    return result;
  }

  using TableType = std::vector<std::atomic<TableNode*>>;

 public:
  MPMCConcurrentSetImpl()
      : table_(new TableType(InitialTableSize)), populated_count_(0) {
    epoch_framework_.Start();
  }
  ~MPMCConcurrentSetImpl() final override;
  DataItem* Get(const std::string_view) final override;
  bool Put(const std::string_view, const DataItem* const) final override;
  void ForAllWithExclusiveLock(
      std::function<void(const std::string_view, const DataItem*)>)
      final override;
  void Clear() final override;  // thread-unsafe
  void ForEach(std::function<bool(std::string_view, DataItem&)>) final override;

 private:
  inline size_t Hash(std::string_view, TableType*);
  bool Rehash();

 private:
  std::atomic<TableType*> table_;
  std::atomic<size_t> populated_count_;
  std::mutex table_lock_;
  EpochFramework epoch_framework_;
};
}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_MPMC_CONCURRENT_SET_IMPL_H */
