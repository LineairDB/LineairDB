/*
 *   Copyright (c) 2022 Nippon Telegraph and Telephone Corporation
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

#ifndef LINEAIRDB_INDEX_PRECISION_LOCKING_INDEX_HPP
#define LINEAIRDB_INDEX_PRECISION_LOCKING_INDEX_HPP

#include <functional>
#include <optional>
#include <string_view>

#include "point_index/mpmc_concurrent_set_impl.hpp"
#include "range_index/precision_locking.h"

namespace LineairDB {

namespace Index {

template <typename T>
class HashTableWithPrecisionLockingIndex {
 public:
  HashTableWithPrecisionLockingIndex(Config c, EpochFramework& e)
      : point_index_(c.rehash_threshold), range_index_(e) {}

  T* Get(const std::string_view key) { return point_index_.Get(key); }

  /**
   * @note return false if a phantom anomaly has detected.
   */
  bool Put(const std::string_view key, T&& rhs) { return Put(key, rhs); }
  bool Put(const std::string_view key, const T& rhs) {
    bool r_success = range_index_.Insert(key);
    if (!r_success) return false;
    auto* value = new T(rhs);
    bool p_success = point_index_.Put(key, value);
    if (!p_success) delete value;
    return true;
  }

  void ForcePutBlankEntry(const std::string_view key) {
    auto* new_entry = new T();
    if (!point_index_.Put(key, new_entry))
      delete new_entry;  // already inserted
    range_index_.ForceInsert(key);
  }

  /**
   * @brief Scan with key and values
   *
   * @param begin Starting point of the range. Matching entry is included.
   * @param end Ending point of the range. Matching entry isn't included. If
   * this parameter is not given (std::nullopt_t is given), this function
   * continues scanning to the end of index.
   * @param operation This callback function will be invoked for every entry
   * matching the range, The key/value pair will be given as an argument.
   * @return std::optional<size_t> returns std::nullopt if a phantom anomaly has
   * detected.
   */
  std::optional<size_t> Scan(
      const std::string_view begin, const std::optional<std::string_view> end,
      std::function<bool(std::string_view, T&)> operation) {
    return Scan(begin, end, [&](std::string_view key) {
      auto* value = Get(key);
      return operation(key, *value);
    });
  };

  /**
   * @brief Scan without values; that is, an interface to collect only keys from
   * range index.
   */
  std::optional<size_t> Scan(const std::string_view begin,
                             const std::optional<std::string_view> end,
                             std::function<bool(std::string_view)> operation) {
    return range_index_.Scan(begin, end, operation);
  };

  void ForEach(std::function<bool(std::string_view, T&)> f) {
    point_index_.ForEach(f);
  };

 private:
  MPMCConcurrentSetImpl<T> point_index_;
  PrecisionLockingIndex range_index_;
};

}  // namespace Index

}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_PRECISION_LOCKING_INDEX_HPP */
