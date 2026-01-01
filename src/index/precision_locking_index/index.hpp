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

#include <cassert>
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
  /**
   * @brief Entry state matrix:
   *
   * | State          | Range | Point | Insert | Delete | Read | Write | Scan  |
   * |----------------|-------|-------|--------|--------|------|-------|-------|
   * | EXISTS         | Yes   | Yes   | Fail   | OK     | OK   | OK    | Hit   |
   * | NOT_EXISTS     | No    | No    | OK     | Fail   | Fail | OK    | Miss  |
   * | DELETED        | No    | Yes   | OK     | Fail   | Fail | OK    | Miss  |
   * | INCONSISTENT   | Yes   | No    | N/A    | N/A    | N/A  | N/A   | N/A   |
   *
   * - EXISTS: Normal state where entry exists in both indexes
   * - NOT_EXISTS: Entry doesn't exist in either index
   * - DELETED: Logically deleted (removed from range but physical entry remains
   * in point)
   * - INCONSISTENT: Should never occur (assertion failure if detected)
   */
  enum class EntryState {
    EXISTS,       // Range: Yes, Point: Yes
    NOT_EXISTS,   // Range: No,  Point: No
    DELETED,      // Range: No,  Point: Yes (logically deleted)
    INCONSISTENT  // Range: Yes, Point: No
  };

  HashTableWithPrecisionLockingIndex(Config c, EpochFramework& e)
      : point_index_(c.rehash_threshold), range_index_(e) {}

  T* Get(const std::string_view key) { return point_index_.Get(key); }

  /**
   * @brief Get the current state of an entry
   * @param key The key to check
   * @return EntryState The current state of the entry
   */
  EntryState GetEntryState(const std::string_view key) {
    T* point_entry = point_index_.Get(key);
    bool in_point = (point_entry != nullptr);
    bool in_range = range_index_.Contains(key);
    bool deleted = (in_point && point_entry->IsInitialized() == false);

    if (in_range && in_point && !deleted) {
      return EntryState::EXISTS;
    } else if (!in_range && !in_point) {
      return EntryState::NOT_EXISTS;
    } else if (deleted) {
      return EntryState::DELETED;
    } else {
      return EntryState::INCONSISTENT;
    }
  }

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
   * @brief Insert a new entry
   * @param key The key to insert
   * @return true if insert succeeds, false otherwise
   *
   * Behavior based on entry state:
   * - EXISTS: Fails (entry already exists)
   * - NOT_EXISTS: Succeeds (inserts into both indexes)
   * - DELETED: Succeeds (only adds to range index, point entry already exists)
   * - INCONSISTENT: Should never occur (assertion failure)
   */
  bool Insert(const std::string_view key) {
    EntryState state = GetEntryState(key);

    switch (state) {
      case EntryState::EXISTS:
        // Entry already exists in both indexes - Insert fails
        return false;

      case EntryState::NOT_EXISTS: {
        // Put blank entry, then insert into range index
        auto* new_entry = new T();
        bool inserted_point = point_index_.Put(key, new_entry);
        if (!inserted_point) {
          delete new_entry;
          return false;
        }

        return range_index_.Insert(key);
      }

      case EntryState::DELETED:
        // Logically deleted - Only need to add back to range index
        return range_index_.Insert(key);

      case EntryState::INCONSISTENT:
        assert(false && "Inconsistent entry state detected.");
        break;
    }

    return false;  // Unreachable
  };

  /**
   * @brief Ensure the key is visible in range index for secondary writes.
   * @return true on success, false if phantom anomaly is detected.
   */
  bool EnsureVisibleForSecondaryWrite(const std::string_view key) {
    T* point_entry = point_index_.Get(key);
    const bool in_range = range_index_.Contains(key);

    if (point_entry == nullptr) {
      auto* new_entry = new T();
      bool inserted_point = point_index_.Put(key, new_entry);
      if (!inserted_point) {
        delete new_entry;
        point_entry = point_index_.Get(key);
        if (point_entry == nullptr) return false;
      }
      if (!in_range) return range_index_.Insert(key);
      return true;
    }

    if (!point_entry->IsInitialized()) return range_index_.Insert(key);
    if (!in_range) return range_index_.Insert(key);
    return true;
  };

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

  bool Delete(const std::string_view key) { return range_index_.Delete(key); };

  void WaitForIndexIsLinearizable() {
    range_index_.WaitForIndexIsLinearizable();
  }

 private:
  MPMCConcurrentSetImpl<T> point_index_;
  PrecisionLockingIndex range_index_;
};

}  // namespace Index

}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_PRECISION_LOCKING_INDEX_HPP */
