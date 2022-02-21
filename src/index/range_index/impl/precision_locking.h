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

#ifndef LINEAIRDB_INDEX_PRECISION_LOCKING_INDEX_H
#define LINEAIRDB_INDEX_PRECISION_LOCKING_INDEX_H

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <map>
#include <shared_mutex>
#include <string_view>

#include "index/range_index/range_index_base.h"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

/**
 * @brief
 * Range-index with phantom avoidance via precision locking [1].
 * We named it PrecisionLockingIndex.
 * It consists of a sorted index, a insert/delete key set (L_u), and a predicate
 * set (L_p).
 * @note
 * The special thread updates the index periodically (per epoch). It
 * means that other worker threads fetch the (maybe stale) index. To prevent
 * anomalies caused by the stale index (i.e., to prevent phantoms), we use L_u
 * and L_p. All update operations (such as Insert/Delete) and scan operations
 * are grouped as L_u and L_p for each epoch, respectively. Updates in L_u will
 * be applied as a batch by the special thread. If a transaction detects that
 * the addition of an element to L_u satisfies with some predicate in L_p, or
 * vice versa, we will fail the transaction because a phantom may exist.
 *
 * @ref [1] https://dl.acm.org/doi/pdf/10.1145/582318.582340
 *
 */
class PrecisionLockingIndex final : public RangeIndexBase {
 public:
  PrecisionLockingIndex(LineairDB::EpochFramework&);
  ~PrecisionLockingIndex() final override;
  std::optional<size_t> Scan(
      const std::string_view begin, const std::string_view end,
      std::function<bool(std::string_view)> operation) final override;
  bool Insert(const std::string_view key) final override;
  bool Delete(const std::string_view key) final override;

 private:
  bool IsInPredicateSet(const std::string_view);
  bool IsOverlapWithInsertOrDelete(const std::string_view,
                                   const std::string_view);

  struct Predicate {
    std::string begin;
    std::string end;
    Predicate(std::string_view b, std::string_view e): begin(b), end(e) {}
  };

  struct InsertOrDeleteEvent {
    std::string key;
    bool is_delete_event;
    InsertOrDeleteEvent(std::string_view k, bool i): key(k), is_delete_event(i){}
  };

  struct IndexItem {
    bool is_deleted;
  };

  using PredicateList = std::map<EpochNumber, std::vector<Predicate>>;
  using InsertOrDeleteKeySet =
      std::map<EpochNumber, std::vector<InsertOrDeleteEvent>>;
  using ROWEXRangeIndexContainer = std::map<std::string, IndexItem>;

  PredicateList predicate_list_;
  std::shared_mutex plock_;

  InsertOrDeleteKeySet insert_or_delete_key_set_;
  std::shared_mutex ulock_;

  ROWEXRangeIndexContainer container_;

  std::atomic<bool> manager_stop_flag_;
  std::thread manager_;
};
}  // namespace Index
}  // namespace LineairDB

#endif /*  LINEAIRDB_INDEX_PRECISION_LOCKING_INDEX_H*/
