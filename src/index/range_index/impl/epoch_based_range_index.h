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

#ifndef LINEAIRDB_INDEX_EPOCH_BASED_RANGE_INDEX_H
#define LINEAIRDB_INDEX_EPOCH_BASED_RANGE_INDEX_H

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <set>
#include <shared_mutex>
#include <string_view>
#include <vector>

#include "index/range_index/range_index_base.h"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

/**
 * @brief
 * Single-Producer Multiple-Consumer (SPMC) Read-Optimized Write-EXclusive
 * (ROWREX) Index.
 * There exists a special thread generates the index periodically (per epoch).
 * Other worker threads fetch the generated index and use it simultaneously
 * without locking. All update operations (such as Insert/Delete) are grouped
 * by each epoch and updated as a batch by the special thread.
 * We named it Epoch-based ROWEX.
 * This data structure has small overhead on read (optimized) and is be
 * exclusive on write by a special thread.
 * @note
 * To deal with the phantom anomaly, all the key sets of reads (scan) and
 * writes (insert/delete) that occurred in an epoch are recorded in the shared
 * data structure. If a transaction detects a conflict, it is immediately
 * aborted. Since neither update->scan nor scan->update can track by concurrency
 * control protocols in LineairDB, so we cannot deny the possibility that these
 * edges may become the `last path` of a dependency cycle and result in the
 * correctness failure.
 * @todo Introduce and implement some concurrent data structure. The current
 * mutex-guarded implementation is very conservative and primitive, and suffers
 * from performance.
 */
class EpochBasedRangeIndex final : public RangeIndexBase {
 public:
  EpochBasedRangeIndex(LineairDB::EpochFramework&);
  ~EpochBasedRangeIndex() final override;
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
    EpochNumber epoch;
  };

  struct InsertOrDeleteEvent {
    std::string key;
    bool is_delete_event;
    EpochNumber epoch;
  };

  struct IndexItem {
    bool is_deleted;
    EpochNumber updated_at;
  };

  using PredicateList            = std::vector<Predicate>;
  using InsertOrDeleteKeySet     = std::vector<InsertOrDeleteEvent>;
  using ROWEXRangeIndexContainer = std::map<std::string, IndexItem>;

  PredicateList predicate_list_;
  InsertOrDeleteKeySet insert_or_delete_key_set_;
  ROWEXRangeIndexContainer container_;

  // TODO WANTFIX for performance: use a concurrent data strucuture to
  // manipulate these sets efficiently
  std::recursive_mutex lock_;

  size_t indexed_epoch_;
  std::atomic<bool> manager_stop_flag_;
  std::thread manager_;
};
}  // namespace Index
}  // namespace LineairDB

#endif /*  LINEAIRDB_INDEX_EPOCH_BASED_RANGE_INDEX_H*/
