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

#ifndef LINEAIRDB_RANGE_INDEX_BASE_H
#define LINEAIRDB_RANGE_INDEX_BASE_H

#include <functional>
#include <string_view>

#include "types/data_item.hpp"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

/**
 * @brief
 * Base class of Range Index.
 *  @note
 * The job of the range index is not only 1) sorting inserted keys and 2)
 * returning the set of keys that match the predicate. Range index has a
 * responsibility of 3) preventing/avoiding the Phantom Anomaly. Phantom is an
 * anomaly that occurs when the result set of #Scan by a thread is stale and
 * there exists another transaction updating the index. In this case,
 * concurrency control protocol cannot detect the dependency edge from the
 * updating transaction to the scanning transaction since this edge is phantom
 * (i.e, index hides the existence of the edge). To cope with this anomaly,
 * #Scan needs to return the `correct` set of keys: there must be no cycle in
 * the dependency graph between transactions that execute the #Insert or #Delete
 * and transactions that execute the #Scan.
 *
 */
class RangeIndexBase {
 public:
  RangeIndexBase(LineairDB::EpochFramework& e) : epoch_manager_ref_(e){};
  virtual ~RangeIndexBase(){};
  virtual std::optional<size_t> Scan(
      const std::string_view begin, const std::string_view end,
      std::function<bool(std::string_view)> operation) = 0;
  virtual bool Insert(const std::string_view key)      = 0;
  virtual bool Delete(const std::string_view key)      = 0;

  LineairDB::EpochFramework& epoch_manager_ref_;
};
}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_RANGE_INDEX_BASE_H */
