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

#ifndef LINEAIRDB_CONCURRENT_TABLE_H
#define LINEAIRDB_CONCURRENT_TABLE_H

#include <lineairdb/config.h>

#include <functional>
#include <string>
#include <string_view>

#include "index/precision_locking_index/index.hpp"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "types/snapshot.hpp"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

class ConcurrentTable {
 public:
  ConcurrentTable(EpochFramework& epoch_framework, Config config = Config(),
                  WriteSetType recovery_set = WriteSetType());

  DataItem* Get(const std::string_view key);
  DataItem* GetOrInsert(const std::string_view key);
  bool Put(const std::string_view key, DataItem&& value);
  void ForEach(std::function<bool(std::string_view, DataItem&)>);
  std::optional<size_t> Scan(const std::string_view begin,
                             const std::optional<std::string_view> end,
                             std::function<bool(std::string_view)> operation);
  std::optional<size_t> Scan(
      const std::string_view begin, const std::string_view end,
      std::function<bool(std::string_view, DataItem&)> operation);
  bool Insert(const std::string_view key);

  bool Delete(const std::string_view key);

  void WaitForIndexIsLinearizable();

 private:
  std::unique_ptr<HashTableWithPrecisionLockingIndex<DataItem>> index_;
  LineairDB::EpochFramework& epoch_manager_ref_;
};
}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_CONCURRENT_TABLE_H */
