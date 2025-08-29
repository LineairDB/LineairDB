/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
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

#ifndef LINEAIRDB_SNAPSHOT_HPP
#define LINEAIRDB_SNAPSHOT_HPP

#include <vector>

#include "data_item.hpp"
#include "definitions.h"

namespace LineairDB {

struct Snapshot {
  std::string key;
  DataItem data_item_copy;
  DataItem* index_cache;
  bool is_read_modify_write;
  std::string table_name;

  Snapshot(const std::string_view k, const std::byte v[], const size_t s,
           DataItem* const i, std::string_view tn, const TransactionId ver = 0)
      : key(k), index_cache(i), is_read_modify_write(false), table_name(tn) {
    if (v != nullptr) data_item_copy.Reset(v, s, ver);
  }
  Snapshot(const Snapshot&) = default;
  Snapshot& operator=(const Snapshot&) = default;

  static bool Compare(Snapshot& left, Snapshot& right) {
    return left.key < right.key;
  }
};

using ReadSetType = std::vector<Snapshot>;
using WriteSetType = std::vector<Snapshot>;

}  // namespace LineairDB

#endif /* LINEAIRDB_SNAPSHOT_HPP */
