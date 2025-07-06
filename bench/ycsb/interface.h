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

#ifndef LINEAIRDB_YCSB_INTERFACE_H
#define LINEAIRDB_YCSB_INTERFACE_H

#include <lineairdb/transaction.h>

#include <string_view>

namespace YCSB {
namespace Interface {

void Read(LineairDB::Transaction& tx, std::string_view key, std::string_view, void*, size_t) {
  tx.Read(key);
}

void Update(LineairDB::Transaction& tx, std::string_view key, std::string_view, void* payload,
            size_t size) {
  tx.Write(key, reinterpret_cast<std::byte*>(payload), size);
}

// FIXME discriminate update and insert
void Insert(LineairDB::Transaction& tx, std::string_view key, std::string_view, void* payload,
            size_t size) {
  Update(tx, key, "", payload, size);
}

void Scan(LineairDB::Transaction& tx, std::string_view begin, std::string_view end, void*, size_t) {
  // from original: max scan length = 100
  size_t hit = 0;
  tx.Scan(begin, end, [&](auto, auto) {
    hit++;
    if (100 <= hit)
      return true;
    return false;
  });
}
void ReadModifyWrite(LineairDB::Transaction& tx, std::string_view key, std::string_view,
                     void* payload, size_t size) {
  Read(tx, key, "", payload, size);
  Update(tx, key, "", payload, size);
}

} // namespace Interface
} // namespace YCSB

#endif /* LINEAIRDB_YCSB_INTERFACE_H */
