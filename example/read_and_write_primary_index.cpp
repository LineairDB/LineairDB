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

#include <lineairdb/lineairdb.h>

#include <cassert>
#include <iostream>

int main() {
  {
    // Read and write primary index example
    LineairDB::Database db;
    LineairDB::TxStatus status;

    // Create table and primary index
    bool table_created = db.CreateTable("users");

    // Insert rows: primary index
    {
      auto& tx = db.BeginTransaction();
      tx.Write("users", "user#1", "Alice");
      tx.Write("users", "user#2", "Bob");
      db.EndTransaction(tx, [&](auto s) { status = s; });
      db.Fence();
      assert(status == LineairDB::TxStatus::Committed);
    }

    // Read primary index
    {
      auto& tx = db.BeginTransaction();
      auto v = tx.Read("users", "user#1");
      assert(v.has_value());
      assert(v.value() == std::string("Alice"));
      db.EndTransaction(tx, [&](auto) {});
      db.Fence();
    }

    // Scan primary index
    {
      auto& tx = db.BeginTransaction();
      auto count = tx.ScanPrimaryIndex<std::string_view>(
          "users", "user#1", std::string("user#9"), [&](auto key, auto value) {
            if (key == std::string("user#1")) {
              assert(value == std::string("Alice"));
            }
            if (key == std::string("user#2")) {
              assert(value == std::string("Bob"));
            }
            return false;  // continue scan
          });
      assert(count.has_value());
      db.EndTransaction(tx, [&](auto) {});
      db.Fence();
    }
  }
}
