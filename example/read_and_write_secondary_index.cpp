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
    // Write and read secondary index example
    LineairDB::Database db;
    LineairDB::TxStatus status;

    // Create simple secondary index
    bool table_created = db.CreateTable("users");
    bool secondary_index_created = db.CreateSecondaryIndex<std::string>(
        "users", "email", LineairDB::SecondaryIndexOption::Constraint::NONE);

    // Insert rows: primary and simple secondary index
    {
      auto& tx = db.BeginTransaction();
      tx.WritePrimaryIndex<std::string_view>("users", "user#1", "Alice");
      tx.WriteSecondaryIndex<std::string_view>(
          "users", "email", std::string("alice@example.com"), "user#1");
      db.EndTransaction(tx, [&](auto s) { status = s; });
      db.Fence();
      assert(status == LineairDB::TxStatus::Committed);
    }

    // Read secondary index (email -> PK list)
    {
      auto& tx = db.BeginTransaction();
      auto pks = tx.ReadSecondaryIndex<std::string_view>(
          "users", "email", std::string("alice@example.com"));
      assert(!pks.empty());
      assert(pks[0] == std::string("user#1"));
      db.EndTransaction(tx, [&](auto) {});
      db.Fence();
    }

    // Scan secondary index
    {
      auto& tx = db.BeginTransaction();
      auto count = tx.ScanSecondaryIndex<std::string_view>(
          "users", "email", std::string("a"), std::string("z"),
          [&](std::string_view /*sk*/, const std::vector<std::string_view>&) {
            return false;  // do nothing and continue scanning
          });
      if (count.has_value()) {
        assert(count.value() >= 1);
      }
      db.EndTransaction(tx, [&](auto) {});
      db.Fence();
    }
    // Update secondary index (move SK from old to new)
    {
      auto& tx = db.BeginTransaction();
      tx.UpdateSecondaryIndex<std::string_view>(
          "users", "email", std::string("alice@example.com"),
          std::string("alice@new.com"), "user#1");
      db.EndTransaction(tx, [&](auto s) { status = s; });
      db.Fence();
      assert(status == LineairDB::TxStatus::Committed);
    }
  }

  {
    // Create secondary index with UNIQUE constraint example
    LineairDB::Database db;
    LineairDB::TxStatus status;
    bool table_created = db.CreateTable("users_unique");
    bool secondary_index_created = db.CreateSecondaryIndex<std::string>(
        "users_unique", "email",
        LineairDB::SecondaryIndexOption::Constraint::UNIQUE);

    // First insert should commit
    {
      auto& tx = db.BeginTransaction();
      tx.WritePrimaryIndex<std::string_view>("users_unique", "user#1", "Bob");
      tx.WriteSecondaryIndex<std::string_view>(
          "users_unique", "email", std::string("bob@example.com"), "user#1");
      db.EndTransaction(tx, [&](auto s) { status = s; });
      db.Fence();
      assert(status == LineairDB::TxStatus::Committed);
    }

    // Second insert with duplicate email should abort
    {
      auto& tx = db.BeginTransaction();
      tx.WritePrimaryIndex<std::string_view>("users_unique", "user#2", "Bobby");
      tx.WriteSecondaryIndex<std::string_view>(
          "users_unique", "email", std::string("bob@example.com"), "user#2");
      db.EndTransaction(tx, [&](auto s) { status = s; });
      db.Fence();
      assert(status == LineairDB::TxStatus::Aborted);
    }
  }
}
