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
    LineairDB::Database db;
    LineairDB::TxStatus status;

    // Execute: enqueue a transaction with an expected callback
    db.ExecuteTransaction(
        [](LineairDB::Transaction& tx) {
          auto alice = tx.Read<int>("alice");
          if (alice.has_value()) {
            std::cout << "alice is recovered: " << alice.value() << std::endl;
          }
          tx.Write<int>("alice", 1);
        },
        [&](LineairDB::TxStatus s) { status = s; });

    // Fence: Block-wait until all running transactions are terminated
    db.Fence();
    assert(status == LineairDB::TxStatus::Committed);
  }

  {
    LineairDB::Database db;
    LineairDB::TxStatus status;

    // Handler interface: execute a transaction on this thread
    auto& tx = db.BeginTransaction();
    tx.Read<int>("alice");
    tx.Write<int>("alice", 1);
    bool committed = db.EndTransaction(tx, [&](auto s) { status = s; });
    // Fence: Block-wait until all running transactions are terminated
    db.Fence();
    assert(committed);
  }

  {
    LineairDB::Database db;
    // Example of failures: database instance is not copy-constructable.
    //    NG: auto db2 = db;
    // Example of failures: we cannot allocate two Database instance at the same
    // time.
    //    NG: LineairDB::Database db2;
    //    NG: auto* db2 = new LineairDB::Database;
  }

  {
    // Here we have instantiated and destructed LineariDB twice, and
    // we can recover stored data from durable logs.
    LineairDB::Database db;
    db.ExecuteTransaction(
        [](LineairDB::Transaction& tx) {
          auto alice = tx.Read<int>("alice");
          assert(alice.has_value());
          assert(alice.value() == 1);
        },
        [&](LineairDB::TxStatus s) {});
  }

  {
    // Instantiate with customized configuration.
    LineairDB::Config config;
    config.concurrency_control_protocol =
        LineairDB::Config::ConcurrencyControl::Silo;
    config.enable_logging = false;
    config.enable_recovery = false;
    config.max_thread = 1;

    LineairDB::Database db(config);
    // Example of failures: we passed `config` as rvalue and it is nop to modify
    // this object after instantiation of LineairDB.
    //    NG: config.max_thread = 10;

    db.ExecuteTransaction(
        [](LineairDB::Transaction& tx) {
          auto alice = tx.Read<int>("alice");
          // Any data item is not recovered
          assert(!alice.has_value());
        },
        [&](LineairDB::TxStatus s) {});
  }

  {
    // Table and Secondary Index usage example
    LineairDB::Database db;

    // Create table and secondary index
    bool ok = db.CreateTable("users");
    assert(ok);
    ok = db.CreateSecondaryIndex<std::string>("users", "email");
    assert(ok);

    // Insert rows: primary and secondary
    {
      auto& tx = db.BeginTransaction();
      tx.WritePrimaryIndex<std::string_view>("users", "user#1", "Alice");
      tx.WriteSecondaryIndex<std::string_view>(
          "users", "email", std::string("alice@example.com"), "user#1");

      tx.WritePrimaryIndex<std::string_view>("users", "user#2", "Bob");
      tx.WriteSecondaryIndex<std::string_view>(
          "users", "email", std::string("bob@example.com"), "user#2");

      bool committed = db.EndTransaction(tx, [&](auto s) {
        (void)s; /* callback not relied upon here */
      });
      db.Fence();
      assert(committed);
    }

    // Read primary index
    {
      auto& tx = db.BeginTransaction();
      auto v = tx.ReadPrimaryIndex<std::string_view>("users", "user#1");
      assert(v.has_value());
      assert(v.value() == std::string("Alice"));
      bool committed = db.EndTransaction(tx, [&](auto s) {
        (void)s; /* ignore */
      });
      db.Fence();
      assert(committed);
    }

    // Read secondary index (email -> PK list)
    {
      auto& tx = db.BeginTransaction();
      auto pks = tx.ReadSecondaryIndex<std::string_view>(
          "users", "email", std::string("alice@example.com"));
      assert(!pks.empty());
      assert(pks[0] == std::string("user#1"));
      bool committed = db.EndTransaction(tx, [&](auto s) {
        (void)s; /* ignore */
      });
      db.Fence();
      assert(committed);
    }

    // Scan secondary index (lex order over SK)
    {
      bool committed = false;
      for (int attempt = 0; attempt < 8 && !committed; ++attempt) {
        auto& tx = db.BeginTransaction();
        auto count = tx.ScanSecondaryIndex<std::string_view>(
            "users", "email", std::string("a"), std::string("z"),
            [&](std::string_view /*sk*/,
                const std::vector<std::string_view>& pks) {
              // Stop early if we found at least one
              return !pks.empty();
            });
        // If scan aborted due to overlap, the transaction will abort below
        if (count.has_value()) {
          assert(count.value() >= 1);
        }
        committed = db.EndTransaction(tx, [&](auto s) { (void)s; });
        db.Fence();
      }
      assert(committed);
    }

    // Scan primary index (range over PK in a table)
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
      bool committed = db.EndTransaction(tx, [&](auto s) {
        (void)s; /* ignore */
      });
      db.Fence();
      assert(committed);
    }

    // Update secondary index (move SK from old to new)
    {
      auto& tx = db.BeginTransaction();
      tx.UpdateSecondaryIndex<std::string_view>(
          "users", "email", std::string("alice@example.com"),
          std::string("alice@new.com"), "user#1");
      bool committed = db.EndTransaction(tx, [&](auto s) { (void)s; });
      db.Fence();
      assert(committed);
    }

    // Verify update result on secondary index
    {
      auto& tx = db.BeginTransaction();
      auto new_pks = tx.ReadSecondaryIndex<std::string_view>(
          "users", "email", std::string("alice@new.com"));
      auto old_pks = tx.ReadSecondaryIndex<std::string_view>(
          "users", "email", std::string("alice@example.com"));
      assert(!new_pks.empty());
      assert(new_pks[0] == std::string("user#1"));
      assert(old_pks.empty());
      bool committed = db.EndTransaction(tx, [&](auto s) { (void)s; });
      db.Fence();
      assert(committed);
    }
  }
}
