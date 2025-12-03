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
    db.EndTransaction(tx, [&](auto s) { status = s; });
    // Fence: Block-wait until all running transactions are terminated
    db.Fence();
    assert(status == LineairDB::TxStatus::Committed);
  }

  {
    LineairDB::Database db;
    LineairDB::TxStatus status;

    db.ExecuteTransaction(
        [](LineairDB::Transaction& tx) {
          tx.Write<int>("carol", 10);
          tx.Delete("carol");
        },
        [&](LineairDB::TxStatus s) { status = s; });
    db.Fence();
    assert(status == LineairDB::TxStatus::Committed);

    db.ExecuteTransaction(
        [](LineairDB::Transaction& tx) {
          auto carol = tx.Read<int>("carol");
          assert(!carol.has_value());
        },
        [&](LineairDB::TxStatus s) { status = s; });
    db.Fence();
    assert(status == LineairDB::TxStatus::Committed);
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
}
