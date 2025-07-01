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

#include "lineairdb/database.h"

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/transaction.h"
#include "lineairdb/tx_status.h"
#include "test_helper.hpp"
class DatabaseTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread        = 4;
    config_.checkpoint_period = 1;
    config_.epoch_duration_ms = 100;
    db_                       = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(DatabaseTest, Instantiate) {}

TEST_F(DatabaseTest, InstantiateWithConfig) {
  db_.reset(nullptr);
  LineairDB::Config conf;
  conf.checkpoint_period = 1;
  ASSERT_NO_THROW(db_ = std::make_unique<LineairDB::Database>(conf));
}
TEST_F(DatabaseTest, ExecuteTransaction) {
  int value_of_alice = 1;
  TestHelper::DoTransactions(
      db_.get(),
      {[&](LineairDB::Transaction& tx) {
         tx.Write("alice", reinterpret_cast<std::byte*>(&value_of_alice),
                  sizeof(int));
       },
       [&](LineairDB::Transaction& tx) {
         auto alice = tx.Read("alice");
         ASSERT_NE(alice.first, nullptr);
         ASSERT_EQ(value_of_alice, *reinterpret_cast<const int*>(alice.first));
         ASSERT_EQ(0, tx.Read("bob").second);
       }});
}

TEST_F(DatabaseTest, ExecuteTransactionWithTemplates) {
  int value_of_alice = 1;
  TestHelper::DoTransactions(db_.get(),
                             {[&](LineairDB::Transaction& tx) {
                                tx.Write<int>("alice", value_of_alice);
                              },
                              [&](LineairDB::Transaction& tx) {
                                auto alice = tx.Read<int>("alice");
                                ASSERT_EQ(value_of_alice, alice.value());
                                ASSERT_FALSE(tx.Read<int>("bob").has_value());
                              }});
}

TEST_F(DatabaseTest, LargeSizeBuffer) {
  constexpr size_t Size = 2048;
  LineairDB::Config conf;
  conf.checkpoint_period    = 1;
  conf.max_thread           = 1;
  conf.enable_checkpointing = false;

  std::array<std::byte, Size> alice;

  ::testing::FLAGS_gtest_death_test_style = "threadsafe";

  TestHelper::DoTransactions(
      db_.get(),
      {[&](LineairDB::Transaction& tx) { tx.Write("alice", &alice[0], Size); },
       [&](LineairDB::Transaction& tx) {
         ASSERT_TRUE(tx.Read<decltype(alice)>("alice").has_value());
       }});
}

TEST_F(DatabaseTest, Scan) {
  int alice = 1;
  int bob   = 2;
  int carol = 3;
  TestHelper::RetryTransactionUntilCommit(db_.get(), [&](auto& tx) {
    tx.template Write<decltype(alice)>("alice", alice);
    tx.template Write<decltype(bob)>("bob", bob);
    tx.template Write<decltype(carol)>("carol", carol);
  });
  TestHelper::DoTransactions(
      db_.get(), {[&](LineairDB::Transaction& tx) {
                    // Scan
                    auto count = tx.Scan<decltype(alice)>(
                        "alice", "carol", [&](auto key, auto value) {
                          if (key == "alice") { EXPECT_EQ(alice, value); }
                          if (key == "bob") { EXPECT_EQ(bob, value); }
                          if (key == "carol") { EXPECT_EQ(carol, value); }
                          return false;
                        });
                    if (count.has_value()) { ASSERT_EQ(count.value(), 3); }
                  },
                  [&](LineairDB::Transaction& tx) {
                    // Cancel
                    auto count = tx.Scan<decltype(alice)>(
                        "alice", "carol", [&](auto key, auto value) {
                          if (key == "alice") { EXPECT_EQ(alice, value); }
                          return true;
                        });
                    if (count.has_value()) { ASSERT_EQ(count.value(), 1); };
                  }});
}

TEST_F(DatabaseTest, SaveAsString) {
  TestHelper::DoTransactions(db_.get(),
                             {[&](LineairDB::Transaction& tx) {
                                tx.Write<std::string_view>("alice", "value");
                              },
                              [&](LineairDB::Transaction& tx) {
                                auto alice = tx.Read<std::string_view>("alice");
                                ASSERT_EQ("value", alice.value());
                              }});
}

TEST_F(DatabaseTest, UserAbort) {
  TestHelper::DoTransactions(db_.get(),
                             {[&](LineairDB::Transaction& tx) {
                                int value_of_alice = 1;
                                tx.Write<int>("alice", value_of_alice);
                                tx.Abort();
                              },
                              [&](LineairDB::Transaction& tx) {
                                auto alice = tx.Read<int>("alice");
                                ASSERT_FALSE(alice.has_value());  // Opacity
                                tx.Abort();
                              }});
}

TEST_F(DatabaseTest, ReadYourOwnWrites) {
  int value_of_alice = 1;
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               tx.Write<int>("alice", value_of_alice);
                               auto alice = tx.Read<int>("alice");
                               ASSERT_EQ(value_of_alice, alice.value());
                             }});
}

TEST_F(DatabaseTest, ThreadSafetyInsertions) {
  TransactionProcedure insertTenTimes([](LineairDB::Transaction& tx) {
    int value = 0xBEEF;
    for (size_t idx = 0; idx <= 10; idx++) {
      tx.Write<int>("alice" + std::to_string(idx), value);
    }
  });

  ASSERT_NO_THROW({
    TestHelper::DoTransactionsOnMultiThreads(
        db_.get(),
        {insertTenTimes, insertTenTimes, insertTenTimes, insertTenTimes});
  });
  db_->Fence();

  TestHelper::DoTransactions(
      db_.get(), {[](LineairDB::Transaction& tx) {
        for (size_t idx = 0; idx <= 10; idx++) {
          auto alice = tx.Read<int>("alice" + std::to_string(idx));
          ASSERT_TRUE(alice.has_value());
          auto current_value = alice.value();
          ASSERT_EQ(0xBEEF, current_value);
        }
      }});
}

TEST_F(DatabaseTest, NoConfigTransaction) {
  // NOTE: this test will take default 5 seconds for checkpointing
  db_.reset(nullptr);
  db_                = std::make_unique<LineairDB::Database>();
  int value_of_alice = 1;
  TestHelper::DoTransactions(
      db_.get(),
      {[&](LineairDB::Transaction& tx) {
         tx.Write("alice", reinterpret_cast<std::byte*>(&value_of_alice),
                  sizeof(int));
       },
       [&](LineairDB::Transaction& tx) {
         auto alice = tx.Read("alice");
         ASSERT_NE(alice.first, nullptr);
         ASSERT_EQ(value_of_alice, *reinterpret_cast<const int*>(alice.first));
         ASSERT_EQ(0, tx.Read("bob").second);
       }});
}

// [Secondary Index TDD] ------------------------------------------------------
TEST_F(DatabaseTest, CreateTable) {
  db_ = std::make_unique<LineairDB::Database>();
  bool success = db_->CreateTable("users");
  ASSERT_TRUE(success);
  bool duplicated = db_->CreateTable("users");
  ASSERT_FALSE(duplicated);
}

//CreateSecondaryIndex("table_name", "index_name", "UNIQUE", "NOT_NULL")
TEST_F(DatabaseTest, CreateSecondaryIndex) {
  db_ = std::make_unique<LineairDB::Database>();
  bool success = db_->CreateTable("users");
  bool index_success = db_->CreateSecondaryIndex("users", "name", "UNIQUE", "NOT_NULL");
  ASSERT_TRUE(index_success);
}

//without constraint
TEST_F(DatabaseTest, CreateSecondaryIndexWithoutConstraints) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "nickname"));
}

// UNIQUE only
TEST_F(DatabaseTest, CreateSecondaryIndexUniqueOnly) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "email", "UNIQUE"));
  
}

// NOT_NULL only
TEST_F(DatabaseTest, CreateSecondaryIndexNotNullOnly) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "phone", "NOT_NULL"));
}

// constraint order
TEST_F(DatabaseTest, CreateSecondaryIndexConstraintOrderIndifference) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "address", "NOT_NULL", "UNIQUE"));
}

// invalid constraint keyword
TEST_F(DatabaseTest, CreateSecondaryIndexInvalidConstraintKeyword) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_FALSE(db_->CreateSecondaryIndex("users", "invalid", "FOO"));
}

TEST_F(DatabaseTest, InsertWithSecondaryIndex) {
  db_ = std::make_unique<LineairDB::Database>();
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "email");

  auto& tx = db_->BeginTransaction();
  int age = 42;
  // Primary key registration
  // Write(table, primary_key, value)
  tx.WritePK<int>("users", "user#1", age);

  // Secondary key registration
  // Write(table, index_name, secondary_key, primary_key)
  tx.WriteSK<std::string>("users", "email", "alice@example.com", "user#1");
  db_->EndTransaction(tx, [](auto s) {
    ASSERT_EQ(LineairDB::TxStatus::Committed, s);
  });

  auto& rtx = db_->BeginTransaction();
  // Secondary key search
  // Read(table, index_name, secondary_key)
  auto pk   = rtx.ReadSK<std::string>("users", "email", "alice@example.com");
  auto val  = rtx.ReadPK<int>("users", pk.value());
  ASSERT_EQ(age, val.value());
  db_->EndTransaction(rtx, [](auto){});
}



// [Secondary Index Constraint Enforcement Tests] ----------------------------
// UNIQUE constraint: insertion of a duplicate value should abort the txn
TEST_F(DatabaseTest, InsertDuplicateSecondaryKeyViolatesUnique) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "email", "UNIQUE"));

  // 1st row – this should commit.
  {
    auto& tx = db_->BeginTransaction();
    int age = 42;
    tx.WritePK<int>("users", "user#1", age);
    tx.WriteSK<std::string>("users", "email", "bob@example.com", "user#1");
    ASSERT_TRUE(db_->EndTransaction(tx, [](auto s){ASSERT_EQ(LineairDB::TxStatus::Committed, s);}));
  }

  // 2nd row with the same email – should abort.
  {
    auto& tx = db_->BeginTransaction();
    int age = 24;
    tx.WritePK<int>("users", "user#2", age);
    tx.WriteSK<std::string>("users", "email", "bob@example.com", "user#2"); // duplicate key
    ASSERT_FALSE(db_->EndTransaction(tx, [](auto s){ASSERT_EQ(LineairDB::TxStatus::Aborted, s);}));
  }
}

// NOT NULL constraint: inserting NULL value should abort the txn
TEST_F(DatabaseTest, InsertNullSecondaryKeyViolatesNotNull) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "email", "NOT_NULL"));

  auto& tx = db_->BeginTransaction();
  int age = 40;
  tx.WritePK<int>("users", "user#1", age);
  // Attempt to store a NULL email (empty string as placeholder)
  tx.WriteSK<std::string>("users", "email", "", "user#1");
  ASSERT_FALSE(db_->EndTransaction(tx, [](auto s){ASSERT_EQ(LineairDB::TxStatus::Aborted, s);}));
}

// Combined UNIQUE & NOT_NULL constraint
TEST_F(DatabaseTest, SecondaryIndexUniqueAndNotNullCombination) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "email", "UNIQUE", "NOT_NULL"));

  // Valid insert
  {
    auto& tx = db_->BeginTransaction();
    int age = 22;
    tx.WritePK<int>("users", "user#1", age);
    tx.WriteSK<std::string>("users", "email", "charlie@example.com", "user#1");
    ASSERT_TRUE(db_->EndTransaction(tx, [](auto s){ASSERT_EQ(LineairDB::TxStatus::Committed, s);}));
  }

  // Attempt duplicate -> Abort
  {
    auto& tx = db_->BeginTransaction();
    int age = 23;
    tx.WritePK<int>("users", "user#2", age);
    tx.WriteSK<std::string>("users", "email", "charlie@example.com", "user#2");
    ASSERT_FALSE(db_->EndTransaction(tx, [](auto s){ASSERT_EQ(LineairDB::TxStatus::Aborted, s);}));
  }
  // 
  // Attempt NULL -> Abort
  {
    auto& tx = db_->BeginTransaction();
    int age = 24;
    tx.WritePK<int>("users", "user#3", age);
    tx.WriteSK<std::string>("users", "email", "", "user#3");
    ASSERT_FALSE(db_->EndTransaction(tx, [](auto s){ASSERT_EQ(LineairDB::TxStatus::Aborted, s);}));
  }
}

// WriteSK to unregistered index should abort
TEST_F(DatabaseTest, WriteSKToUnregisteredIndexShouldAbort) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));

  auto& tx = db_->BeginTransaction();

  int age = 22;
  tx.WritePK<int>("users", "user#1", age);
  tx.WriteSK<std::string>("users", "email", "alice@example.com", "user#1");
  db_->EndTransaction(tx, [](auto s){ASSERT_EQ(LineairDB::TxStatus::Aborted, s);});
}

// ReadSK to unregistered index should abort
TEST_F(DatabaseTest, ReadSKToUnregisteredIndexShouldAbort) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));

  auto& tx = db_->BeginTransaction();
  tx.ReadSK<std::string>("users", "email", "alice@example.com");
  db_->EndTransaction(tx, [](auto s){ASSERT_EQ(LineairDB::TxStatus::Aborted, s);});
}

// WriteSK with non-existent primary key should abort
TEST_F(DatabaseTest, InsertSKWithNonExistentPKShouldAbort) {
  db_ = std::make_unique<LineairDB::Database>();
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "email", "UNIQUE"));

  auto& tx = db_->BeginTransaction();
  // PK "ghost#1" はまだ存在しない
  tx.WriteSK<std::string>("users", "email", "ghost@example.com", "ghost#1");

  // 参照整合性違反で Abort を期待
  ASSERT_FALSE(db_->EndTransaction(tx, [](auto s) {
    ASSERT_EQ(LineairDB::TxStatus::Aborted, s);
  }));
}

// memo:
// 1. type of key and value
//Primary store:
// - key: primary key
// - value: primary value
//
// Secondary store:
// - key: table, index_name, secondary_key
// - value: primary key
//
// 2. type of write interface
// a. Write(table, primary_key, value)
// b. Write(table, index_name, secondary_key, primary_key)
// c. Write(table, index_name, secondary_key, new_primary_key, value)
//
// maybe we will separate 2-c into 2-a and 2-b
// -----------------------------------------------------------------------------
