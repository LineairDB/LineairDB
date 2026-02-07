#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"

class DeleteSecondaryIndexTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    config_.enable_recovery = false;
    config_.enable_logging = false;
    config_.enable_checkpointing = false;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(DeleteSecondaryIndexTest, Delete) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "age_index", 0);
  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  tx.Write("user1", "Alice");
  std::string pk = "user1";
  tx.WriteSecondaryIndex("age_index", "25",
                         reinterpret_cast<const std::byte*>(pk.data()),
                         pk.size());
  auto result = tx.ReadSecondaryIndex("age_index", "25");
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(result[0].second, pk.size());
  std::string pk_from_index(reinterpret_cast<const char*>(result[0].first),
                            result[0].second);
  ASSERT_EQ(pk_from_index, pk);

  db_->EndTransaction(tx, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });

  db_->Fence();

  auto& tx2 = db_->BeginTransaction();
  tx2.SetTable("users");
  tx2.DeleteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk.data()),
                           pk.size());
  auto result2 = tx2.ReadSecondaryIndex("age_index", "25");
  ASSERT_EQ(result2.size(), 0);

  db_->EndTransaction(tx2, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });
}

TEST_F(DeleteSecondaryIndexTest, DeleteAndScan) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "alpha_index", 0);
  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  tx.Write("user1", "Alice");
  tx.Write("user2", "Bob");
  tx.Write("user3", "Carol");
  std::string pk1 = "user1";
  std::string pk2 = "user2";
  std::string pk3 = "user3";

  tx.WriteSecondaryIndex("alpha_index", "a",
                         reinterpret_cast<const std::byte*>(pk1.data()),
                         pk1.size());
  tx.WriteSecondaryIndex("alpha_index", "b",
                         reinterpret_cast<const std::byte*>(pk2.data()),
                         pk2.size());
  tx.WriteSecondaryIndex("alpha_index", "c",
                         reinterpret_cast<const std::byte*>(pk3.data()),
                         pk3.size());

  db_->EndTransaction(tx, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });

  db_->Fence();

  auto& tx2 = db_->BeginTransaction();
  tx2.SetTable("users");

  std::vector<std::pair<std::string, std::string>> scan_results;
  auto result = tx2.ScanSecondaryIndex(
      "alpha_index", "a", "c", [&](auto key, auto primary_keys) {
        for (const auto& primary_key : primary_keys) {
          std::string pk_from_index(
              reinterpret_cast<const char*>(primary_key.data()),
              primary_key.size());
          scan_results.emplace_back(std::string(key), pk_from_index);
        }
        return false;
      });
  ASSERT_EQ(result.value(), 3);
  ASSERT_EQ(scan_results.size(), 3);
  EXPECT_EQ(scan_results[0].first, "a");
  EXPECT_EQ(scan_results[0].second, pk1);
  EXPECT_EQ(scan_results[1].first, "b");
  EXPECT_EQ(scan_results[1].second, pk2);
  EXPECT_EQ(scan_results[2].first, "c");
  EXPECT_EQ(scan_results[2].second, pk3);

  db_->EndTransaction(tx2, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });

  db_->Fence();

  auto& tx3 = db_->BeginTransaction();
  tx3.SetTable("users");
  tx3.DeleteSecondaryIndex("alpha_index", "b",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());
  auto result2 = tx3.ReadSecondaryIndex("alpha_index", "b");
  ASSERT_EQ(result2.size(), 0);

  db_->EndTransaction(tx3, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });

  db_->Fence();

  auto& tx4 = db_->BeginTransaction();
  tx4.SetTable("users");

  scan_results.clear();
  auto result3 = tx4.ScanSecondaryIndex(
      "alpha_index", "a", "c", [&](auto key, auto primary_keys) {
        for (const auto& primary_key : primary_keys) {
          std::string pk_from_index(
              reinterpret_cast<const char*>(primary_key.data()),
              primary_key.size());
          scan_results.emplace_back(std::string(key), pk_from_index);
        }
        return false;
      });
  ASSERT_EQ(result3.value(), 2);
  ASSERT_EQ(scan_results.size(), 2);
  EXPECT_EQ(scan_results[0].first, "a");
  EXPECT_EQ(scan_results[0].second, pk1);
  EXPECT_EQ(scan_results[1].first, "c");
  EXPECT_EQ(scan_results[1].second, pk3);

  db_->EndTransaction(tx4, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });
}