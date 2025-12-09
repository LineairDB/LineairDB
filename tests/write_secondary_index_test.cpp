#include <filesystem>
#include <memory>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/transaction.h"
#include "lineairdb/tx_status.h"

class WriteSecondaryIndexTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread = 4;
    config_.enable_recovery = false;
    config_.enable_logging = false;
    config_.enable_checkpointing = false;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(WriteSecondaryIndexTest, WriteSingleEntry) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  tx.Write("user1", "Alice");

  std::string primary_key = "user1";
  tx.WriteSecondaryIndex("age_index", "10",
                         reinterpret_cast<const std::byte*>(primary_key.data()),
                         primary_key.size());

  auto result = tx.ReadSecondaryIndex("age_index", "10");
  EXPECT_EQ(result.size(), 1);

  if (!result.empty()) {
    std::string pk_str(reinterpret_cast<const char*>(result[0].first),
                       result[0].second);
    EXPECT_EQ(pk_str, "user1");
  }

  db_->EndTransaction(tx, [](auto status) {
    EXPECT_EQ(status, LineairDB::TxStatus::Committed);
  });
}

TEST_F(WriteSecondaryIndexTest, WriteMultipleEntriesWithSameKey) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  tx.Write("user1", "Alice");
  tx.Write("user2", "Bob");
  tx.Write("user3", "Carol");

  std::string pk1 = "user1";
  tx.WriteSecondaryIndex("age_index", "25",
                         reinterpret_cast<const std::byte*>(pk1.data()),
                         pk1.size());
  std::string pk2 = "user2";
  tx.WriteSecondaryIndex("age_index", "25",
                         reinterpret_cast<const std::byte*>(pk2.data()),
                         pk2.size());
  std::string pk3 = "user3";
  tx.WriteSecondaryIndex("age_index", "30",
                         reinterpret_cast<const std::byte*>(pk3.data()),
                         pk3.size());

  // Same key "25" should have 2 primary keys
  auto result_25 = tx.ReadSecondaryIndex("age_index", "25");
  EXPECT_EQ(result_25.size(), 2);

  // Key "30" should have 1 primary key
  auto result_30 = tx.ReadSecondaryIndex("age_index", "30");
  EXPECT_EQ(result_30.size(), 1);

  db_->EndTransaction(tx, [](auto status) {
    EXPECT_EQ(status, LineairDB::TxStatus::Committed);
  });
}

TEST_F(WriteSecondaryIndexTest, WriteDuplicatePrimaryKeyIsIdempotent) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  tx.Write("user1", "Alice");

  std::string pk = "user1";
  // Write same primary key twice to same secondary key
  tx.WriteSecondaryIndex("age_index", "25",
                         reinterpret_cast<const std::byte*>(pk.data()),
                         pk.size());
  tx.WriteSecondaryIndex("age_index", "25",
                         reinterpret_cast<const std::byte*>(pk.data()),
                         pk.size());

  // Should only have 1 entry (idempotent)
  auto result = tx.ReadSecondaryIndex("age_index", "25");
  EXPECT_EQ(result.size(), 1);

  db_->EndTransaction(tx, [](auto status) {
    EXPECT_EQ(status, LineairDB::TxStatus::Committed);
  });
}
