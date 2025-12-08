#include <filesystem>
#include <memory>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/transaction.h"
#include "lineairdb/tx_status.h"

class ReadSecondaryIndexTest : public ::testing::Test {
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

TEST_F(ReadSecondaryIndexTest, ReadNonExistentKeyReturnsEmpty) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  auto result = tx.ReadSecondaryIndex("age_index", "nonexistent");
  EXPECT_TRUE(result.empty());

  db_->EndTransaction(tx, [](auto status) {
    EXPECT_EQ(status, LineairDB::TxStatus::Committed);
  });
}

TEST_F(ReadSecondaryIndexTest, ReadReturnsCorrectPrimaryKeys) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  // Write data
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    tx.Write("user1", "Alice");
    tx.Write("user2", "Bob");

    std::string pk1 = "user1";
    tx.WriteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk1.data()),
                           pk1.size());
    std::string pk2 = "user2";
    tx.WriteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  db_->Fence();

  // Read and verify
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    auto primary_keys = tx.ReadSecondaryIndex("age_index", "25");
    EXPECT_EQ(primary_keys.size(), 2);

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(ReadSecondaryIndexTest, ReadThenFetchPrimaryData) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  tx.Write("user1", "Alice");
  tx.Write("user2", "Bob");

  std::string pk1 = "user1";
  tx.WriteSecondaryIndex("age_index", "25",
                         reinterpret_cast<const std::byte*>(pk1.data()),
                         pk1.size());
  std::string pk2 = "user2";
  tx.WriteSecondaryIndex("age_index", "25",
                         reinterpret_cast<const std::byte*>(pk2.data()),
                         pk2.size());

  // Read secondary index to get primary keys
  auto primary_keys = tx.ReadSecondaryIndex("age_index", "25");
  EXPECT_EQ(primary_keys.size(), 2);

  // Use primary keys to fetch actual data
  for (const auto& [pk_ptr, pk_size] : primary_keys) {
    std::string_view pk_key(reinterpret_cast<const char*>(pk_ptr), pk_size);
    auto [data_ptr, data_size] = tx.Read(pk_key);
    EXPECT_NE(data_ptr, nullptr);
    EXPECT_GT(data_size, 0);
  }

  db_->EndTransaction(tx, [](auto status) {
    EXPECT_EQ(status, LineairDB::TxStatus::Committed);
  });
}
