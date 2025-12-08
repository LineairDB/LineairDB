#include <filesystem>
#include <memory>
#include <set>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/transaction.h"
#include "lineairdb/tx_status.h"

class DeleteSecondaryIndexTest : public ::testing::Test {
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

TEST_F(DeleteSecondaryIndexTest, DeleteRemovesPrimaryKey) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  // Setup: write data
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    tx.Write("user1", "Alice");
    std::string pk = "user1";
    tx.WriteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk.data()),
                           pk.size());

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  db_->Fence();

  // Delete the entry
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    std::string pk = "user1";
    auto before_delete = tx.ReadSecondaryIndex("age_index", "25");
    ASSERT_EQ(before_delete.size(), 1u);

    tx.DeleteSecondaryIndex("age_index", "25",
                            reinterpret_cast<const std::byte*>(pk.data()),
                            pk.size());

    auto after_delete = tx.ReadSecondaryIndex("age_index", "25");
    EXPECT_TRUE(after_delete.empty());

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  db_->Fence();

  // Verify deletion persists
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    auto result = tx.ReadSecondaryIndex("age_index", "25");
    EXPECT_TRUE(result.empty());

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(DeleteSecondaryIndexTest, DeleteRemovesOnlySpecifiedPrimaryKey) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  // Setup: write multiple entries with same secondary key
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    tx.Write("user1", "Alice");
    tx.Write("user2", "Bob");
    tx.Write("user3", "Carol");

    std::string pk1 = "user1";
    std::string pk2 = "user2";
    std::string pk3 = "user3";

    tx.WriteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk1.data()),
                           pk1.size());
    tx.WriteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());
    tx.WriteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk3.data()),
                           pk3.size());

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  db_->Fence();

  // Delete only user2
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    auto before_delete = tx.ReadSecondaryIndex("age_index", "25");
    ASSERT_EQ(before_delete.size(), 3u);

    std::string pk2 = "user2";
    tx.DeleteSecondaryIndex("age_index", "25",
                            reinterpret_cast<const std::byte*>(pk2.data()),
                            pk2.size());

    auto after_delete = tx.ReadSecondaryIndex("age_index", "25");
    ASSERT_EQ(after_delete.size(), 2u);

    std::set<std::string> expected{"user1", "user3"};
    std::set<std::string> actual;
    for (const auto& [pk_ptr, pk_size] : after_delete) {
      actual.emplace(reinterpret_cast<const char*>(pk_ptr), pk_size);
    }
    EXPECT_EQ(actual, expected);

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  db_->Fence();

  // Verify only user2 was deleted
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    auto result = tx.ReadSecondaryIndex("age_index", "25");
    ASSERT_EQ(result.size(), 2u);

    std::set<std::string> expected{"user1", "user3"};
    std::set<std::string> actual;
    for (const auto& [pk_ptr, pk_size] : result) {
      actual.emplace(reinterpret_cast<const char*>(pk_ptr), pk_size);
    }
    EXPECT_EQ(actual, expected);

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}
