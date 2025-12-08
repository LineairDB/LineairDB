#include <filesystem>
#include <memory>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/transaction.h"
#include "lineairdb/tx_status.h"

class UpdateSecondaryIndexTest : public ::testing::Test {
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

TEST_F(UpdateSecondaryIndexTest, UpdateMovesPrimaryKeyToNewKey) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  // Setup
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

  // Update: move from "25" to "30"
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    std::string pk = "user1";

    auto before_update = tx.ReadSecondaryIndex("age_index", "25");
    ASSERT_EQ(before_update.size(), 1u);

    tx.UpdateSecondaryIndex("age_index", "25", "30",
                            reinterpret_cast<const std::byte*>(pk.data()),
                            pk.size());

    // Verify read-your-own-write
    auto old_key_result = tx.ReadSecondaryIndex("age_index", "25");
    EXPECT_TRUE(old_key_result.empty());

    auto new_key_result = tx.ReadSecondaryIndex("age_index", "30");
    ASSERT_EQ(new_key_result.size(), 1u);
    EXPECT_EQ(
        std::string(reinterpret_cast<const char*>(new_key_result[0].first),
                    new_key_result[0].second),
        "user1");

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  db_->Fence();

  // Verify after commit
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    auto old_key_result = tx.ReadSecondaryIndex("age_index", "25");
    EXPECT_TRUE(old_key_result.empty());

    auto new_key_result = tx.ReadSecondaryIndex("age_index", "30");
    ASSERT_EQ(new_key_result.size(), 1u);

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(UpdateSecondaryIndexTest, UpdateDoesNotDuplicateIfAlreadyExists) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  // Setup: register same primary key under both old and new keys
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    tx.Write("user1", "Alice");

    std::string pk = "user1";
    tx.WriteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk.data()),
                           pk.size());
    tx.WriteSecondaryIndex("age_index", "30",
                           reinterpret_cast<const std::byte*>(pk.data()),
                           pk.size());

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  db_->Fence();

  // Update from "25" to "30" - should not add duplicate
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    std::string pk = "user1";

    auto before_update = tx.ReadSecondaryIndex("age_index", "25");
    ASSERT_EQ(before_update.size(), 1u);

    tx.UpdateSecondaryIndex("age_index", "25", "30",
                            reinterpret_cast<const std::byte*>(pk.data()),
                            pk.size());

    auto old_key_result = tx.ReadSecondaryIndex("age_index", "25");
    EXPECT_TRUE(old_key_result.empty());

    // Should still have only 1 entry (no duplicate)
    auto new_key_result = tx.ReadSecondaryIndex("age_index", "30");
    ASSERT_EQ(new_key_result.size(), 1u);

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(UpdateSecondaryIndexTest, UpdateWithMissingOldKeyActsAsInsert) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    tx.Write("user1", "Alice");
    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  db_->Fence();

  // Update with non-existent old key - should act as insert
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    std::string pk = "user1";

    tx.UpdateSecondaryIndex("age_index", "99", "40",
                            reinterpret_cast<const std::byte*>(pk.data()),
                            pk.size());

    auto old_key_result = tx.ReadSecondaryIndex("age_index", "99");
    EXPECT_TRUE(old_key_result.empty());

    auto new_key_result = tx.ReadSecondaryIndex("age_index", "40");
    ASSERT_EQ(new_key_result.size(), 1u);

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  db_->Fence();

  // Verify after commit
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    auto result = tx.ReadSecondaryIndex("age_index", "40");
    ASSERT_EQ(result.size(), 1u);

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(UpdateSecondaryIndexTest, MultipleUpdatesInSingleTransaction) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  // Setup
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    tx.Write("user1", "Alice");
    std::string pk = "user1";
    tx.WriteSecondaryIndex("age_index", "18",
                           reinterpret_cast<const std::byte*>(pk.data()),
                           pk.size());

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  db_->Fence();

  // Multiple updates: 18 -> 19 -> 20
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    std::string pk = "user1";

    auto r1 = tx.ReadSecondaryIndex("age_index", "18");
    ASSERT_EQ(r1.size(), 1u);

    tx.UpdateSecondaryIndex("age_index", "18", "19",
                            reinterpret_cast<const std::byte*>(pk.data()),
                            pk.size());

    auto r2 = tx.ReadSecondaryIndex("age_index", "19");
    ASSERT_EQ(r2.size(), 1u);

    tx.UpdateSecondaryIndex("age_index", "19", "20",
                            reinterpret_cast<const std::byte*>(pk.data()),
                            pk.size());

    auto r3 = tx.ReadSecondaryIndex("age_index", "20");
    ASSERT_EQ(r3.size(), 1u);

    // Old keys should be empty
    auto r4_old = tx.ReadSecondaryIndex("age_index", "19");
    EXPECT_TRUE(r4_old.empty());

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  db_->Fence();

  // Verify final state
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    auto old18 = tx.ReadSecondaryIndex("age_index", "18");
    EXPECT_TRUE(old18.empty());

    auto current = tx.ReadSecondaryIndex("age_index", "20");
    ASSERT_EQ(current.size(), 1u);

    db_->EndTransaction(tx, [](auto status) {
      EXPECT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}
