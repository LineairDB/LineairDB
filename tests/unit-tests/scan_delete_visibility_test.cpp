#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <memory>
#include <optional>
#include <vector>

#include "gtest/gtest.h"

class ScanDeleteVisibilityTest : public ::testing::Test {
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

TEST_F(ScanDeleteVisibilityTest, ScanShouldExcludeDeletedKeys) {
  // First transaction: insert keys
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("alice", 1);
    tx.Write<int>("bob", 2);
    tx.Write<int>("carol", 3);
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  // Second transaction: delete a key
  {
    auto& tx = db_->BeginTransaction();
    tx.Delete("bob");
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  // Third transaction: scan
  {
    auto& tx = db_->BeginTransaction();
    std::vector<std::string> scanned_keys;

    auto count = tx.Scan<int>("alice", "carol", [&](auto key, auto value) {
      scanned_keys.push_back(std::string(key));
      if (key == "alice") {
        EXPECT_EQ(value, 1);
      }
      if (key == "carol") {
        EXPECT_EQ(value, 3);
      }
      return false;
    });

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), size_t(2));  // bob should be excluded

    std::vector<std::string> expected = {"alice", "carol"};
    ASSERT_EQ(scanned_keys, expected);

    db_->EndTransaction(tx, [](auto) {});
  }
}

TEST_F(ScanDeleteVisibilityTest, ScanShouldExcludeReadYourWriteDeletedKeys) {
  // First transaction: insert keys
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("alice", 1);
    tx.Write<int>("bob", 2);
    tx.Write<int>("carol", 3);
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  // Second transaction: delete a key and scan within the same transaction
  {
    auto& tx = db_->BeginTransaction();
    tx.Delete("bob");

    std::vector<std::string> scanned_keys;

    auto count = tx.Scan<int>("alice", "carol", [&](auto key, auto value) {
      scanned_keys.push_back(std::string(key));
      if (key == "alice") {
        EXPECT_EQ(value, 1);
      }
      if (key == "carol") {
        EXPECT_EQ(value, 3);
      }
      return false;
    });

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), size_t(2));  // bob should be excluded

    std::vector<std::string> expected = {"alice", "carol"};
    ASSERT_EQ(scanned_keys, expected);

    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
  }
}
