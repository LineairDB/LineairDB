#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <memory>
#include <optional>
#include <vector>

#include "gtest/gtest.h"

class ReadYourWriteTest : public ::testing::Test {
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

TEST_F(ReadYourWriteTest, ScanShouldIncludeInsertedKeys) {
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("alice", 1);
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  constexpr int erin = 4;
  constexpr int bob = 5;
  constexpr int carol = 6;

  auto& tx = db_->BeginTransaction();
  tx.Write<int>("erin", erin);
  tx.Write<int>("bob", bob);
  tx.Write<int>("carol", carol);

  auto count = tx.Scan<int>("alice", "erin", [&](auto key, auto value) {
    if (key == "erin") {
      EXPECT_EQ(value, erin);
    }
    if (key == "bob") {
      EXPECT_EQ(value, bob);
    }
    if (key == "carol") {
      EXPECT_EQ(value, carol);
    }
    return false;
  });
  ASSERT_TRUE(count.has_value());
  ASSERT_EQ(count.value(), 4);

  const bool committed = db_->EndTransaction(tx, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });
  ASSERT_TRUE(committed);
}

TEST_F(ReadYourWriteTest, ScanShouldReturnKeysInOrder) {
  // First transaction: insert some keys into index
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("alice", 1);
    tx.Write<int>("diana", 4);
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  // Second transaction: write new keys and scan
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("bob", 2);
    tx.Write<int>("carol", 3);
    tx.Write<int>("erin", 5);

    // Scan should return keys in alphabetical order:
    // alice(index), bob(write_set), carol(write_set), diana(index),
    // erin(write_set)
    std::vector<std::string> expected_order = {"alice", "bob", "carol", "diana",
                                               "erin"};
    std::vector<std::string> actual_order;

    auto count = tx.Scan<int>("alice", "erin", [&](auto key, auto value) {
      actual_order.push_back(std::string(key));

      // Verify values match expected
      if (key == "alice") {
        EXPECT_EQ(value, 1);
      }
      if (key == "bob") {
        EXPECT_EQ(value, 2);
      }
      if (key == "carol") {
        EXPECT_EQ(value, 3);
      }
      if (key == "diana") {
        EXPECT_EQ(value, 4);
      }
      if (key == "erin") {
        EXPECT_EQ(value, 5);
      }

      return false;
    });

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), 5);
    ASSERT_EQ(actual_order, expected_order);

    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
  }
}

TEST_F(ReadYourWriteTest, ScanShouldStopAtCorrectPosition) {
  // Insert keys into index
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("alice", 1);
    tx.Write<int>("diana", 4);
    tx.Write<int>("frank", 6);
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  // Write new keys and scan with early stop
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("bob", 2);
    tx.Write<int>("carol", 3);
    tx.Write<int>("erin", 5);

    std::vector<std::string> scanned_keys;

    auto count = tx.Scan<int>("alice", "zzz", [&](auto key, auto) {
      scanned_keys.push_back(std::string(key));

      // Stop after "carol" (3rd key in sorted order)
      if (key == "carol") {
        return true;  // Stop scanning
      }
      return false;
    });

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), 3);  // alice, bob, carol

    // Verify we scanned exactly: alice, bob, carol (in order)
    std::vector<std::string> expected = {"alice", "bob", "carol"};
    ASSERT_EQ(scanned_keys, expected);

    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
  }
}

// TODO: We should implement `ScanShouldExcludeDeletedKeys` test when the
// lineairdb supports delete operation.
