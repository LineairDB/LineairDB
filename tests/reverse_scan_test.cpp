#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <memory>
#include <optional>
#include <vector>

#include "gtest/gtest.h"

class ReverseScanTest : public ::testing::Test {
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

TEST_F(ReverseScanTest, ScanReverseShouldReturnKeysInReverseOrder) {
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

  {
    auto& tx = db_->BeginTransaction();
    std::vector<std::string> scanned_keys;
    auto count = tx.Scan<int>(
        "alice", "carol",
        [&](auto key, auto) {
          scanned_keys.push_back(std::string(key));
          return false;
        },
        {LineairDB::Transaction::ScanOption::REVERSE});
    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), size_t(3));
    std::vector<std::string> expected = {"carol", "bob", "alice"};
    ASSERT_EQ(scanned_keys, expected);
    db_->EndTransaction(tx, [](auto) {});
  }
}

TEST_F(ReverseScanTest, ScanReverseShouldExcludeDeletedKeys) {
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

    auto count = tx.Scan<int>(
        "alice", "carol",
        [&](auto key, auto value) {
          scanned_keys.push_back(std::string(key));
          if (key == "alice") {
            EXPECT_EQ(value, 1);
          }
          if (key == "carol") {
            EXPECT_EQ(value, 3);
          }
          return false;
        },
        {LineairDB::Transaction::ScanOption::REVERSE});

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), size_t(2));  // bob should be excluded

    std::vector<std::string> expected = {"carol", "alice"};
    ASSERT_EQ(scanned_keys, expected);

    db_->EndTransaction(tx, [](auto) {});
  }
}

TEST_F(ReverseScanTest, ScanReverseShouldExcludeReadYourWriteDeletedKeys) {
  // First transaction: insert keysReverse
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

    auto count = tx.Scan<int>(
        "alice", "carol",
        [&](auto key, auto value) {
          scanned_keys.push_back(std::string(key));
          if (key == "alice") {
            EXPECT_EQ(value, 1);
          }
          if (key == "carol") {
            EXPECT_EQ(value, 3);
          }
          return false;
        },
        {LineairDB::Transaction::ScanOption::REVERSE});

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), size_t(2));  // bob should be excluded

    std::vector<std::string> expected = {"carol", "alice"};
    ASSERT_EQ(scanned_keys, expected);

    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
  }
}

TEST_F(ReverseScanTest, ScanReverseShouldStopEarly) {
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

  {
    auto& tx = db_->BeginTransaction();
    std::vector<std::string> scanned_keys;
    auto count = tx.Scan<int>(
        "alice", "carol",
        [&](auto key, auto) {
          scanned_keys.push_back(std::string(key));
          return true;
        },
        {LineairDB::Transaction::ScanOption::REVERSE});
    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), size_t(1));
    std::vector<std::string> expected = {"carol"};
    ASSERT_EQ(scanned_keys, expected);
    db_->EndTransaction(tx, [](auto) {});
  }
}

TEST_F(ReverseScanTest, ScanReverseWithoutEndShouldIncludeTail) {
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("alice", 1);
    tx.Write<int>("bob", 2);
    tx.Write<int>("carol", 3);
    tx.Write<int>("dave", 4);
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  {
    auto& tx = db_->BeginTransaction();
    std::vector<std::string> scanned_keys;
    auto count = tx.Scan<int>(
        "bob", std::nullopt,
        [&](auto key, auto) {
          scanned_keys.push_back(std::string(key));
          return false;
        },
        {LineairDB::Transaction::ScanOption::REVERSE});
    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), size_t(3));
    std::vector<std::string> expected = {"dave", "carol", "bob"};
    ASSERT_EQ(scanned_keys, expected);
    db_->EndTransaction(tx, [](auto) {});
  }
}

TEST_F(ReverseScanTest, ScanReverseWithInvalidRangeShouldReturnNullopt) {
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

  {
    auto& tx = db_->BeginTransaction();
    auto count = tx.Scan<int>(
        "carol", "alice", [&](auto, auto) { return false; },
        {LineairDB::Transaction::ScanOption::REVERSE});
    ASSERT_FALSE(count.has_value());
    db_->EndTransaction(tx, [](auto) {});
  }
}
