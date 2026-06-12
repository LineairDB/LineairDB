#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <memory>
#include <vector>

#include "gtest/gtest.h"

class AbortedDeleteScanTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;

  void SetUp() override {
    config_.enable_recovery = false;
    config_.enable_logging = false;
    config_.enable_checkpointing = false;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(AbortedDeleteScanTest, AbortedDeleteMustRemainVisibleToScan) {
  {
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("bob", 42);
    ASSERT_TRUE(db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    }));
    db_->Fence();
  }

  {
    auto& tx = db_->BeginTransaction();
    tx.Delete("bob");
    tx.Abort();
    ASSERT_FALSE(db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Aborted);
    }));
    db_->Fence();
  }

  {
    auto& tx = db_->BeginTransaction();
    auto value = tx.Read<int>("bob");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value.value(), 42);
    ASSERT_TRUE(db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    }));
  }

  {
    auto& tx = db_->BeginTransaction();
    std::vector<std::string> scanned_keys;
    auto count = tx.Scan<int>("bob", "bob", [&](std::string_view key, int value) {
      scanned_keys.emplace_back(key);
      EXPECT_EQ(value, 42);
      return false;
    });

    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(count.value(), size_t(1));
    EXPECT_EQ(scanned_keys, std::vector<std::string>{"bob"});

    ASSERT_TRUE(db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    }));
  }
}
