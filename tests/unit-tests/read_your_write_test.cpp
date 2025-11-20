#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <memory>
#include <optional>

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
  db_->CreateTable("users");
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
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

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
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
}

// TODO: We should implement `ScanShouldExcludeDeletedKeys` test when the
// lineairdb supports delete operation.