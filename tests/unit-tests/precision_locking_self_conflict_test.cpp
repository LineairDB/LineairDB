#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <memory>
#include <optional>

#include "../test_helper.hpp"
#include "gtest/gtest.h"

class IndexTest : public ::testing::Test {
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

TEST_F(IndexTest, InsertAndScanConflictWithinSameTransaction) {
  db_->CreateTable("users");
  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  constexpr int erin = 4;
  tx.Insert<int>("erin", erin);

  tx.Scan("erin", std::nullopt, [&](auto, auto) { return false; });
  const bool committed = db_->EndTransaction(tx, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });
  ASSERT_TRUE(committed);
}