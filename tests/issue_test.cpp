
#include <filesystem>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/transaction.h"
#include "lineairdb/tx_status.h"

class IssueTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread = 4;
    config_.checkpoint_period = 1;
    config_.epoch_duration_ms = 100;
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(IssueTest, FenceShouldWaitForAllCallbacks_ExecuteInterface) {
  int value_of_alice = 1;
  std::atomic<bool> callback_executed{false};
  for (size_t i = 0; i < 30; i++) {
    callback_executed.store(false);
    db_->ExecuteTransaction(
        [&](LineairDB::Transaction& tx) {
          tx.Write<int>("alice", value_of_alice);
        },
        [&](LineairDB::TxStatus) { callback_executed.store(true); });
    db_->Fence();
    ASSERT_TRUE(callback_executed.load());
  }
}

TEST_F(IssueTest, FenceShouldWaitForAllCallbacks_HandlerInterface) {
  int value_of_alice = 1;
  std::atomic<bool> callback_executed{false};

  for (size_t i = 0; i < 30; i++) {
    callback_executed.store(false);
    auto& tx = db_->BeginTransaction();
    tx.Write<int>("alice", value_of_alice);
    db_->EndTransaction(
        tx, [&](LineairDB::TxStatus) { callback_executed.store(true); });
    db_->Fence();
    ASSERT_TRUE(callback_executed.load());
  }
}
