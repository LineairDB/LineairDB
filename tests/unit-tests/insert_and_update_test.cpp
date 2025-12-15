#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <memory>
#include <optional>
#include <vector>

#include "gtest/gtest.h"

class InsertAndUpdateTest : public ::testing::Test {
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

TEST_F(InsertAndUpdateTest, Insert) {
  {
    auto& tx = db_->BeginTransaction();
    tx.Insert<int>("alice", 1);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }
  {
    auto& tx = db_->BeginTransaction();
    auto data = tx.Read<int>("alice");
    ASSERT_TRUE(data.has_value());
    ASSERT_EQ(data.value(), 1);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(InsertAndUpdateTest, InsertAndUpdate) {
  {
    auto& tx = db_->BeginTransaction();
    tx.Insert<int>("alice", 1);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }
  {
    auto& tx = db_->BeginTransaction();
    auto data = tx.Read<int>("alice");
    ASSERT_TRUE(data.has_value());
    ASSERT_EQ(data.value(), 1);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }
  {
    auto& tx = db_->BeginTransaction();
    tx.Update<int>("alice", 2);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }
  {
    auto& tx = db_->BeginTransaction();
    auto data = tx.Read<int>("alice");
    ASSERT_TRUE(data.has_value());
    ASSERT_EQ(data.value(), 2);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(InsertAndUpdateTest, InsertDoesNotAllowDuplicateKeys) {
  {
    auto& tx = db_->BeginTransaction();
    tx.Insert<int>("alice", 1);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }
  {
    auto& tx = db_->BeginTransaction();
    tx.Insert<int>("alice", 1);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Aborted);
    });
  }
}

TEST_F(InsertAndUpdateTest, InsertToEmptyEntry) {
  {
    auto& tx = db_->BeginTransaction();
    tx.Insert<int>("alice", 1);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }
  {
    auto& tx = db_->BeginTransaction();
    auto data = tx.Read<int>("alice");
    ASSERT_TRUE(data.has_value());
    ASSERT_EQ(data.value(), 1);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }
  {
    auto& tx = db_->BeginTransaction();
    tx.Delete("alice");
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }
  {
    auto& tx = db_->BeginTransaction();
    auto data = tx.Read<int>("alice");
    ASSERT_FALSE(data.has_value());
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
  {
    auto& tx = db_->BeginTransaction();
    tx.Insert<int>("alice", 1);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }
  {
    auto& tx = db_->BeginTransaction();
    auto data = tx.Read<int>("alice");
    ASSERT_TRUE(data.has_value());
    ASSERT_EQ(data.value(), 1);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}