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

TEST_F(InsertAndUpdateTest, UpdateToNonExistentKey) {
  {
    auto& tx = db_->BeginTransaction();
    tx.Update<int>("alice", 2);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Aborted);
    });
  }
}

TEST_F(InsertAndUpdateTest, UpdateToEmptyEntry) {
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
    tx.Delete("alice");
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }
  {
    auto& tx = db_->BeginTransaction();
    tx.Update<int>("alice", 2);
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Aborted);
    });
  }
}

TEST_F(InsertAndUpdateTest, SecondaryIndexReinsertAfterDelete) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  const std::string primary_key = "user1";

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    tx.Write("user1", "Alice");
    tx.WriteSecondaryIndex(
        "age_index", "25",
        reinterpret_cast<const std::byte*>(primary_key.data()),
        primary_key.size());
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    tx.DeleteSecondaryIndex(
        "age_index", "25",
        reinterpret_cast<const std::byte*>(primary_key.data()),
        primary_key.size());
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    tx.WriteSecondaryIndex(
        "age_index", "25",
        reinterpret_cast<const std::byte*>(primary_key.data()),
        primary_key.size());
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    db_->Fence();
  }

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    auto read_result = tx.ReadSecondaryIndex("age_index", "25");
    ASSERT_EQ(read_result.size(), 1u);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(read_result[0].first),
                          read_result[0].second),
              primary_key);

    std::vector<std::string> scanned_keys;
    auto scan_result = tx.ScanSecondaryIndex(
        "age_index", "25", std::nullopt,
        [&](std::string_view key, const std::vector<std::string> pks) {
          scanned_keys.emplace_back(key);
          EXPECT_EQ(pks.size(), 1u);
          if (!pks.empty()) {
            EXPECT_EQ(pks[0], primary_key);
          }
          return false;
        });
    ASSERT_TRUE(scan_result.has_value());
    EXPECT_EQ(scan_result.value(), size_t(1));
    ASSERT_EQ(scanned_keys.size(), 1u);
    EXPECT_EQ(scanned_keys[0], "25");

    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}

TEST_F(InsertAndUpdateTest, SecondaryIndexUpdateAfterInsert) {
  ASSERT_TRUE(db_->CreateTable("users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "age_index", 0));

  const std::string primary_key = "user1";

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    tx.Write("user1", "Alice");
    tx.WriteSecondaryIndex(
        "age_index", "25",
        reinterpret_cast<const std::byte*>(primary_key.data()),
        primary_key.size());
    tx.WriteSecondaryIndex(
        "age_index", "26",
        reinterpret_cast<const std::byte*>(primary_key.data()),
        primary_key.size());
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }
}
