#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <filesystem>
#include <memory>
#include <string>

#include "gtest/gtest.h"

namespace {

constexpr uint kDictUnique = 2;

LineairDB::TxStatus WriteSecondary(LineairDB::Database& db,
                                   const std::string& table_name,
                                   const std::string& primary_key,
                                   const std::string& value,
                                   const std::string& index_name,
                                   const std::string& secondary_key) {
  auto& tx = db.BeginTransaction();
  EXPECT_TRUE(tx.SetTable(table_name));
  tx.Write(primary_key, reinterpret_cast<const std::byte*>(value.data()),
           value.size());
  tx.WriteSecondaryIndex(index_name, secondary_key,
                         reinterpret_cast<const std::byte*>(primary_key.data()),
                         primary_key.size());

  LineairDB::TxStatus status = LineairDB::TxStatus::Running;
  db.EndTransaction(tx, [&](auto s) { status = s; });
  db.Fence();
  return status;
}

}  // namespace

class UniqueSecondaryIndexTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;

  void SetUp() override {
    std::filesystem::remove_all("lineairdb_logs");
    config_.max_thread = 4;
    config_.epoch_duration_ms = 100;
    config_.checkpoint_period = 1;
  }

  void TearDown() override { std::filesystem::remove_all("lineairdb_logs"); }
};

TEST_F(UniqueSecondaryIndexTest, DictUniqueFlagRejectsDuplicateSecondaryKey) {
  config_.enable_logging = false;
  config_.enable_recovery = false;
  config_.enable_checkpointing = false;

  LineairDB::Database db(config_);
  ASSERT_TRUE(db.CreateTable("users"));
  ASSERT_TRUE(db.CreateSecondaryIndex("users", "email_idx", kDictUnique));

  const auto status1 = WriteSecondary(db, "users", "user1", "Alice",
                                      "email_idx", "alice@example.com");
  ASSERT_EQ(status1, LineairDB::TxStatus::Committed);

  const auto status2 = WriteSecondary(db, "users", "user2", "Bob", "email_idx",
                                      "alice@example.com");
  EXPECT_EQ(status2, LineairDB::TxStatus::Aborted);
}

TEST_F(UniqueSecondaryIndexTest,
       RecoveryRestoresUniqueSecondaryIndexTypeFromLogs) {
  config_.enable_logging = true;
  config_.enable_recovery = true;
  config_.enable_checkpointing = false;

  {
    LineairDB::Database db(config_);
    ASSERT_TRUE(db.CreateTable("users"));
    ASSERT_TRUE(db.CreateSecondaryIndex("users", "email_idx", kDictUnique));

    const auto status1 = WriteSecondary(db, "users", "user1", "Alice",
                                        "email_idx", "alice@example.com");
    ASSERT_EQ(status1, LineairDB::TxStatus::Committed);
  }

  LineairDB::Database recovered_db(config_);

  {
    auto& tx = recovered_db.BeginTransaction();
    ASSERT_TRUE(tx.SetTable("users"));
    const auto result = tx.ReadSecondaryIndex("email_idx", "alice@example.com");
    ASSERT_EQ(result.size(), 1u);
    recovered_db.EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    recovered_db.Fence();
  }

  const auto status2 = WriteSecondary(recovered_db, "users", "user2", "Bob",
                                      "email_idx", "alice@example.com");
  EXPECT_EQ(status2, LineairDB::TxStatus::Aborted);
}
