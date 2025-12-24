#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <memory>
#include <optional>
#include <thread>

#include "../test_helper.hpp"
#include "gtest/gtest.h"

class PrecisionLockingIndexTest : public ::testing::Test {
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

TEST_F(PrecisionLockingIndexTest, InsertAndScanConflictWithinSameTransaction) {
  db_->CreateTable("users");
  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  constexpr int erin = 4;
  tx.Write<int>("erin", erin);

  tx.Scan("erin", std::nullopt, [&](auto, auto) { return false; });
  const bool committed = db_->EndTransaction(tx, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });
  ASSERT_TRUE(committed);
}

TEST_F(PrecisionLockingIndexTest, InsertAfterFailedInsertDueToScan) {
  /**
   * Test scenario: Insert can succeed even after a previous Insert failed due
   * to concurrent Scan (phantom avoidance). This verifies that the insertion
   * failure does not permanently prevent future insertions.
   *
   * 1. Thread A: Scan that creates a predicate
   * 2. Thread B: Insert fails because it conflicts with Thread A's Scan
   * 3. -- Fence --
   * 4. Thread C: Insert should succeed
   *
   */
  const std::string test_key = "concurrent_insert_test_key";
  std::atomic<bool> scan_started(false);
  std::atomic<bool> first_insert_completed(false);
  std::atomic<bool> first_insert_succeeded(false);
  std::atomic<bool> second_insert_succeeded(false);

  // Thread 1: Perform a scan that may conflict with Insert
  std::thread scan_thread([&]() {
    scan_started.store(true);

    auto& tx = db_->BeginTransaction();
    tx.Scan<int>(test_key, std::nullopt,
                 [](std::string_view, int) { return false; });

    while (!first_insert_completed.load()) {
      std::this_thread::yield();
    }
    db_->EndTransaction(tx, [](auto) {});
  });

  // Thread 2: Try to insert while scan is active (may fail due to phantom
  // avoidance)
  std::thread first_insert_thread([&]() {
    // Wait for scan to start
    while (!scan_started.load()) {
      std::this_thread::yield();
    }

    auto& tx = db_->BeginTransaction();
    tx.Insert<int>(test_key, 100);
    db_->EndTransaction(tx, [&](auto status) {
      first_insert_completed.store(true);
      if (status == LineairDB::TxStatus::Committed) {
        first_insert_succeeded.store(true);
      }
    });
  });

  // 3. Fence
  first_insert_thread.join();
  scan_thread.join();
  db_->Fence();

  // Thread 3: Insert should now succeed, regardless of whether the first Insert
  // failed
  std::thread second_insert_thread([&]() {
    auto& tx = db_->BeginTransaction();
    tx.Insert<int>(test_key, 200);
    db_->EndTransaction(tx, [&](auto status) {
      if (status == LineairDB::TxStatus::Committed) {
        second_insert_succeeded.store(true);
      }
    });
    db_->Fence();
  });

  second_insert_thread.join();

  // Verify that at least one Insert succeeded (first or second)
  ASSERT_TRUE(first_insert_succeeded.load() || second_insert_succeeded.load())
      << "At least one Insert should have succeeded. "
      << "first_succeeded=" << first_insert_succeeded.load()
      << ", second_succeeded=" << second_insert_succeeded.load();

  // Verify the entry is accessible
  {
    auto& tx = db_->BeginTransaction();
    auto result = tx.Read<int>(test_key);
    ASSERT_TRUE(result.has_value())
        << "Entry should be readable after Insert operations";
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
  }

  // Verify the entry can be found in Scan
  {
    bool found_in_scan = false;
    auto& tx = db_->BeginTransaction();
    tx.Scan<int>(test_key, std::nullopt, [&](std::string_view key, int) {
      if (key == test_key) {
        found_in_scan = true;
      }
      return false;
    });
    db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });

    ASSERT_TRUE(found_in_scan)
        << "Entry should be found in Scan, proving it's properly indexed";
  }
}