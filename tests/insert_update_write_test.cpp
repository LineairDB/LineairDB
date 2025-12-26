#include <gtest/gtest.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <atomic>

#include "test_helper.hpp"

/**
 * @file insert_update_write_test.cpp
 * @brief Test cases for Insert, Update, and Write operations in LineairDB.
 *
 */

/**
 * Test case 1: Insert operation
 * - Insert succeeds when the key does not exist
 * - Insert fails when the key already exists (transaction should abort)
 */
TEST(InsertUpdateWriteTest, InsertBehavior) {
  LineairDB::Config config;
  config.enable_recovery = false;
  LineairDB::Database db(config);
  std::string key = "insert_test_key";
  int value1 = 100;
  int value2 = 200;

  // First insert should succeed
  std::atomic<bool> first_committed(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Insert(key, value1); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          first_committed.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(first_committed.load());

  // Verify the value was inserted
  std::atomic<bool> verified(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value1) {
          verified.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(verified.load());

  // Second insert with the same key should fail (transaction aborts)
  std::atomic<bool> second_aborted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Insert(key, value2); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Aborted) {
          second_aborted.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(second_aborted.load());

  // Verify the original value is still there
  std::atomic<bool> value_unchanged(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value1) {
          value_unchanged.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(value_unchanged.load());
}

/**
 * Test case 2: Update operation
 * - Update fails when the key does not exist (transaction should abort)
 * - Update succeeds when the key exists
 */
TEST(InsertUpdateWriteTest, UpdateBehavior) {
  LineairDB::Config config;
  config.enable_recovery = false;
  LineairDB::Database db(config);
  std::string key = "update_test_key";
  int value1 = 300;
  int value2 = 400;

  // Try to update a non-existent key (should fail)
  std::atomic<bool> update_aborted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Update(key, value1); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Aborted) {
          update_aborted.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(update_aborted.load());

  // Verify the key still doesn't exist
  std::atomic<bool> key_not_exists(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (!result.has_value()) {
          key_not_exists.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(key_not_exists.load());

  // Insert the key first
  std::atomic<bool> inserted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Insert(key, value1); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          inserted.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(inserted.load());

  // Now update should succeed
  std::atomic<bool> updated(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Update(key, value2); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          updated.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(updated.load());

  // Verify the value was updated
  std::atomic<bool> value_updated(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value2) {
          value_updated.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(value_updated.load());
}

/**
 * Test case 3: Write operation (Upsert)
 * - Write never fails
 * - Write inserts when the key does not exist
 * - Write updates when the key already exists
 */
TEST(InsertUpdateWriteTest, WriteBehavior) {
  LineairDB::Config config;
  config.enable_recovery = false;
  LineairDB::Database db(config);
  std::string key = "write_test_key";
  int value1 = 500;
  int value2 = 600;

  // Write to a non-existent key (should insert)
  std::atomic<bool> first_write_committed(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Write(key, value1); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          first_write_committed.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(first_write_committed.load());

  // Verify the value was inserted
  std::atomic<bool> value_inserted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value1) {
          value_inserted.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(value_inserted.load());

  // Write to an existing key (should update)
  std::atomic<bool> second_write_committed(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Write(key, value2); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          second_write_committed.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(second_write_committed.load());

  // Verify the value was updated
  std::atomic<bool> value_updated_to_v2(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value2) {
          value_updated_to_v2.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(value_updated_to_v2.load());

  // Write again to demonstrate it never fails
  int value3 = 700;
  std::atomic<bool> third_write_committed(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Write(key, value3); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          third_write_committed.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(third_write_committed.load());

  // Final verification
  std::atomic<bool> final_value_correct(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value3) {
          final_value_correct.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(final_value_correct.load());
}
