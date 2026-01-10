#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"

class ScanSecondaryIndexTest : public ::testing::Test {
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

// ============================================================
// 既存のテスト（クラス名をScanSecondaryIndexTestに変更）
// ============================================================

TEST_F(ScanSecondaryIndexTest, Delete) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "age_index", 0);
  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  tx.Write("user1", "Alice");
  std::string pk = "user1";
  tx.WriteSecondaryIndex("age_index", "25",
                         reinterpret_cast<const std::byte*>(pk.data()),
                         pk.size());
  auto result = tx.ReadSecondaryIndex("age_index", "25");
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(result[0].second, pk.size());
  std::string pk_from_index(reinterpret_cast<const char*>(result[0].first),
                            result[0].second);
  ASSERT_EQ(pk_from_index, pk);

  db_->EndTransaction(tx, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });

  db_->Fence();

  auto& tx2 = db_->BeginTransaction();
  tx2.SetTable("users");
  tx2.DeleteSecondaryIndex("age_index", "25",
                           reinterpret_cast<const std::byte*>(pk.data()),
                           pk.size());
  auto result2 = tx2.ReadSecondaryIndex("age_index", "25");
  ASSERT_EQ(result2.size(), 0);

  db_->EndTransaction(tx2, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });
}

TEST_F(ScanSecondaryIndexTest, DeleteAndScan) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "alpha_index", 0);
  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  tx.Write("user1", "Alice");
  tx.Write("user2", "Bob");
  tx.Write("user3", "Carol");
  std::string pk1 = "user1";
  std::string pk2 = "user2";
  std::string pk3 = "user3";

  tx.WriteSecondaryIndex("alpha_index", "a",
                         reinterpret_cast<const std::byte*>(pk1.data()),
                         pk1.size());
  tx.WriteSecondaryIndex("alpha_index", "b",
                         reinterpret_cast<const std::byte*>(pk2.data()),
                         pk2.size());
  tx.WriteSecondaryIndex("alpha_index", "c",
                         reinterpret_cast<const std::byte*>(pk3.data()),
                         pk3.size());

  db_->EndTransaction(tx, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });

  db_->Fence();

  auto& tx2 = db_->BeginTransaction();
  tx2.SetTable("users");

  std::vector<std::pair<std::string, std::string>> scan_results;
  auto result = tx2.ScanSecondaryIndex(
      "alpha_index", "a", "c", [&](auto key, auto primary_keys) {
        for (const auto& primary_key : primary_keys) {
          std::string pk_from_index(
              reinterpret_cast<const char*>(primary_key.data()),
              primary_key.size());
          scan_results.emplace_back(std::string(key), pk_from_index);
        }
        return false;
      });
  ASSERT_EQ(result.value(), 3);
  ASSERT_EQ(scan_results.size(), 3);
  EXPECT_EQ(scan_results[0].first, "a");
  EXPECT_EQ(scan_results[0].second, pk1);
  EXPECT_EQ(scan_results[1].first, "b");
  EXPECT_EQ(scan_results[1].second, pk2);
  EXPECT_EQ(scan_results[2].first, "c");
  EXPECT_EQ(scan_results[2].second, pk3);

  db_->EndTransaction(tx2, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });

  db_->Fence();

  auto& tx3 = db_->BeginTransaction();
  tx3.SetTable("users");
  tx3.DeleteSecondaryIndex("alpha_index", "b",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());
  auto result2 = tx3.ReadSecondaryIndex("alpha_index", "b");
  ASSERT_EQ(result2.size(), 0);

  db_->EndTransaction(tx3, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });

  db_->Fence();

  auto& tx4 = db_->BeginTransaction();
  tx4.SetTable("users");

  scan_results.clear();
  auto result3 = tx4.ScanSecondaryIndex(
      "alpha_index", "a", "c", [&](auto key, auto primary_keys) {
        for (const auto& primary_key : primary_keys) {
          std::string pk_from_index(
              reinterpret_cast<const char*>(primary_key.data()),
              primary_key.size());
          scan_results.emplace_back(std::string(key), pk_from_index);
        }
        return false;
      });
  ASSERT_EQ(result3.value(), 2);
  ASSERT_EQ(scan_results.size(), 2);
  EXPECT_EQ(scan_results[0].first, "a");
  EXPECT_EQ(scan_results[0].second, pk1);
  EXPECT_EQ(scan_results[1].first, "c");
  EXPECT_EQ(scan_results[1].second, pk3);

  db_->EndTransaction(tx4, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });
}

// ============================================================
// 新規テスト: Read Your Own Writes（同一トランザクション内）
// ============================================================

// 同一トランザクション内でInsertしてScanする（self-conflict回避確認）
TEST_F(ScanSecondaryIndexTest, InsertAndScanWithinSameTransaction) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "name_index", 0);

  auto& tx = db_->BeginTransaction();
  tx.SetTable("users");

  std::string pk = "user1";
  tx.Write("user1", "Alice");
  tx.WriteSecondaryIndex("name_index", "alice",
                         reinterpret_cast<const std::byte*>(pk.data()),
                         pk.size());

  // 同一トランザクション内でScanSecondaryIndex
  auto result = tx.ScanSecondaryIndex("name_index", "alice", std::nullopt,
                                      [&](auto, auto) { return false; });

  ASSERT_TRUE(result.has_value());
  ASSERT_GE(result.value(), 1);

  const bool committed = db_->EndTransaction(tx, [](auto status) {
    ASSERT_EQ(status, LineairDB::TxStatus::Committed);
  });
  ASSERT_TRUE(committed);
}

// ScanSecondaryIndexがwrite_setに挿入されたキーを含むことを確認
TEST_F(ScanSecondaryIndexTest, ScanShouldIncludeInsertedKeys) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "name_index", 0);

  // 最初のトランザクション: インデックスにいくつかのキーを挿入
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    std::string pk1 = "user1";
    tx.Write("user1", "Alice");
    tx.WriteSecondaryIndex("name_index", "alice",
                           reinterpret_cast<const std::byte*>(pk1.data()),
                           pk1.size());
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  // 2番目のトランザクション: 新しいキーを追加してScan
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    std::string pk2 = "user2";
    std::string pk3 = "user3";
    std::string pk4 = "user4";
    tx.Write("user2", "Bob");
    tx.Write("user3", "Carol");
    tx.Write("user4", "Erin");
    tx.WriteSecondaryIndex("name_index", "bob",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());
    tx.WriteSecondaryIndex("name_index", "carol",
                           reinterpret_cast<const std::byte*>(pk3.data()),
                           pk3.size());
    tx.WriteSecondaryIndex("name_index", "erin",
                           reinterpret_cast<const std::byte*>(pk4.data()),
                           pk4.size());

    std::vector<std::pair<std::string, std::string>> scan_results;
    auto count = tx.ScanSecondaryIndex(
        "name_index", "alice", "erin", [&](auto key, auto primary_keys) {
          for (const auto& primary_key : primary_keys) {
            scan_results.emplace_back(std::string(key), primary_key);
          }
          return false;
        });

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), 4);  // alice, bob, carol, erin
    ASSERT_EQ(scan_results.size(), 4);

    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
  }
}

// ScanSecondaryIndexがキーをソートされた順序で返すことを確認
TEST_F(ScanSecondaryIndexTest, ScanShouldReturnKeysInOrder) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "name_index", 0);

  // 最初のトランザクション: インデックスにキーを挿入
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    std::string pk1 = "user1";
    std::string pk4 = "user4";
    tx.Write("user1", "Alice");
    tx.Write("user4", "Diana");
    tx.WriteSecondaryIndex("name_index", "alice",
                           reinterpret_cast<const std::byte*>(pk1.data()),
                           pk1.size());
    tx.WriteSecondaryIndex("name_index", "diana",
                           reinterpret_cast<const std::byte*>(pk4.data()),
                           pk4.size());
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  // 2番目のトランザクション: 新しいキーを追加してScan
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    std::string pk2 = "user2";
    std::string pk3 = "user3";
    std::string pk5 = "user5";
    tx.Write("user2", "Bob");
    tx.Write("user3", "Carol");
    tx.Write("user5", "Erin");
    tx.WriteSecondaryIndex("name_index", "bob",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());
    tx.WriteSecondaryIndex("name_index", "carol",
                           reinterpret_cast<const std::byte*>(pk3.data()),
                           pk3.size());
    tx.WriteSecondaryIndex("name_index", "erin",
                           reinterpret_cast<const std::byte*>(pk5.data()),
                           pk5.size());

    // Scanはアルファベット順で返すべき:
    // alice(index), bob(write_set), carol(write_set), diana(index),
    // erin(write_set)
    std::vector<std::string> expected_order = {"alice", "bob", "carol", "diana",
                                               "erin"};
    std::vector<std::string> actual_order;

    auto count = tx.ScanSecondaryIndex(
        "name_index", "alice", "erin", [&](auto key, auto primary_keys) {
          actual_order.push_back(std::string(key));
          return false;
        });

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), 5);
    ASSERT_EQ(actual_order, expected_order);

    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
  }
}

TEST_F(ScanSecondaryIndexTest,
       ScanReverseShouldReturnPrimaryKeysInReverseOrder) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "group_index", 0);

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    std::string pk1 = "user1";
    std::string pk2 = "user2";
    std::string pk3 = "user3";
    tx.Write(pk2, "Bob");
    tx.Write(pk1, "Alice");
    tx.Write(pk3, "Carol");
    tx.WriteSecondaryIndex("group_index", "group",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());
    tx.WriteSecondaryIndex("group_index", "group",
                           reinterpret_cast<const std::byte*>(pk1.data()),
                           pk1.size());
    tx.WriteSecondaryIndex("group_index", "group",
                           reinterpret_cast<const std::byte*>(pk3.data()),
                           pk3.size());
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    std::vector<std::string> actual_order;
    auto count = tx.ScanSecondaryIndexReverse(
        "group_index", "group", "group", [&](auto key, auto primary_keys) {
          EXPECT_EQ(std::string(key), "group");
          for (const auto& primary_key : primary_keys) {
            actual_order.emplace_back(primary_key);
          }
          return false;
        });

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), 1);

    std::vector<std::string> expected_order = {"user3", "user2", "user1"};
    ASSERT_EQ(actual_order, expected_order);

    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
  }
}

TEST_F(ScanSecondaryIndexTest, ScanShouldStopAtCorrectPosition) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "name_index", 0);

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    std::string pk1 = "user1";
    std::string pk4 = "user4";
    std::string pk6 = "user6";
    tx.Write("user1", "Alice");
    tx.Write("user4", "Diana");
    tx.Write("user6", "Frank");
    tx.WriteSecondaryIndex("name_index", "alice",
                           reinterpret_cast<const std::byte*>(pk1.data()),
                           pk1.size());
    tx.WriteSecondaryIndex("name_index", "diana",
                           reinterpret_cast<const std::byte*>(pk4.data()),
                           pk4.size());
    tx.WriteSecondaryIndex("name_index", "frank",
                           reinterpret_cast<const std::byte*>(pk6.data()),
                           pk6.size());
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    std::string pk2 = "user2";
    std::string pk3 = "user3";
    std::string pk5 = "user5";
    tx.Write("user2", "Bob");
    tx.Write("user3", "Carol");
    tx.Write("user5", "Erin");
    tx.WriteSecondaryIndex("name_index", "bob",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());
    tx.WriteSecondaryIndex("name_index", "carol",
                           reinterpret_cast<const std::byte*>(pk3.data()),
                           pk3.size());
    tx.WriteSecondaryIndex("name_index", "erin",
                           reinterpret_cast<const std::byte*>(pk5.data()),
                           pk5.size());

    std::vector<std::string> scanned_keys;

    auto count = tx.ScanSecondaryIndex(
        "name_index", "alice", "zzz", [&](auto key, auto) {
          scanned_keys.push_back(std::string(key));

          if (key == "carol") {
            return true; 
          }
          return false;
        });

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), 3);  // alice, bob, carol

    std::vector<std::string> expected = {"alice", "bob", "carol"};
    ASSERT_EQ(scanned_keys, expected);

    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
  }
}

// 別トランザクションで削除されたキーがScanSecondaryIndexから除外されることを確認
TEST_F(ScanSecondaryIndexTest, ScanShouldExcludeDeletedKeys) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "name_index", 0);

  std::string pk1 = "user1";
  std::string pk2 = "user2";
  std::string pk3 = "user3";

  // 最初のトランザクション: キーを挿入
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    tx.Write("user1", "Alice");
    tx.Write("user2", "Bob");
    tx.Write("user3", "Carol");
    tx.WriteSecondaryIndex("name_index", "alice",
                           reinterpret_cast<const std::byte*>(pk1.data()),
                           pk1.size());
    tx.WriteSecondaryIndex("name_index", "bob",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());
    tx.WriteSecondaryIndex("name_index", "carol",
                           reinterpret_cast<const std::byte*>(pk3.data()),
                           pk3.size());
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  // 2番目のトランザクション: キーを削除
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    tx.DeleteSecondaryIndex("name_index", "bob",
                            reinterpret_cast<const std::byte*>(pk2.data()),
                            pk2.size());
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  // 3番目のトランザクション: Scan
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    std::vector<std::string> scanned_keys;

    auto count = tx.ScanSecondaryIndex(
        "name_index", "alice", "carol", [&](auto key, auto primary_keys) {
          scanned_keys.push_back(std::string(key));
          return false;
        });

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), 2);  // bob should be excluded

    std::vector<std::string> expected = {"alice", "carol"};
    ASSERT_EQ(scanned_keys, expected);

    db_->EndTransaction(tx, [](auto) {});
  }
}

// 同一トランザクション内で削除されたキーがScanSecondaryIndexから除外されることを確認
// （Read Your Own Writes - Delete版）
TEST_F(ScanSecondaryIndexTest, ScanShouldExcludeReadYourWriteDeletedKeys) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "name_index", 0);

  std::string pk1 = "user1";
  std::string pk2 = "user2";
  std::string pk3 = "user3";

  // 最初のトランザクション: キーを挿入
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    tx.Write("user1", "Alice");
    tx.Write("user2", "Bob");
    tx.Write("user3", "Carol");
    tx.WriteSecondaryIndex("name_index", "alice",
                           reinterpret_cast<const std::byte*>(pk1.data()),
                           pk1.size());
    tx.WriteSecondaryIndex("name_index", "bob",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());
    tx.WriteSecondaryIndex("name_index", "carol",
                           reinterpret_cast<const std::byte*>(pk3.data()),
                           pk3.size());
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  // 2番目のトランザクション: 同一トランザクション内でキーを削除してScan
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    tx.DeleteSecondaryIndex("name_index", "bob",
                            reinterpret_cast<const std::byte*>(pk2.data()),
                            pk2.size());

    std::vector<std::string> scanned_keys;

    auto count = tx.ScanSecondaryIndex(
        "name_index", "alice", "carol", [&](auto key, auto primary_keys) {
          scanned_keys.push_back(std::string(key));
          return false;
        });

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), 2);  // bob should be excluded

    std::vector<std::string> expected = {"alice", "carol"};
    ASSERT_EQ(scanned_keys, expected);

    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
  }
}

// 同一トランザクション内で挿入してすぐ削除した場合、ScanSecondaryIndexから除外されることを確認
TEST_F(ScanSecondaryIndexTest, ScanShouldExcludeInsertThenDeletedKeys) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "name_index", 0);

  std::string pk1 = "user1";
  std::string pk2 = "user2";

  // 最初のトランザクション: 既存のキーを挿入
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");
    tx.Write("user1", "Alice");
    tx.WriteSecondaryIndex("name_index", "alice",
                           reinterpret_cast<const std::byte*>(pk1.data()),
                           pk1.size());
    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
    db_->Fence();
  }

  // 2番目のトランザクション: 挿入してすぐ削除
  {
    auto& tx = db_->BeginTransaction();
    tx.SetTable("users");

    // bobを挿入
    tx.Write("user2", "Bob");
    tx.WriteSecondaryIndex("name_index", "bob",
                           reinterpret_cast<const std::byte*>(pk2.data()),
                           pk2.size());

    // すぐにbobを削除
    tx.DeleteSecondaryIndex("name_index", "bob",
                            reinterpret_cast<const std::byte*>(pk2.data()),
                            pk2.size());

    std::vector<std::string> scanned_keys;

    auto count = tx.ScanSecondaryIndex(
        "name_index", "alice", "carol", [&](auto key, auto primary_keys) {
          scanned_keys.push_back(std::string(key));
          return false;
        });

    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), 1);  // aliceのみ

    std::vector<std::string> expected = {"alice"};
    ASSERT_EQ(scanned_keys, expected);

    const bool committed = db_->EndTransaction(tx, [](auto status) {
      ASSERT_EQ(status, LineairDB::TxStatus::Committed);
    });
    ASSERT_TRUE(committed);
  }
}
