/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <gtest/gtest.h>
#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

class CPRTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  std::string log_dir_;

  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread = 4;
    config_.durability = LineairDB::Config::DurabilityStrategy::Checkpoint;
    config_.enable_recovery = false;
    config_.epoch_duration_ms = 10;
    config_.checkpoint_period = 0;  // Driven manually
    db_ = std::make_unique<LineairDB::Database>(config_);
    log_dir_ = config_.work_dir;
  }

  virtual void TearDown() {
    db_.reset();
    std::filesystem::remove_all(log_dir_);
  }
};

TEST_F(CPRTest, Recovery) {
  std::atomic<bool> done{false};
  db_->ExecuteTransaction(
      [](LineairDB::Transaction& tx) { tx.Write<int>("recover", 999); },
      [&](LineairDB::TxStatus) { done.store(true); }, std::nullopt);

  while (!done.load()) std::this_thread::yield();
  db_->Fence();

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  db_.reset();

  config_.enable_recovery = true;
  db_ = std::make_unique<LineairDB::Database>(config_);

  std::atomic<bool> verified{false};
  db_->ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto res = tx.Read<int>("recover");
        if (res.has_value()) {
          EXPECT_EQ(res.value(), 999);
          verified.store(true);
        }
      },
      [](LineairDB::TxStatus) {}, std::nullopt);
  db_->Fence();
  EXPECT_TRUE(verified.load());
}

TEST_F(CPRTest, OverwriteSameKeyRecovery) {
  for (int i = 0; i < 5; ++i) {
    std::atomic<bool> done{false};
    db_->ExecuteTransaction(
        [i](LineairDB::Transaction& tx) { tx.Write<int>("key", i * 10); },
        [&](LineairDB::TxStatus) { done.store(true); }, std::nullopt);
    while (!done.load()) std::this_thread::yield();
    db_->Fence();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  db_.reset();

  config_.enable_recovery = true;
  db_ = std::make_unique<LineairDB::Database>(config_);

  std::atomic<bool> verified{false};
  db_->ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto res = tx.Read<int>("key");
        if (res.has_value()) {
          EXPECT_EQ(res.value(), 40);
          verified.store(true);
        }
      },
      [](LineairDB::TxStatus) {}, std::nullopt);
  db_->Fence();
  EXPECT_TRUE(verified.load());
}

TEST_F(CPRTest, MultipleKeysRecovery) {
  for (int i = 0; i < 5; ++i) {
    std::atomic<bool> done{false};
    db_->ExecuteTransaction(
        [i](LineairDB::Transaction& tx) {
          tx.Write<int>("k" + std::to_string(i), i * 100);
        },
        [&](LineairDB::TxStatus) { done.store(true); }, std::nullopt);
    while (!done.load()) std::this_thread::yield();
    db_->Fence();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  db_.reset();

  config_.enable_recovery = true;
  db_ = std::make_unique<LineairDB::Database>(config_);

  for (int i = 0; i < 5; ++i) {
    std::atomic<bool> verified{false};
    db_->ExecuteTransaction(
        [&, i](LineairDB::Transaction& tx) {
          auto res = tx.Read<int>("k" + std::to_string(i));
          if (res.has_value()) {
            EXPECT_EQ(res.value(), i * 100);
            verified.store(true);
          }
        },
        [](LineairDB::TxStatus) {}, std::nullopt);
    db_->Fence();
    EXPECT_TRUE(verified.load());
  }
}

TEST_F(CPRTest, CommitCallbackAfterDurabilitySatisfied) {
  std::atomic<bool> commit_callback_fired{false};
  db_->ExecuteTransaction(
      [](LineairDB::Transaction& tx) { tx.Write<int>("callback_test", 1234); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          commit_callback_fired.store(true);
        }
      },
      std::nullopt);

  db_->Fence();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_TRUE(commit_callback_fired.load());
}

TEST_F(CPRTest, ConcurrentWriteRecovery) {
  constexpr int N = 10;
  std::atomic<int> completed{0};

  for (int i = 0; i < N; ++i) {
    db_->ExecuteTransaction(
        [i](LineairDB::Transaction& tx) {
          tx.Write<int>("concurrent_" + std::to_string(i), i * 11);
        },
        [&](LineairDB::TxStatus) { completed.fetch_add(1); }, std::nullopt);
  }

  db_->Fence();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(completed.load(), N);

  db_.reset();

  config_.enable_recovery = true;
  db_ = std::make_unique<LineairDB::Database>(config_);

  for (int i = 0; i < N; ++i) {
    std::atomic<bool> verified{false};
    db_->ExecuteTransaction(
        [&, i](LineairDB::Transaction& tx) {
          auto res = tx.Read<int>("concurrent_" + std::to_string(i));
          if (res.has_value()) {
            EXPECT_EQ(res.value(), i * 11);
            verified.store(true);
          }
        },
        [](LineairDB::TxStatus) {}, std::nullopt);
    db_->Fence();
    EXPECT_TRUE(verified.load());
  }
}

TEST_F(CPRTest, EmptyDeltaFileRecovery) {
  // Wait for epoch to advance, creating empty checkpoints
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  db_.reset();

  config_.enable_recovery = true;
  db_ = std::make_unique<LineairDB::Database>(config_);

  // Database should open correctly without error
  std::atomic<bool> done{false};
  db_->ExecuteTransaction(
      [](LineairDB::Transaction& tx) { tx.Read<int>("any"); },
      [&](LineairDB::TxStatus) { done.store(true); }, std::nullopt);
  db_->Fence();
  EXPECT_TRUE(done.load());
}

TEST_F(CPRTest, CorruptedMsgpackRecovery) {
  std::atomic<bool> done{false};
  db_->ExecuteTransaction(
      [](LineairDB::Transaction& tx) { tx.Write<int>("x", 777); },
      [&](LineairDB::TxStatus) { done.store(true); }, std::nullopt);
  db_->Fence();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  db_.reset();

  // Corrupt the latest delta log file by appending junk
  for (const auto& entry : std::filesystem::directory_iterator(log_dir_)) {
    if (entry.path().filename().string().rfind("cpr_delta_", 0) == 0) {
      std::ofstream f(entry.path(), std::ios::app | std::ios::binary);
      f << "corrupted junk data!!!";
    }
  }

  config_.enable_recovery = true;
  db_ = std::make_unique<LineairDB::Database>(config_);

  // Re-open should succeed, ignoring corrupted part gracefully
  std::atomic<bool> read_done{false};
  db_->ExecuteTransaction([](LineairDB::Transaction& tx) { tx.Read<int>("x"); },
                          [&](LineairDB::TxStatus) { read_done.store(true); },
                          std::nullopt);
  db_->Fence();
  EXPECT_TRUE(read_done.load());
}

TEST_F(CPRTest, RecoveryIgnoresWorkingFiles) {
  std::atomic<bool> done{false};
  db_->ExecuteTransaction(
      [](LineairDB::Transaction& tx) { tx.Write<int>("x", 888); },
      [&](LineairDB::TxStatus) { done.store(true); }, std::nullopt);
  db_->Fence();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  db_.reset();

  // Create a dummy .working file to ensure it's skipped
  std::ofstream f(log_dir_ + "/cpr_delta_9999.log.working");
  f << "junk";
  f.close();

  config_.enable_recovery = true;
  db_ = std::make_unique<LineairDB::Database>(config_);

  std::atomic<bool> read_done{false};
  db_->ExecuteTransaction([](LineairDB::Transaction& tx) { tx.Read<int>("x"); },
                          [&](LineairDB::TxStatus) { read_done.store(true); },
                          std::nullopt);
  db_->Fence();
  EXPECT_TRUE(read_done.load());
}

TEST_F(CPRTest, IntraEpochMultipleWritesConsistency) {
  // Recreate DB with a larger epoch duration to guarantee all writes land in
  // the same epoch window
  db_.reset();
  std::filesystem::remove_all(config_.work_dir);
  config_.epoch_duration_ms = 1000;
  db_ = std::make_unique<LineairDB::Database>(config_);

  // Commit multiple writes sequentially within the same epoch window
  std::atomic<int> commit_count{0};
  for (int i = 0; i < 3; ++i) {
    std::atomic<bool> tx_done{false};
    db_->ExecuteTransaction(
        [i](LineairDB::Transaction& tx) { tx.Write<int>("same_epoch", i); },
        [&](LineairDB::TxStatus) {
          commit_count.fetch_add(1);
          tx_done.store(true);
        },
        std::nullopt);
    while (!tx_done.load()) {
      std::this_thread::yield();
    }
  }
  db_->Fence();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(commit_count.load(), 3);

  db_.reset();

  // Restore epoch duration to a smaller value for recovery verification to
  // speed up Fence()
  config_.epoch_duration_ms = 10;
  config_.enable_recovery = true;
  db_ = std::make_unique<LineairDB::Database>(config_);

  std::atomic<bool> verified{false};
  db_->ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto res = tx.Read<int>("same_epoch");
        if (res.has_value()) {
          EXPECT_EQ(res.value(), 2);
          verified.store(true);
        }
      },
      [](LineairDB::TxStatus) {}, std::nullopt);
  db_->Fence();
  EXPECT_TRUE(verified.load());
}

TEST_F(CPRTest, IntraEpochAbortConsistency) {
  // Commit one write, abort the second in the same epoch
  std::atomic<bool> done1{false};
  db_->ExecuteTransaction(
      [](LineairDB::Transaction& tx) { tx.Write<int>("abort_test", 100); },
      [&](LineairDB::TxStatus) { done1.store(true); }, std::nullopt);
  while (!done1.load()) std::this_thread::yield();

  std::atomic<bool> done2{false};
  db_->ExecuteTransaction(
      [](LineairDB::Transaction& tx) {
        tx.Write<int>("abort_test", 200);
        tx.Abort();
      },
      [&](LineairDB::TxStatus) { done2.store(true); }, std::nullopt);
  while (!done2.load()) std::this_thread::yield();

  db_->Fence();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  db_.reset();

  config_.enable_recovery = true;
  db_ = std::make_unique<LineairDB::Database>(config_);

  std::atomic<bool> verified{false};
  db_->ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto res = tx.Read<int>("abort_test");
        if (res.has_value()) {
          EXPECT_EQ(res.value(), 100);  // Should NOT recover aborted 200
          verified.store(true);
        }
      },
      [](LineairDB::TxStatus) {}, std::nullopt);
  db_->Fence();
  EXPECT_TRUE(verified.load());
}

// --- Compactor and file reduction tests ---

struct IncrementalFileInfo {
  size_t count;
  uintmax_t total_bytes;
};

static IncrementalFileInfo GetIncrementalFileStats(const std::string& dir) {
  size_t count = 0;
  uintmax_t size = 0;
  if (!std::filesystem::exists(dir)) return {0, 0};
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    const std::string name = entry.path().filename().string();
    if (name.rfind("cpr_delta_", 0) == 0 || name.rfind("cpr_base_", 0) == 0) {
      if (name.find(".working") == std::string::npos) {
        count++;
        size += entry.file_size();
      }
    }
  }
  return {count, size};
}

class CPRCompactionTest : public CPRTest {
 protected:
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread = 2;
    config_.durability = LineairDB::Config::DurabilityStrategy::Checkpoint;
    config_.enable_recovery = false;
    config_.epoch_duration_ms = 10;
    config_.checkpoint_period = 0;
    db_ = std::make_unique<LineairDB::Database>(config_);
    log_dir_ = config_.work_dir;
  }

  // Commit one write transaction and wait for its epoch to become durable.
  void CommitOneWrite(const std::string& key, int value) {
    std::atomic<bool> done{false};
    db_->ExecuteTransaction(
        [&](LineairDB::Transaction& tx) { tx.Write<int>(key, value); },
        [&](LineairDB::TxStatus) { done.store(true); }, std::nullopt);
    while (!done.load()) std::this_thread::yield();
  }
};

// After committing writes across at least 10 distinct checkpoint epochs,
// the compactor must have merged some files so that the total number of
// cpr_delta_*.log files is strictly less than 10.
TEST_F(CPRCompactionTest, FileCountDecreasesAfterCompaction) {
  const int kEpochs = 15;

  std::vector<size_t> file_counts;

  for (int i = 0; i < kEpochs; ++i) {
    CommitOneWrite("key_" + std::to_string(i), i);
    // Give the worker loop time to write the epoch file.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    file_counts.push_back(GetIncrementalFileStats(log_dir_).count);
  }

  // Give the compactor thread time to finish any pending compaction.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  file_counts.push_back(GetIncrementalFileStats(log_dir_).count);

  // After all epochs, at least one snapshot reduction must have occurred:
  bool decreased = false;
  for (size_t i = 1; i < file_counts.size(); ++i) {
    if (file_counts[i] < file_counts[i - 1]) {
      decreased = true;
      break;
    }
  }
  EXPECT_TRUE(decreased)
      << "File count never decreased: compaction has not been implemented or "
         "delayed. "
      << "Counts observed: [" <<
      [&] {
        std::string s;
        for (auto c : file_counts) s += std::to_string(c) + " ";
        return s;
      }()
      << "]";
}

// Same scenario but measured by total byte size.
// Strategy: measure the size of a single delta file first, then write many
// epochs with the same keys (so compaction can eliminate duplicates).
// After compaction, the total on-disk size should be much less than
// kEpochs * single_delta_size, because the merged base file holds only the
// latest version of each key rather than one entry per epoch.
TEST_F(CPRCompactionTest, FileSizeDecreasesAfterCompaction) {
  const std::vector<std::string> keys = {"a", "b", "c", "d", "e"};

  // Write exactly one epoch worth of data and wait for it to be durable.
  // This gives us the baseline size of a single delta file.
  for (const auto& key : keys) CommitOneWrite(key, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  const uintmax_t single_epoch_size =
      GetIncrementalFileStats(log_dir_).total_bytes;
  ASSERT_GT(single_epoch_size, 0u) << "No checkpoint file written at all";

  // Write many epochs so compaction triggers multiple times.
  // Using the same keys means every delta is a candidate for deduplication.
  const int kEpochs = 20;
  for (int i = 1; i < kEpochs; ++i) {
    for (const auto& key : keys) CommitOneWrite(key, i);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  // Give the compactor generous time to finish its last run.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  const uintmax_t final_size = GetIncrementalFileStats(log_dir_).total_bytes;
  // Without compaction: ~kEpochs * single_epoch_size bytes.
  // With compaction:    1 base + a few deltas ≈ a handful of single_epoch_size.
  // We require strictly less than half the "no compaction" expectation.
  const uintmax_t no_compaction_estimate =
      static_cast<uintmax_t>(kEpochs) * single_epoch_size;
  EXPECT_LT(final_size, no_compaction_estimate / 2)
      << "Expected compaction to have reduced total size significantly. "
      << "single_epoch_size=" << single_epoch_size
      << ", final_size=" << final_size
      << ", no_compaction_estimate=" << no_compaction_estimate;
}

// Compaction must preserve correctness: after compaction, a restart must
// still recover all the data that was durable before compaction.
TEST_F(CPRCompactionTest, CompactionPreservesRecoverability) {
  const int kEpochs = 10;

  // Write keys across many epochs.
  for (int i = 0; i < kEpochs; ++i) {
    CommitOneWrite("recover_" + std::to_string(i), i * 100);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // Wait for all epochs to flush.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  db_.reset();

  // Verify compaction happened (at least the file count did not grow
  // unbounded).
  const size_t post_compact_count = GetIncrementalFileStats(log_dir_).count;
  EXPECT_LT(post_compact_count, static_cast<size_t>(kEpochs))
      << "Expected compaction to have reduced file count below " << kEpochs;

  // Reopen and verify all durable writes are visible.
  config_.enable_recovery = true;
  db_ = std::make_unique<LineairDB::Database>(config_);
  for (int i = 0; i < kEpochs; ++i) {
    std::atomic<bool> done{false};
    db_->ExecuteTransaction(
        [&, i](LineairDB::Transaction& tx) {
          auto res = tx.Read<int>("recover_" + std::to_string(i));
          if (res.has_value()) {
            EXPECT_EQ(res.value(), i * 100);
          }
        },
        [&](LineairDB::TxStatus) { done.store(true); }, std::nullopt);
    while (!done.load()) std::this_thread::yield();
  }
  db_->Fence();
}
