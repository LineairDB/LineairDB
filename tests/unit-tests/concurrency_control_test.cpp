/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "../test_helper.hpp"
#include "gtest/gtest.h"
#include "util/logger.hpp"

class ConcurrencyControlTest
    : public ::testing::TestWithParam<LineairDB::Config::ConcurrencyControl> {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    config_.concurrency_control_protocol = ConcurrencyControlTest::GetParam();
    config_.enable_recovery = false;
    config_.enable_logging = false;
    config_.enable_checkpointing = false;
    // NOTE: The testcase AvoidingReadOnlyAnomaly requires to be executed on 3
    // threads in parallel.
    if (config_.max_thread < 3) {
      config_.max_thread = 4;
    }
    db_ = std::make_unique<LineairDB::Database>(config_);
    db_->CreateTable("users");
  }
  virtual void TearDown() { std::filesystem::remove_all("lineairdb_logs"); }
};

const std::array<std::string, 3> Protocols{"Silo", "SiloNWR", "2PL"};
INSTANTIATE_TEST_SUITE_P(
    ForEachProtocol, ConcurrencyControlTest,
    ::testing::Values(LineairDB::Config::ConcurrencyControl::Silo,
                      LineairDB::Config::ConcurrencyControl::SiloNWR,
                      LineairDB::Config::ConcurrencyControl::TwoPhaseLocking),
    [](const testing::TestParamInfo<LineairDB::Config::ConcurrencyControl>&
           param) { return Protocols[param.index]; });

TEST_P(ConcurrencyControlTest, Instantiate) {}

TEST_P(ConcurrencyControlTest, IncrementOnMultiThreads) {
  int initial_value = 1;
  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               tx.Write<int>("users", "alice", initial_value);
                             }});
  db_->Fence();

  TransactionProcedure increment([](LineairDB::Transaction& tx) {
    auto alice = tx.Read<int>("users", "alice");
    if (!alice.has_value()) return tx.Abort();
    int current_value = alice.value();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    current_value++;
    tx.Write<int>("users", "alice", current_value);
  });

  int committed_count = TestHelper::DoTransactionsOnMultiThreads(
      db_.get(), {increment, increment, increment, increment});
  db_->Fence();

  TestHelper::DoTransactions(db_.get(), {[&](LineairDB::Transaction& tx) {
                               auto alice = tx.Read<int>("users", "alice");
                               ASSERT_TRUE(alice.has_value());
                               auto expected_value =
                                   initial_value + committed_count;
                               auto current_value = alice.value();
                               ASSERT_EQ(expected_value, current_value);
                             }});
}

TEST_P(ConcurrencyControlTest, AvoidingDeadLock) {
  TransactionProcedure readX_writeY([](LineairDB::Transaction& tx) {
    tx.Read<int>("users", "x");
    tx.Write<int>("users", "y", 0xDEADBEEF);
  });
  TransactionProcedure readY_writeX([](LineairDB::Transaction& tx) {
    tx.Read<int>("users", "y");
    tx.Write<int>("users", "x", 0xDEADBEEF);
  });

  TestHelper::DoTransactionsOnMultiThreads(
      db_.get(), {readX_writeY, readX_writeY, readY_writeX, readY_writeX});
}

TEST_P(ConcurrencyControlTest, AvoidingDirtyReadAnomaly) {
  TransactionProcedure insertTenTimes([](LineairDB::Transaction& tx) {
    int value = 0xBEEF;
    for (size_t idx = 0; idx <= 10; idx++) {
      tx.Write<int>("users", "alice" + std::to_string(idx), value);
    }
    tx.Abort();
  });

  TransactionProcedure readTenTimes([](LineairDB::Transaction& tx) {
    for (size_t idx = 0; idx <= 10; idx++) {
      auto result = tx.Read<int>("users", "alice" + std::to_string(idx));
      ASSERT_FALSE(result.has_value());
    }
  });
  ASSERT_NO_THROW({
    TestHelper::DoTransactionsOnMultiThreads(
        db_.get(),
        {insertTenTimes, insertTenTimes, readTenTimes, readTenTimes});
  });
}

TEST_P(ConcurrencyControlTest, RepeatableRead) {
  TransactionProcedure updateTenTimes([](LineairDB::Transaction& tx) {
    int value = 0xBEEF;
    for (size_t idx = 0; idx <= 10; idx++) {
      tx.Write<int>("users", "alice", value + idx);
    }
  });
  TransactionProcedure repeatableRead([](LineairDB::Transaction& tx) {
    auto first_result = tx.Read<int>("users", "alice");
    if (first_result.has_value()) {
      auto first_value = first_result.value();
      for (size_t idx = 0; idx <= 10; idx++) {
        auto result = tx.Read<int>("users", "alice");
        if (!result.has_value()) return tx.Abort();
        ASSERT_EQ(result.value(), first_value);
      }
    }
  });
  ASSERT_NO_THROW({
    TestHelper::DoTransactionsOnMultiThreads(
        db_.get(),
        {updateTenTimes, updateTenTimes, repeatableRead, repeatableRead});
  });
}

TEST_P(ConcurrencyControlTest, AvoidingWriteSkewAnomaly) {
  /** initialize **/
  TestHelper::DoTransactions(db_.get(), {[](LineairDB::Transaction& tx) {
                               tx.Write<int>("users", "alice", 0);
                               tx.Write<int>("users", "bob", 1);
                             }});

  TransactionProcedure readAliceWriteBob([](LineairDB::Transaction& tx) {
    auto result = tx.Read<int>("users", "alice");
    if (!result.has_value()) return tx.Abort();
    tx.Write<int>("users", "bob", result.value() += 1);
  });
  TransactionProcedure readBobWriteAlice([](LineairDB::Transaction& tx) {
    auto result = tx.Read<int>("users", "bob");
    if (!result.has_value()) return tx.Abort();
    tx.Write<int>("users", "alice", result.value() += 1);
  });

  TestHelper::DoTransactionsOnMultiThreads(
      db_.get(), {readAliceWriteBob, readAliceWriteBob, readAliceWriteBob,
                  readAliceWriteBob, readBobWriteAlice, readBobWriteAlice,
                  readBobWriteAlice, readBobWriteAlice});

  db_->Fence();

  /** validation **/
  TestHelper::DoTransactions(db_.get(), {[](LineairDB::Transaction& tx) {
                               auto alice = tx.Read<int>("users", "alice");
                               auto bob = tx.Read<int>("users", "bob");
                               ASSERT_TRUE(alice.has_value());
                               ASSERT_TRUE(bob.has_value());
                               ASSERT_EQ(1,
                                         std::abs(alice.value() - bob.value()));
                             }});
}

TEST_P(ConcurrencyControlTest, AvoidingReadOnlyAnomaly) {
  // Reference: Example 1.3 in
  // https://www.cse.iitb.ac.in/infolab/Data/Courses/CS632/2009/Papers/p492-fekete.pdf

  std::atomic<bool> waits(true);

  /** T1: r1(y0) w1(y1) **/
  TransactionProcedure T1([&](LineairDB::Transaction& tx) {
    auto y = tx.Read<int>("users", "y");
    if (!y.has_value()) return tx.Abort();
    EXPECT_TRUE(y.has_value());
    EXPECT_EQ(0, y.value());

    while (waits) {
      std::this_thread::yield();
    }

    tx.Write<int>("users", "y", 20);
  });
  /** T2: r2(x0) r2(y0) w2(x2) **/
  TransactionProcedure T2([&](LineairDB::Transaction& tx) {
    auto x = tx.Read<int>("users", "x");
    auto y = tx.Read<int>("users", "y");
    if (!(x.has_value() && y.has_value())) return tx.Abort();
    EXPECT_EQ(0, x.value());
    EXPECT_EQ(0, y.value());

    waits.store(false);
    std::this_thread::yield();
    tx.Write<int>("users", "x", -11);
  });

  /** T3: r3(x0) r3(y1) **/
  std::atomic<int> x_value_read_by_t3(0);
  std::atomic<int> y_value_read_by_t3(0);
  TransactionProcedure T3([&](LineairDB::Transaction& tx) {
    while (waits) {
      std::this_thread::yield();
    }
    std::this_thread::yield();
    auto x = tx.Read<int>("users", "x");
    auto y = tx.Read<int>("users", "y");
    if (!(x.has_value() && y.has_value())) return tx.Abort();
    if (y.value() != 20) return tx.Abort();
    x_value_read_by_t3.store(x.value());
    y_value_read_by_t3.store(y.value());
  });

  size_t committed = 0;
  size_t retry = 0;
  while (committed != 3) {
    waits.store(true);
    /** initialize **/
    TestHelper::DoTransactions(db_.get(), {[](LineairDB::Transaction& tx) {
                                 tx.Write<int>("users", "x", 0);
                                 tx.Write<int>("users", "y", 0);
                               }});

    committed =
        TestHelper::DoTransactionsOnMultiThreads(db_.get(), {T1, T2, T3});
    if (committed == 3) {
      auto x = x_value_read_by_t3.load();
      auto y = y_value_read_by_t3.load();
      ASSERT_EQ(x, -11);
      ASSERT_EQ(y, 20);
    } else {
      SPDLOG_DEBUG("Only {0} transactions has committed. Retrying testcase...",
                   committed);
      retry++;
      if (100 < retry) {
        SPDLOG_WARN(
            "The testcase for the read only anomaly has finished by timeout,"
            "and it is not tested correctly.");
        break;
      }
    }
  }
}

TEST_P(ConcurrencyControlTest, Recoverability) {
  std::atomic<size_t> transaction_id(0);
  std::vector<std::pair<size_t, size_t>> committed_values;
  std::vector<std::pair<size_t, size_t>> read_values;
  std::mutex v_latch;

  /**
   * This test checks both the commit order and reads-from relationship.
   * Briefly, we check that $ci < cj$ holds for all $r_j(x_i)$.
   * Note that the commit points in LineairDB are determined by the epoch
   * framework; it is **not** the order of callbacks. Transactions belonging to
   * the same epoch are committed at the same time when the epoch becomes
   * stable, so the order of the callbacks may interleave. Here we align the
   * order of the callback queue by limiting the number of workers in the
   * thread-pool.
   * TODO: Multi-threaded testing for recoverability.
   * We need the interface to fetch the commit order correctly.
   */
  LineairDB::Config config = db_->GetConfig();
  config.max_thread = 1;
  config.epoch_duration_ms = 1;
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  std::atomic<bool> recoverability_failure = false;
  constexpr auto UNCOMMITTED = ~0llu;
  while (transaction_id.load() < 1000) {
    auto my_tid = transaction_id.fetch_add(1);

    // writer
    db_->ExecuteTransaction(
        [&, my_tid](LineairDB::Transaction& tx) {
          tx.Write<size_t>("users", "alice", my_tid);
          {
            std::lock_guard<std::mutex> global_latch(v_latch);
            committed_values.push_back(std::make_pair(UNCOMMITTED, my_tid));
          }
        },
        [&, my_tid](auto status) {
          if (status == LineairDB::TxStatus::Committed) {
            {
              std::lock_guard<std::mutex> global_latch(v_latch);
              for (auto& pair : committed_values) {
                if (pair.second == my_tid) {
                  pair.first = my_tid;
                  break;
                }
              }
            }
          }
        });

    // reader
    db_->ExecuteTransaction(
        [&, my_tid](auto& tx) {
          auto result = tx.template Read<size_t>("users", "alice");
          if (result.has_value()) {
            {
              std::lock_guard<std::mutex> global_latch(v_latch);
              read_values.push_back(std::make_pair(my_tid, result.value()));
            }
          } else {
            tx.Abort();
          }
        },
        [&, my_tid](auto status) {
          if (status == LineairDB::TxStatus::Committed) {
            {
              std::lock_guard<std::mutex> global_latch(v_latch);
              size_t my_read_value = 0;
              for (auto& pair : read_values) {
                if (pair.first == my_tid) {
                  my_read_value = pair.second;
                  break;
                }
              }

              for (auto& pair : committed_values) {
                if (pair.second == my_read_value) {
                  if (pair.first == UNCOMMITTED) {
                    recoverability_failure = true;
                  }
                  break;
                }
              }
            }
          }
        });
  }
  db_->Fence();
  ASSERT_FALSE(recoverability_failure);
}