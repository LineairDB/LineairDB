/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.

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

#ifndef LINEAIRDB_TEST_HELPER_HPP
#define LINEAIRDB_TEST_HELPER_HPP

#include <lineairdb/lineairdb.h>

#include <atomic>
#include <cstddef>
#include <functional>
#include <future>
#include <thread>
#include <vector>

using TransactionProcedure = std::function<void(LineairDB::Transaction&)>;

namespace TestHelper {
bool DoTransactions(LineairDB::Database* db,
                    const std::vector<TransactionProcedure> txns) {
  std::atomic<size_t> terminated(0);
  for (auto& tx : txns) {
    db->ExecuteTransaction(tx, [&](const auto) { terminated++; });
    db->Fence();
  }

  size_t msec_elapsed_for_termination = 0;
  while (terminated != txns.size()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    msec_elapsed_for_termination++;
    bool too_long_time_elapsed = (db->GetConfig().epoch_duration_ms * 1000) <
                                 msec_elapsed_for_termination;
    if (too_long_time_elapsed) return false;
  }
  return true;
}

size_t DoTransactionsOnMultiThreads(
    LineairDB::Database* db, const std::vector<TransactionProcedure> txns) {
  std::atomic<size_t> terminated(0);
  std::atomic<size_t> committed(0);
  std::vector<std::future<void>> jobs;
  std::atomic<size_t> waits(0);
  std::atomic<bool> barrier(false);
  for (auto& tx : txns) {
    jobs.push_back(std::async(std::launch::async, [&]() {
      waits.fetch_add(1);
      for (;;) {
        if (barrier.load()) break;
      }
      db->ExecuteTransaction(tx, [&](const auto status) {
        terminated++;
        if (status == LineairDB::TxStatus::Committed) { committed++; }
      });
    }));
  }
  for (;;) {
    if (waits.load() == txns.size()) break;
  }
  barrier.store(true);

  for (auto& job : jobs) { job.wait(); }

  size_t msec_elapsed_for_termination = 0;
  while (terminated != txns.size()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    msec_elapsed_for_termination++;
    bool too_long_time_elapsed = (db->GetConfig().epoch_duration_ms * 1000) <
                                 msec_elapsed_for_termination;
    if (too_long_time_elapsed) return 0;
  }

  return committed;
}

size_t DoHandlerTransactionsOnMultiThreads(
    LineairDB::Database* db, const std::vector<TransactionProcedure> txns) {
  std::atomic<size_t> terminated(0);
  std::atomic<size_t> committed(0);

  std::vector<std::future<void>> jobs;
  std::atomic<bool> barrier(false);
  std::atomic<size_t> waits(0);

  for (auto& proc : txns) {
    jobs.push_back(std::async(std::launch::async, [&]() {
      waits.fetch_add(1);
      for (;;) {
        if (barrier.load()) break;
      }
      auto& tx = db->BeginTransaction();
      proc(tx);
      db->EndTransaction(tx, [&](auto status) {
        if (status == LineairDB::TxStatus::Committed) {
          committed++;
          terminated++;
        }
      });
    }));
  }
  for (;;) {
    if (waits.load() == txns.size()) break;
  }
  barrier.store(true);

  for (auto& job : jobs) { job.wait(); }

  size_t msec_elapsed_for_termination = 0;
  while (terminated != txns.size()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    msec_elapsed_for_termination++;
    bool too_long_time_elapsed = (db->GetConfig().epoch_duration_ms * 1000) <
                                 msec_elapsed_for_termination;
    if (too_long_time_elapsed) return 0;
  }

  return committed;
}  // namespace TestHelper

}  // namespace TestHelper
#endif /* LINEAIRDB_TEST_HELPER_HPP */
