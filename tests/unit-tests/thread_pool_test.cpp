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

#include "thread_pool/thread_pool.h"

#include <mutex>
#include <set>
#include <thread>

#include "gtest/gtest.h"

void Blocking(std::atomic<size_t>& num_of_running_txns) {
  size_t msec_elapsed_for_termination = 0;
  while (num_of_running_txns.load() != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    msec_elapsed_for_termination++;
    bool too_long_time_elapsed = 100 < msec_elapsed_for_termination;
    EXPECT_FALSE(too_long_time_elapsed);
    if (too_long_time_elapsed)
      break;
  }
}

TEST(ThreadPoolTest, Instantiate) { ASSERT_NO_THROW(LineairDB::ThreadPool thread_pool); }

TEST(ThreadPoolTest, Enqueue) {
  LineairDB::ThreadPool thread_pool;
  std::atomic<size_t> num_of_running_txns(1);
  thread_pool.Enqueue([&]() { num_of_running_txns--; });
  Blocking(num_of_running_txns);
}

TEST(ThreadPoolTest, StopAcceptingTransactions) {
  LineairDB::ThreadPool thread_pool;

  thread_pool.StopAcceptingTransactions();

  ASSERT_FALSE(
      thread_pool.Enqueue([&]() { ASSERT_FALSE("Expected not to invoke this assertion"); }));
}

TEST(ThreadPoolTest, ResumeAcceptingTransactions) {
  LineairDB::ThreadPool thread_pool;

  thread_pool.StopAcceptingTransactions();

  ASSERT_FALSE(
      thread_pool.Enqueue([&]() { ASSERT_FALSE("Expected not to invoke this assertion"); }));

  thread_pool.ResumeAcceptingTransactions();
  ASSERT_TRUE(thread_pool.Enqueue([]() {}));
}

TEST(ThreadPoolTest, UseMultipleThreads) {
  LineairDB::ThreadPool thread_pool(10);
  std::atomic<size_t> num_of_running_txns(10);

  for (size_t i = 0; i < 10; i++) {
    ASSERT_TRUE(thread_pool.Enqueue([&]() { num_of_running_txns--; }));
  }
  Blocking(num_of_running_txns);
}

TEST(ThreadPoolTest, EnqueueForAllThreads) {
  LineairDB::ThreadPool thread_pool(10);
  std::atomic<size_t> num_of_running_txns(10);

  thread_pool.EnqueueForAllThreads([&]() {
    num_of_running_txns--;
    thread_local bool im_already_executed = false;
    ASSERT_FALSE(im_already_executed);
    if (im_already_executed == false) {
      im_already_executed = true;
    }
  });

  Blocking(num_of_running_txns);
}
