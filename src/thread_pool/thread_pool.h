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

#ifndef LINEAIRDB_THREADPOOL_H
#define LINEAIRDB_THREADPOOL_H

#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "concurrentqueue.h"  // moodycamel::concurrentqueue

namespace LineairDB {

/**
 * @brief
 * MPMC (Multiple producer / multiple consumer) thread pool.
 */
class ThreadPool {
 public:
  ThreadPool(size_t pool_size = std::thread::hardware_concurrency());
  ~ThreadPool();
  bool Enqueue(std::function<void()>&&);
  bool EnqueueForAllThreads(std::function<void()>&&);
  void StopAcceptingTransactions();
  void ResumeAcceptingTransactions();
  void Shutdown();
  void JoinAll();
  void WaitForQueuesToBecomeEmpty();
  bool IsEmpty();
  size_t GetPoolSize() const;

 private:
  size_t GetIdxByThreadId();
  void Dequeue();

 private:
  bool stop_;
  bool shutdown_;
  std::vector<moodycamel::ConcurrentQueue<std::function<void()>>> work_queues_;
  std::vector<moodycamel::ConcurrentQueue<std::function<void()>>>
      no_steal_queues_;
  std::vector<std::thread> worker_threads_;
  std::vector<std::thread::id> thread_ids_;
  std::mutex thread_ids_lock_;
};
}  // namespace LineairDB
#endif
