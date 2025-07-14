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

#include "thread_pool.h"

#include <concurrentqueue.h>  // moodycamel::concurrentqueue

#ifndef __APPLE__
#include <numa.h>
#include <unistd.h>

#if __GLIBC__ == 2 && __GLIBC_MINOR__ < 30
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)
#endif

#endif

#include <atomic>
#include <functional>
#include <mutex>
#include <queue>
#include <random>
#include <thread>
#include <vector>

namespace LineairDB {
ThreadPool::ThreadPool(size_t pool_size)
    : stop_(false),
      shutdown_(false),
      work_queues_(pool_size),
      no_steal_queues_(pool_size) {
  assert(work_queues_.size() == pool_size);
  for (size_t i = 0; i < pool_size; i++) {
#ifndef __APPLE__
    worker_threads_.emplace_back([&, i]() {
      const auto pid = gettid();
      auto* mask = numa_bitmask_alloc(std::thread::hardware_concurrency());
      const auto cpu_bit = i % mask->size;
      numa_bitmask_clearall(mask);
      numa_bitmask_setbit(mask, cpu_bit);

      numa_sched_setaffinity(pid, mask);
      numa_free_cpumask(mask);
#else
    worker_threads_.emplace_back([&]() {
#endif
      for (;;) {
        Dequeue();
        if (stop_ && IsEmpty() && shutdown_) {
          break;
        }
      }
    });
  }
}

ThreadPool::~ThreadPool() {
  stop_ = true;
  shutdown_ = true;
  for (auto& thread : worker_threads_) {
    thread.join();
  }
}

size_t ThreadPool::GetPoolSize() const { return worker_threads_.size(); }
void ThreadPool::StopAcceptingTransactions() { stop_ = true; }
void ThreadPool::ResumeAcceptingTransactions() { stop_ = false; }
void ThreadPool::Shutdown() { shutdown_ = true; }

bool ThreadPool::Enqueue(std::function<void()>&& job) {
  if (stop_) return false;
  thread_local static std::mt19937 random(0xDEADBEEF);
  auto& queue = work_queues_[random() % work_queues_.size()];
  return queue.enqueue(job);
}

bool ThreadPool::EnqueueForAllThreads(std::function<void()>&& job) {
  if (stop_) return false;
  for (auto& queue : no_steal_queues_) {
    while (!queue.enqueue(job)) {
    };
  }
  return true;
}

// FYI:
// https://github.com/cameron314/concurrentqueue/blob/d1ce7d3e3a6376f3d8e2831f6728e0048f339f77/samples.md#wait-for-a-queue-to-become-empty-without-dequeueing
// tl;dr concurrentqueue::size_approx returns unaccurate (approx) number.
// Thus, you cannot use this method to wait until all queues become empty.
bool ThreadPool::IsEmpty() {
  for (auto& queue : work_queues_) {
    if (queue.size_approx() != 0) {
      return false;
    }
  }
  for (auto& queue : no_steal_queues_) {
    if (queue.size_approx() != 0) {
      return false;
    }
  }
  return true;
}

void ThreadPool::WaitForQueuesToBecomeEmpty() {
  std::atomic<size_t> ends(0);
  for (auto& queue : no_steal_queues_) {
    for (;;) {
      bool success = queue.enqueue([&]() { ends.fetch_add(1); });
      if (success) break;
    }
  }
  while (ends.load() < worker_threads_.size()) std::this_thread::yield();
}

void ThreadPool::Dequeue() {
  size_t idx = GetIdxByThreadId();
  auto* my_queue = &work_queues_[idx];
  auto* my_no_steal_queue = &no_steal_queues_[idx];
  auto* selected_queue = my_queue;

  if (my_queue->size_approx() == 0 && my_no_steal_queue->size_approx() != 0) {
    selected_queue = my_no_steal_queue;
  } else {
    // work stealing
    while (selected_queue->size_approx() == 0) {
      idx++;
      if (work_queues_.size() <= idx) idx = 0;
      selected_queue = &work_queues_[idx];

      // It seems that there does not exist any active transaction
      if (my_queue == selected_queue) {
        std::this_thread::yield();
        return;
      }
    }
  }
  std::function<void()> f;
  bool dequeued = selected_queue->try_dequeue(f);
  if (dequeued) {
    assert(f);
    f();
  }
}

size_t ThreadPool::GetIdxByThreadId() {
  thread_local size_t idx = ~0llu;
  if (idx == ~0llu) {
    std::lock_guard<std::mutex> lock(thread_ids_lock_);
    idx = thread_ids_.size();
    thread_ids_.push_back(std::this_thread::get_id());
    assert(thread_ids_.size() <= no_steal_queues_.size());
  }
  return idx;
}

}  // namespace LineairDB
