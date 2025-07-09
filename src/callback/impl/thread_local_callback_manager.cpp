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

#include "thread_local_callback_manager.h"

#include <lineairdb/database.h>

#include "types/definitions.h"
#include "util/backoff.hpp"

namespace LineairDB {

namespace Callback {

ThreadLocalCallbackManager::ThreadLocalCallbackManager()
    : work_steal_queue_size_(0) {}

void ThreadLocalCallbackManager::Enqueue(
    const LineairDB::Database::CallbackType& callback, EpochNumber epoch,
    bool entrusting) {
  if (entrusting) {
    // The callee thread is not willing to manage this callback.
    // Enqueue to work-stealing queue.
    auto* my_work_steal_queue = GetMyWorkStealingQueue();
    my_work_steal_queue->queue.enqueue({epoch, callback});

  } else {
    // The caller thread manages the callback. Enqueue to thread-local queue.
    auto* my_storage = thread_key_storage_.Get();
    my_storage->callback_queue.push({epoch, callback});
  }
}
void ThreadLocalCallbackManager::ExecuteCallbacks(EpochNumber stable_epoch) {
  auto* queues = thread_key_storage_.Get();
  auto& callback_queue = queues->callback_queue;

  if (callback_queue.empty()) {
    // my thread-local callback queue is empty.
    // helping to the jobs on the work-stealing queue.
    const size_t queue_size = work_steal_queue_size_.load();
    if (0 == queue_size) return;
    size_t checked_queue = 0;
    auto work_steal_queue_itr = work_steal_queues_.begin();
    for (;;) {
      auto& queue = work_steal_queue_itr->queue;

      for (;;) {
        if (queue.size_approx() == 0) break;
        std::pair<EpochNumber, LineairDB::Database::CallbackType> pair;
        auto dequeued = queue.try_dequeue(pair);
        if (!dequeued) break;
        if (pair.first < stable_epoch) {
          pair.second(TxStatus::Committed);
        } else {
          queue.enqueue(std::move(pair));
          break;
        }
      }

      checked_queue++;
      if (checked_queue == queue_size) break;
      work_steal_queue_itr++;
    }
  } else {
    for (;;) {
      if (callback_queue.empty()) break;
      auto& entry = callback_queue.front();
      if (entry.first < stable_epoch) {
        entry.second(TxStatus::Committed);
        callback_queue.pop();
      } else {
        break;
      }
    }
  }
}
void ThreadLocalCallbackManager::WaitForAllCallbacksToBeExecuted() {
  thread_key_storage_.ForEach(
      [&](const ThreadLocalStorageNode* thread_local_node) {
        auto& queue = thread_local_node->callback_queue;
        for (;;) {
          if (queue.empty()) {
            break;
          }
          std::this_thread::yield();
        }
      });

  size_t checked = 0;
  const size_t work_steal_queue_size = work_steal_queue_size_.load();
  if (0 == work_steal_queue_size) return;
  auto itr = work_steal_queues_.begin();
  for (;;) {
    for (;;) {
      if (itr->queue.size_approx() == 0) break;
      std::this_thread::yield();
    }
    checked++;
    if (checked == work_steal_queue_size) break;
    itr++;
  }
  // Here we observed empty queue for all thread.
}

ThreadLocalCallbackManager::WorkStealingQueueNode*
ThreadLocalCallbackManager::GetMyWorkStealingQueue() {
  auto* my_node = thread_local_work_steal_queue_
                      .Get<ThreadLocalCallbackManager::WorkStealingQueueNode*>(
                          []() { return nullptr; });
  if (nullptr != *my_node) return *my_node;

  std::lock_guard<std::mutex> guard(list_lock_);
  work_steal_queues_.emplace_back();
  work_steal_queue_size_.fetch_add(1);
  *my_node = &work_steal_queues_.back();

  return *my_node;
}

}  // namespace Callback
}  // namespace LineairDB
