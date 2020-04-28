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

#include "types.h"

namespace LineairDB {

namespace Callback {

void ThreadLocalCallbackManager::Enqueue(
    const LineairDB::Database::CallbackType& callback, EpochNumber epoch) {
  /** Add callback into callbackqueue **/
  auto* my_storage = thread_key_storage_.Get();
  my_storage->callback_queue.push({epoch, callback});
}
void ThreadLocalCallbackManager::ExecuteCallbacks(EpochNumber stable_epoch) {
  auto* queues         = thread_key_storage_.Get();
  auto& callback_queue = queues->callback_queue;

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
void ThreadLocalCallbackManager::WaitForAllCallbacksToBeExecuted() {
  // NOTE DO NOT CALL FROM WORKER THREAD
  thread_key_storage_.ForEach(
      [&](const ThreadLocalStorageNode* thread_local_node) {
        auto& queue = thread_local_node->callback_queue;
        for (;;) {
          if (queue.empty()) { break; }
          std::this_thread::yield();
        }
      });
  // Here we observed empty queue for all thread.
}

}  // namespace Callback
}  // namespace LineairDB
