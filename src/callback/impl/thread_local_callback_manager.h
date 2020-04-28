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

#ifndef LINEAIRDB_THREAD_LOCAL_CALLBACK_MANAGER_BASE_H
#define LINEAIRDB_THREAD_LOCAL_CALLBACK_MANAGER_BASE_H

#include <atomic>
#include <queue>

#include "callback/callback_manager_base.h"
#include "types.h"
#include "util/thread_key_storage.h"

namespace LineairDB {

namespace Callback {

class ThreadLocalCallbackManager final : public CallbackManagerBase {
 public:
  void Enqueue(const LineairDB::Database::CallbackType& callback,
               EpochNumber epoch) final override;
  void ExecuteCallbacks(EpochNumber new_epoch) final override;
  void WaitForAllCallbacksToBeExecuted() final override;

 private:
  struct ThreadLocalStorageNode {
   private:
    static std::atomic<size_t> ThreadIdCounter;

   public:
    std::queue<std::pair<EpochNumber, LineairDB::Database::CallbackType>>
        callback_queue;
  };

 private:
  ThreadKeyStorage<ThreadLocalStorageNode> thread_key_storage_;
};

}  // namespace Callback
}  // namespace LineairDB

#endif /* LINEAIRDB_THREAD_LOCAL_CALLBACK_MANAGER_BASE_H */
