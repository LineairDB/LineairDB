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

#ifndef LINEAIRDB_CALLBACK_MANAGER_H
#define LINEAIRDB_CALLBACK_MANAGER_H

#include <lineairdb/config.h>

#include <memory>

#include "callback_manager_base.h"
#include "types/definitions.h"

namespace LineairDB {

namespace Callback {

class CallbackManager {
 public:
  CallbackManager(const Config& c);
  ~CallbackManager();

  /**
   * @brief Enqueue a callback function which will be invoked after the
   * recoverability is satisfied.
   *
   * @param callback A callback function.
   * @param epoch A epoch number which includes the transaction related to
   * callback.
   * @param entrusting Flag to delegate the management of this callback
function. We can decide here whether the callee thread is willing to manage the
callback or not. If it is set to false, it is expected the other threads to
manage this callback as work-stealing.
   */
  void Enqueue(const Database::CallbackType& callback, EpochNumber epoch,
               bool entrusting = false);
  void ExecuteCallbacks(EpochNumber new_epoch);
  void WaitForAllCallbacksToBeExecuted();

 private:
  std::unique_ptr<CallbackManagerBase> callback_manager_pimpl_;
};

}  // namespace Callback
}  // namespace LineairDB

#endif /* LINEAIRDB_CALLBACK_MANAGER_H */
