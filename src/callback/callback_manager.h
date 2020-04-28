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
#include "types.h"

namespace LineairDB {

namespace Callback {

class CallbackManager {
 public:
  CallbackManager(const Config& c);
  ~CallbackManager();

  // Methods that pass (delegate) to callback_manager_base_
  void Enqueue(const LineairDB::Database::CallbackType& callback,
               EpochNumber epoch);
  void ExecuteCallbacks(EpochNumber new_epoch);
  void WaitForAllCallbacksToBeExecuted();

 private:
  std::unique_ptr<CallbackManagerBase> callback_manager_pimpl_;
};

}  // namespace Callback
}  // namespace LineairDB

#endif /* LINEAIRDB_CALLBACK_MANAGER_H */
