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

#ifndef LINEAIRDB_CALLBACK_MANAGER_BASE_H
#define LINEAIRDB_CALLBACK_MANAGER_BASE_H

#include <lineairdb/database.h>

#include "types/definitions.h"

namespace LineairDB {

namespace Callback {

class CallbackManagerBase {
 public:
  virtual ~CallbackManagerBase() {}
  virtual void Enqueue(const Database::CallbackType& callback,
                       EpochNumber epoch, bool entrusting) = 0;
  virtual void ExecuteCallbacks(EpochNumber new_epoch)     = 0;
  virtual void WaitForAllCallbacksToBeExecuted()           = 0;
};

}  // namespace Callback
}  // namespace LineairDB

#endif /* LINEAIRDB_CALLBACK_MANAGER_BASE_H */
