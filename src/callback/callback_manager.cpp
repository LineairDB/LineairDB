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

#include "callback_manager.h"

#include <lineairdb/config.h>

#include <memory>

#include "impl/thread_local_callback_manager.h"
#include "util/logger.hpp"

namespace LineairDB {

namespace Callback {

CallbackManager::CallbackManager(const Config& config) {
  LineairDB::Util::SetUpSPDLog();
  switch (config.callback_engine) {
    case Config::CallbackEngine::ThreadLocal:
      if (config.logger != Config::Logger::ThreadLocalLogger) {
        SPDLOG_ERROR(
            "ThreadLocal callback engine must be used with ThreadLocalLogger. "
            "Please change the configuration.");
        exit(EXIT_FAILURE);
      }
      callback_manager_pimpl_ = std::make_unique<ThreadLocalCallbackManager>();
      break;
    default:
      SPDLOG_ERROR(
          "ThreadLocal callback engine must be used with ThreadLocalLogger. "
          "Please change the configuration.");
      exit(EXIT_FAILURE);
      callback_manager_pimpl_ = std::make_unique<ThreadLocalCallbackManager>();
      break;
  }
}
CallbackManager::~CallbackManager() = default;

void CallbackManager::Enqueue(const LineairDB::Database::CallbackType& callback,
                              EpochNumber epoch, bool entrusting) {
  callback_manager_pimpl_->Enqueue(std::forward<decltype(callback)>(callback),
                                   epoch, entrusting);
}
void CallbackManager::ExecuteCallbacks(EpochNumber new_epoch) {
  callback_manager_pimpl_->ExecuteCallbacks(new_epoch);
}
void CallbackManager::WaitForAllCallbacksToBeExecuted() {
  callback_manager_pimpl_->WaitForAllCallbacksToBeExecuted();
}
};  // namespace Callback

}  // namespace LineairDB
