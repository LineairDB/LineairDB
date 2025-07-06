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

#ifndef LINEAIRDB_RECOVERY_LOGGER_BASE_H
#define LINEAIRDB_RECOVERY_LOGGER_BASE_H

#include "types/data_item.hpp"
#include "types/definitions.h"
#include "types/snapshot.hpp"

namespace LineairDB {
namespace Recovery {

class LoggerBase {
public:
  virtual ~LoggerBase() {}
  virtual void RememberMe(const EpochNumber) = 0;
  virtual void Enqueue(const WriteSetType& ws_ref_, EpochNumber epoch, bool entrusting) = 0;
  virtual void FlushLogs(EpochNumber stable_epoch) = 0;
  virtual void TruncateLogs(const EpochNumber) = 0;
  virtual EpochNumber GetMinDurableEpochForAllThreads() = 0;
};

} // namespace Recovery
} // namespace LineairDB

#endif /* LINEAIRDB_RECOVERY_LOGGER_BASE_H */
