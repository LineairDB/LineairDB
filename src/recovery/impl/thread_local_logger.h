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
#ifndef LINEAIRDB_RECOVERY_THREAD_LOCAL_LOGGER_H
#define LINEAIRDB_RECOVERY_THREAD_LOCAL_LOGGER_H

#include <lineairdb/database.h>
#include <lineairdb/tx_status.h>
#include <stdio.h>

#include <cstdio>
#include <fstream>
#include <functional>
#include <msgpack.hpp>
#include <queue>
#include <sstream>

#include "recovery/logger.h"
#include "recovery/logger_base.h"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"
#include "util/thread_key_storage.h"

namespace LineairDB {
namespace Recovery {

class ThreadLocalLogger final : public LoggerBase {
 public:
  ThreadLocalLogger(const Config&);
  void RememberMe(const EpochNumber) final override;
  void Enqueue(const WriteSetType& ws_ref_, EpochNumber epoch,
               bool entrusting) final override;
  void FlushLogs(EpochNumber stable_epoch) final override;
  void TruncateLogs(
      const EpochNumber checkpoint_completed_epoch) final override;
  EpochNumber GetMinDurableEpochForAllThreads() final override;
  std::string GetLogFileName(size_t thread_id) const;
  std::string GetWorkingLogFileName(size_t thread_id) const;

 private:
  std::string WorkingDir;
  struct ThreadLocalStorageNode {
   private:
    static std::atomic<size_t> ThreadIdCounter;

   public:
    size_t thread_id;
    std::atomic<EpochNumber> durable_epoch;
    EpochNumber truncated_epoch;
    std::fstream log_file;
    Logger::LogRecords log_records;
    MSGPACK_DEFINE(log_records);

    ThreadLocalStorageNode()
        : thread_id(ThreadIdCounter.fetch_add(1)),
          durable_epoch(EpochFramework::THREAD_OFFLINE),
          truncated_epoch(0) {}
    ~ThreadLocalStorageNode() {}
  };

 private:
  ThreadKeyStorage<ThreadLocalStorageNode> thread_key_storage_;
};

}  // namespace Recovery
}  // namespace LineairDB
#endif /* LINEAIRDB_RECOVERY_THREAD_LOCAL_LOGGER_H */
