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

#include "thread_local_logger.h"

#include <glob.h>
#include <lineairdb/database.h>
#include <lineairdb/tx_status.h>
#include <types.h>

#include <cassert>
#include <cstring>
#include <experimental/filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <msgpack.hpp>
#include <util/logger.hpp>

#include "recovery/logger.h"

namespace LineairDB {
namespace Recovery {

ThreadLocalLogger::ThreadLocalLogger() { LineairDB::Util::SetUpSPDLog(); }

void ThreadLocalLogger::RememberMe(const EpochNumber epoch) {
  auto* my_storage = thread_key_storage_.Get();
  my_storage->durable_epoch.store(epoch);
}

void ThreadLocalLogger::Enqueue(const WriteSetType& ws_ref, EpochNumber epoch) {
  if (ws_ref.empty()) return;

  /** Make log record and add it into local buffer  **/
  Recovery::Logger::LogRecord record;
  record.epoch = epoch;

  for (auto& snapshot : ws_ref) {
    assert(snapshot.data_item_copy.size < 256 &&
           "WANTFIX: LineairDB's log manager can hold only 256-bytes for a "
           "buffer of a single write operation.");
    Logger::LogRecord::KeyValuePair kvp;
    kvp.key = snapshot.key;
    std::memcpy(reinterpret_cast<void*>(&kvp.value),
                snapshot.data_item_copy.value, snapshot.data_item_copy.size);
    kvp.size               = snapshot.data_item_copy.size;
    kvp.version_with_epoch = snapshot.version_in_epoch;

    record.key_value_pairs.emplace_back(std::move(kvp));
  }
  auto* my_storage = thread_key_storage_.Get();
  my_storage->log_records.emplace_back(std::move(record));
}

void ThreadLocalLogger::FlushLogs(EpochNumber stable_epoch) {
  auto* my_storage = thread_key_storage_.Get();

  if (!my_storage->log_records.empty()) {
    msgpack::pack(my_storage->log_file, my_storage->log_records);
    my_storage->log_file.flush();
    my_storage->log_records.clear();
  }

  my_storage->durable_epoch.store(stable_epoch);
}

EpochNumber ThreadLocalLogger::GetMinDurableEpochForAllThreads() {
  EpochNumber min_flushed_epoch = EpochFramework::THREAD_OFFLINE;
  thread_key_storage_.ForEach(
      [&](const ThreadLocalStorageNode* thread_local_node) {
        const EpochNumber epoch = thread_local_node->durable_epoch.load();
        if (epoch < min_flushed_epoch) min_flushed_epoch = epoch;
      });
  return min_flushed_epoch;
}

std::atomic<size_t> ThreadLocalLogger::ThreadLocalStorageNode::ThreadIdCounter =
    {0};

}  // namespace Recovery
}  // namespace LineairDB
