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

#include <cassert>
#include <cstring>
#include <experimental/filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <msgpack.hpp>
#include <util/logger.hpp>
#include <vector>

#include "recovery/logger.h"
#include "types/definitions.h"

namespace LineairDB {
namespace Recovery {

ThreadLocalLogger::ThreadLocalLogger(const Config& config)
  : WorkingDir(config.work_dir) {
    LineairDB::Util::SetUpSPDLog();
}

void ThreadLocalLogger::RememberMe(const EpochNumber epoch) {
  auto* my_storage = thread_key_storage_.Get();
  my_storage->durable_epoch.store(epoch);
}

void ThreadLocalLogger::Enqueue(const WriteSetType& ws_ref, EpochNumber epoch,
                                bool entrusting) {
  if (ws_ref.empty()) return;

  /** Make log record and add it into local buffer  **/
  Recovery::Logger::LogRecord record;
  {
    record.epoch = epoch;

    for (auto& snapshot : ws_ref) {
      assert(snapshot.data_item_copy.buffer.size < 256 &&
             "WANTFIX: LineairDB's log manager can hold only 256-bytes for a "
             "buffer of a single write operation.");
      Logger::LogRecord::KeyValuePair kvp;
      kvp.key = snapshot.key;
      std::memcpy(reinterpret_cast<void*>(&kvp.value),
                  snapshot.data_item_copy.buffer.value,
                  snapshot.data_item_copy.buffer.size);
      kvp.size = snapshot.data_item_copy.buffer.size;
      kvp.tid  = snapshot.data_item_copy.transaction_id.load();

      record.key_value_pairs.emplace_back(std::move(kvp));
    }
  }
  auto* my_storage = thread_key_storage_.Get();
  my_storage->log_records.emplace_back(std::move(record));

  if (entrusting) {
    // The callee thread is not in LineairDB's thread pool and thus
    // this thread may be terminated after this transaction
    // The log record is not persisted when 1) this thread buffers its log
    // records and 2) will be terminated soon after here.
    // To ensure durability, we immediately flush the log records.

    if (!my_storage->log_file.is_open()) {
      my_storage->log_file = std::fstream(
        GetLogFileName(my_storage->thread_id), std::fstream::out | std::fstream::binary | std::fstream::ate);
    }
    msgpack::pack(my_storage->log_file, my_storage->log_records);
    my_storage->log_file.flush();
    my_storage->log_records.clear();
    my_storage->durable_epoch.store(epoch);
  }
}

void ThreadLocalLogger::FlushLogs(EpochNumber stable_epoch) {
  auto* my_storage = thread_key_storage_.Get();

  if (!my_storage->log_records.empty()) {
    if (!my_storage->log_file.is_open()) {
      my_storage->log_file = std::fstream(
        GetLogFileName(my_storage->thread_id), std::fstream::out | std::fstream::binary | std::fstream::ate);
    }
    msgpack::pack(my_storage->log_file, my_storage->log_records);
    my_storage->log_file.flush();
    my_storage->log_records.clear();
  }

  my_storage->durable_epoch.store(stable_epoch);
}

void ThreadLocalLogger::TruncateLogs(
    const EpochNumber checkpoint_completed_epoch) {
  auto* my_storage = thread_key_storage_.Get();

  assert(my_storage->truncated_epoch <= checkpoint_completed_epoch);
  if (checkpoint_completed_epoch == my_storage->truncated_epoch) return;
  auto log_filename = GetLogFileName(my_storage->thread_id);
  std::ifstream old_file(log_filename,
                         std::ifstream::in | std::ifstream::binary);

  std::string buffer((std::istreambuf_iterator<char>(old_file)),
                     std::istreambuf_iterator<char>());
  if (buffer.empty()) {
    my_storage->truncated_epoch = checkpoint_completed_epoch;

    return;
  }
  Logger::LogRecords records;
  Logger::LogRecords deserialized_records;
  size_t offset = 0;
  for (;;) {
    if (offset == buffer.size()) break;
    try {
      auto oh  = msgpack::unpack(buffer.data(), buffer.size(), offset);
      auto obj = oh.get();
      obj.convert(deserialized_records);

    } catch (const std::bad_cast& e) {
      SPDLOG_ERROR(
          "  Stop recovery procedure: msgpack deserialize failure. Some "
          "records may not be recovered.");
      exit(EXIT_FAILURE);
    } catch (...) {
      SPDLOG_ERROR(
          "  Stop recovery procedure: msgpack deserialize failure. Some "
          "records may not be recovered.");
      exit(EXIT_FAILURE);
    }

    deserialized_records.erase(
        remove_if(deserialized_records.begin(), deserialized_records.end(),
                  [&](auto record) {
                    return record.epoch < checkpoint_completed_epoch;
                  }),
        deserialized_records.end());
    records.insert(records.end(), deserialized_records.begin(),
                   deserialized_records.end());
  }

  std::ofstream new_file(GetWorkingLogFileName(my_storage->thread_id));
  msgpack::pack(new_file, records);
  new_file.flush();

  // NOTE POSIX ensures that rename syscall provides atomicity
  if (rename(log_filename.c_str(), GetLogFileName(my_storage->thread_id).c_str())) {
    SPDLOG_ERROR("Durability Error: fail to truncate logfile. errno: {1}",
                 errno);
    exit(1);
  }
  my_storage->truncated_epoch = checkpoint_completed_epoch;
  my_storage->log_file        = std::fstream(
      GetLogFileName(my_storage->thread_id),
      std::fstream::out | std::fstream::binary | std::fstream::ate);
}

EpochNumber ThreadLocalLogger::GetMinDurableEpochForAllThreads() {
  EpochNumber min_flushed_epoch = EpochFramework::THREAD_OFFLINE;
  thread_key_storage_.ForEach(
      [&](const ThreadLocalStorageNode* thread_local_node) {
        const EpochNumber epoch = thread_local_node->durable_epoch.load();
        if (epoch == EpochFramework::THREAD_OFFLINE) return;
        if (epoch < min_flushed_epoch) min_flushed_epoch = epoch;
      });
  return min_flushed_epoch;
}

std::string ThreadLocalLogger::GetLogFileName(size_t thread_id) const {
  // TODO: think of beautiful path concatation in C++
  return WorkingDir + "/thread" + std::to_string(thread_id) + ".log";
}

std::string ThreadLocalLogger::GetWorkingLogFileName(size_t thread_id) const {
  return WorkingDir + "/thread" + std::to_string(thread_id) +
         ".working.log";
}

std::atomic<size_t> ThreadLocalLogger::ThreadLocalStorageNode::ThreadIdCounter =
    {0};

}  // namespace Recovery
}  // namespace LineairDB
