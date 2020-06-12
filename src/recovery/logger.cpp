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

#include "logger.h"

#include <glob.h>
#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/tx_status.h>

#include <cstring>
#include <experimental/filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <msgpack.hpp>
#include <util/logger.hpp>

#include "impl/thread_local_logger.h"
#include "types.h"

namespace LineairDB {
namespace Recovery {

Logger::Logger(const Config& config)
    : durable_epoch_(0),
      durable_epoch_working_file_(DurableEpochNumberWorkingFileName,
                                  std::ofstream::trunc) {
  std::experimental::filesystem::create_directory("lineairdb_logs");
  LineairDB::Util::SetUpSPDLog();
  switch (config.logger) {
    case Config::Logger::ThreadLocalLogger:
      logger_ = std::make_unique<ThreadLocalLogger>();
      break;
    default:
      logger_ = std::make_unique<ThreadLocalLogger>();
      break;
  }
}
Logger::~Logger() = default;

void Logger::RememberMe(const EpochNumber epoch) { logger_->RememberMe(epoch); }
void Logger::Enqueue(const WriteSetType& ws_ref, EpochNumber epoch) {
  logger_->Enqueue(ws_ref, epoch);
}
void Logger::FlushLogs(const EpochNumber stable_epoch) {
  logger_->FlushLogs(stable_epoch);
}

EpochNumber Logger::FlushDurableEpoch() {
  auto min_flushed_epoch = logger_->GetMinDurableEpochForAllThreads();
  if (min_flushed_epoch == EpochFramework::THREAD_OFFLINE ||
      min_flushed_epoch == durable_epoch_) {
    return durable_epoch_;
  }

  assert(durable_epoch_ < min_flushed_epoch);
  if (!durable_epoch_working_file_.is_open())
    durable_epoch_working_file_.open(DurableEpochNumberWorkingFileName);

  durable_epoch_ = min_flushed_epoch;
  durable_epoch_working_file_ << durable_epoch_;

  // NOTE POSIX ensures that rename syscall provides atomicity
  if (rename(DurableEpochNumberWorkingFileName, DurableEpochNumberFileName)) {
    SPDLOG_ERROR(
        "Durability Error: fail to flush the durable epoch number {0:d}. "
        "errno: {1}",
        durable_epoch_, errno);
    exit(1);
  }
  durable_epoch_working_file_.close();
  durable_epoch_working_file_.open(DurableEpochNumberWorkingFileName,
                                   std::fstream::trunc);

  return durable_epoch_;
}

EpochNumber Logger::GetDurableEpoch() { return durable_epoch_; }
void Logger::SetDurableEpoch(const EpochNumber e) { durable_epoch_ = e; }

EpochNumber Logger::GetDurableEpochFromLog() {
  std::ifstream file(Recovery::Logger::DurableEpochNumberFileName,
                     std::ios::binary | std::ios::ate);
  EpochNumber epoch;
  auto filesize = file.tellg();

  if (0 < filesize) {
    file.seekg(0, std::ios::beg);
    file >> epoch;
  } else {
    epoch = 0;
  }
  SPDLOG_DEBUG("Durable epoch number is resumed from {0:d}", epoch);

  return epoch;
}

static inline std::vector<std::string> glob(const std::string& pat) {
  using namespace std;
  glob_t glob_result;
  ::glob(pat.c_str(), GLOB_TILDE, NULL, &glob_result);
  vector<string> ret;
  for (unsigned int i = 0; i < glob_result.gl_pathc; ++i) {
    ret.push_back(string(glob_result.gl_pathv[i]));
  }
  globfree(&glob_result);
  return ret;
}

WriteSetType Logger::GetRecoverySetFromLogs(const EpochNumber durable_epoch) {
  auto logfiles = glob("lineairdb_logs/thread*");
  SPDLOG_DEBUG("Replay the logs in epoch 0-{0}", durable_epoch);
  WriteSetType recovery_set;
  recovery_set.clear();

  for (auto filename : logfiles) {
    std::ifstream file(filename, std::ifstream::in | std::ifstream::binary);
    SPDLOG_DEBUG(" Recovery filename {0}", filename);
    if (!file.good()) {
      SPDLOG_ERROR(
          "  Stop recovery procedure: file {0} is broken. Some records may not "
          "be recovered.");
      return recovery_set;
    };

    std::string buffer((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());
    if (buffer.empty()) continue;

    LogRecords log_records;
    size_t offset = 0;
    for (;;) {
      if (offset == buffer.size()) break;
      try {
        auto oh  = msgpack::unpack(buffer.data(), buffer.size(), offset);
        auto obj = oh.get();
        obj.convert(log_records);
      } catch (const std::bad_cast& e) {
        SPDLOG_ERROR(
            "  Stop recovery procedure: msgpack deserialize failure on file "
            "{0}. Some records may not be recovered.");
        return recovery_set;
      } catch (...) {
        SPDLOG_ERROR(
            "  Stop recovery procedure: msgpack deserialize failure on file "
            "{0}. Some records may not be recovered.");
        return recovery_set;
      }

      for (auto& log_record : log_records) {
        assert(0 < log_record.epoch);
        if (log_record.epoch <= durable_epoch) {
          for (auto& kvp : log_record.key_value_pairs) {
            SPDLOG_DEBUG("    kvp", kvp.key);
            bool not_found = true;
            for (auto& item : recovery_set) {
              if (item.key == kvp.key) {
                not_found = false;
                if (item.index_cache->transaction_id.load() < kvp.tid) {
                  item.index_cache->Reset(
                      reinterpret_cast<std::byte*>(&kvp.value), kvp.size);
                  item.index_cache->transaction_id = kvp.tid;
                  SPDLOG_DEBUG("    update-> key {0}, version {1} in epoch {2}",
                               kvp.key, kvp.tid.tid, kvp.tid.epoch);
                }
              }
            }
            if (not_found) {
              SPDLOG_DEBUG("    insert-> key {0}, version {1} in epoch {2}",
                           kvp.key, kvp.tid.tid, kvp.tid.epoch);
              Snapshot snapshot = {kvp.key, &kvp.value[0], kvp.size, nullptr,
                                   kvp.tid};
              recovery_set.emplace_back(std::move(snapshot));
            }
          }
        }
      }
    }

    SPDLOG_DEBUG(" Close filename {0}", filename);
  }
  return recovery_set;
}

}  // namespace Recovery
}  // namespace LineairDB
