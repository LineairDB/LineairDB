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

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <msgpack.hpp>
#include <unordered_map>
#include <util/logger.hpp>

#include "impl/thread_local_logger.h"
#include "types/definitions.h"

namespace LineairDB {
namespace Recovery {

Logger::Logger(const Config& config)
    : DurableEpochNumberFileName(config.work_dir + "/durable_epoch.json"),
      DurableEpochNumberWorkingFileName(config.work_dir +
                                        "/durable_epoch.working.json"),
      WorkingDir(config.work_dir),
      durable_epoch_(0),
      durable_epoch_working_file_(DurableEpochNumberWorkingFileName,
                                  std::ofstream::trunc) {
  std::filesystem::create_directory(config.work_dir);
  LineairDB::Util::SetUpSPDLog();
  switch (config.logger) {
    case Config::Logger::ThreadLocalLogger:
      logger_ = std::make_unique<ThreadLocalLogger>(config);
      break;
    default:
      logger_ = std::make_unique<ThreadLocalLogger>(config);
      break;
  }
}
Logger::~Logger() = default;

void Logger::RememberMe(const EpochNumber epoch) { logger_->RememberMe(epoch); }
void Logger::Enqueue(const WriteSetType& ws_ref, EpochNumber epoch,
                     bool entrusting) {
  logger_->Enqueue(ws_ref, epoch, entrusting);
}
void Logger::FlushLogs(const EpochNumber stable_epoch) {
  logger_->FlushLogs(stable_epoch);
}

void Logger::TruncateLogs(const EpochNumber checkpoint_completed_epoch) {
  logger_->TruncateLogs(checkpoint_completed_epoch);
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
  if (rename(DurableEpochNumberWorkingFileName.c_str(),
             DurableEpochNumberFileName.c_str())) {
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
  std::ifstream file(DurableEpochNumberFileName,
                     std::ios::binary | std::ios::ate);
  EpochNumber epoch;
  auto filesize = file.tellg();

  if (0 < filesize) {
    file.seekg(0, std::ios::beg);
    file >> epoch;
  } else {
    epoch = 0;
  }

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
  SPDLOG_DEBUG("Replay the logs in epoch 0-{0}", durable_epoch);
  SPDLOG_DEBUG("Check WorkingDirectory {0}", WorkingDir);

  auto logfiles = glob(WorkingDir + "/thread*");
  const std::string checkpoint_filename = WorkingDir + "/checkpoint.log";
  bool checkpoint_file_exists = false;
  {
    std::ifstream ifs(checkpoint_filename);
    checkpoint_file_exists = ifs.is_open();
  }
  if (checkpoint_file_exists) logfiles.push_back(checkpoint_filename);
  WriteSetType recovery_set;
  recovery_set.clear();
  struct SecondaryOpKey {
    std::string table_name;
    std::string index_name;
    uint32_t index_type;
    std::string secondary_key;
    std::string primary_key;
    bool operator==(const SecondaryOpKey& rhs) const {
      return table_name == rhs.table_name && index_name == rhs.index_name &&
             index_type == rhs.index_type &&
             secondary_key == rhs.secondary_key &&
             primary_key == rhs.primary_key;
    }
  };
  struct SecondaryOpKeyHash {
    size_t operator()(const SecondaryOpKey& key) const {
      const std::hash<std::string> hasher;
      size_t seed = hasher(key.table_name);
      seed ^= hasher(key.index_name) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= std::hash<uint32_t>{}(key.index_type) + 0x9e3779b9 +
              (seed << 6) + (seed >> 2);
      seed ^=
          hasher(key.secondary_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= hasher(key.primary_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      return seed;
    }
  };
  struct SecondaryOpState {
    TransactionId tid;
    SecondaryIndexOp op;
  };
  struct SecondaryGroupKey {
    std::string table_name;
    std::string index_name;
    uint32_t index_type;
    std::string secondary_key;
    bool operator==(const SecondaryGroupKey& rhs) const {
      return table_name == rhs.table_name && index_name == rhs.index_name &&
             index_type == rhs.index_type &&
             secondary_key == rhs.secondary_key;
    }
  };
  struct SecondaryGroupKeyHash {
    size_t operator()(const SecondaryGroupKey& key) const {
      const std::hash<std::string> hasher;
      size_t seed = hasher(key.table_name);
      seed ^= hasher(key.index_name) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= std::hash<uint32_t>{}(key.index_type) + 0x9e3779b9 +
              (seed << 6) + (seed >> 2);
      seed ^=
          hasher(key.secondary_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      return seed;
    }
  };
  struct SecondaryGroupValue {
    TransactionId max_tid{};
    std::vector<std::string> primary_keys;
  };

  std::unordered_map<SecondaryOpKey, SecondaryOpState, SecondaryOpKeyHash>
      secondary_latest;

  for (auto filename : logfiles) {
    std::ifstream file(filename, std::ifstream::in | std::ifstream::binary);
    if (!file.good()) {
      SPDLOG_ERROR(
          "  Stop recovery procedure: file {0} is broken. Some records may not "
          "be recovered.",
          filename);
      exit(EXIT_FAILURE);
    };

    std::string buffer((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());
    if (buffer.empty()) continue;
    SPDLOG_DEBUG(" Start recovery from {0}", filename);

    LogRecords log_records;
    size_t primary_inserts = 0;
    size_t primary_updates = 0;
    size_t secondary_full_entries = 0;
    size_t secondary_delta_entries = 0;
    size_t offset = 0;
    for (;;) {
      if (offset == buffer.size()) break;
      try {
        auto oh = msgpack::unpack(buffer.data(), buffer.size(), offset);
        auto obj = oh.get();
        obj.convert(log_records);
      } catch (const std::bad_cast& e) {
        SPDLOG_ERROR(
            "  Stop recovery procedure: msgpack deserialize failure on file "
            "{0}. Some records may not be recovered.");
        SPDLOG_DEBUG("Error code: {0}", e.what());
        return recovery_set;
      } catch (...) {
        SPDLOG_ERROR(
            "  Stop recovery procedure: msgpack deserialize failure on file "
            "{0}. Some records may not be recovered.");
        return recovery_set;
      }

      auto ensure_initialized = [](LineairDB::DataItem& data_item) {
        data_item.initialized = !data_item.primary_keys.empty();
      };
      for (auto& log_record : log_records) {
        assert(0 < log_record.epoch);
        if (filename == checkpoint_filename ||
            log_record.epoch <= durable_epoch) {
          for (auto& kvp : log_record.key_value_pairs) {
            const auto op = static_cast<SecondaryIndexOp>(kvp.secondary_op);
            const bool is_secondary_index =
                !kvp.index_name.empty() || op != SecondaryIndexOp::None ||
                !kvp.primary_keys.empty() || !kvp.secondary_primary_key.empty() ||
                kvp.index_type != 0;
            if (is_secondary_index) {
              if (op == SecondaryIndexOp::Full) {
                for (const auto& pk : kvp.primary_keys) {
                  SecondaryOpKey op_key{kvp.table_name, kvp.index_name,
                                        kvp.index_type, kvp.key, pk};
                  auto it = secondary_latest.find(op_key);
                  if (it == secondary_latest.end() ||
                      it->second.tid < kvp.tid) {
                    secondary_latest[op_key] = {kvp.tid, SecondaryIndexOp::Add};
                  }
                }
                secondary_full_entries += kvp.primary_keys.size();
              } else if (!kvp.secondary_primary_key.empty()) {
                SecondaryOpKey op_key{kvp.table_name, kvp.index_name,
                                      kvp.index_type, kvp.key,
                                      kvp.secondary_primary_key};
                auto it = secondary_latest.find(op_key);
                if (it == secondary_latest.end() || it->second.tid < kvp.tid) {
                  secondary_latest[op_key] = {kvp.tid, op};
                }
                secondary_delta_entries++;
              }
              continue;
            }
            const std::byte* kvp_value_ptr =
                kvp.buffer.empty()
                    ? nullptr
                    : reinterpret_cast<const std::byte*>(kvp.buffer.data());
            const size_t kvp_value_size = kvp.buffer.size();
            bool not_found = true;
            for (auto& item : recovery_set) {
              if (item.key == kvp.key && item.table_name == kvp.table_name &&
                  item.index_name == kvp.index_name) {
                not_found = false;
                if (item.data_item_copy.transaction_id.load() < kvp.tid) {
                  item.data_item_copy.Reset(kvp_value_ptr, kvp_value_size,
                                            kvp.tid);
                  item.table_name = kvp.table_name;
                  item.index_name = kvp.index_name;
                  item.index_type =
                      Index::SecondaryIndexType::FromRaw(kvp.index_type);
                  if (is_secondary_index) {
                    item.data_item_copy.primary_keys = kvp.primary_keys;
                    ensure_initialized(item.data_item_copy);
                  }

                  primary_updates++;
                }
              }
            }
            if (not_found) {
              Snapshot snapshot = {
                  kvp.key,
                  reinterpret_cast<std::byte*>(kvp.buffer.data()),
                  kvp.buffer.size(),
                  nullptr,
                  kvp.table_name,
                  kvp.index_name,
                  kvp.tid,
                  Index::SecondaryIndexType::FromRaw(kvp.index_type),
              };
              if (is_secondary_index) {
                snapshot.data_item_copy.primary_keys = kvp.primary_keys;
                ensure_initialized(snapshot.data_item_copy);
              }
              recovery_set.emplace_back(std::move(snapshot));
              primary_inserts++;
            }
          }
        }
      }
    }

    if (primary_inserts || primary_updates || secondary_full_entries ||
        secondary_delta_entries) {
      SPDLOG_DEBUG(
          "  Recovery summary: primary inserts {0}, updates {1}, "
          "secondary full entries {2}, secondary delta entries {3}",
          primary_inserts, primary_updates, secondary_full_entries,
          secondary_delta_entries);
    }
    SPDLOG_DEBUG(" Close filename {0}", filename);
  }

  std::unordered_map<SecondaryGroupKey, SecondaryGroupValue,
                     SecondaryGroupKeyHash>
      grouped_secondary;
  for (const auto& [op_key, state] : secondary_latest) {
    if (state.op != SecondaryIndexOp::Add) continue;
    SecondaryGroupKey group_key{op_key.table_name, op_key.index_name,
                                op_key.index_type,
                                op_key.secondary_key};
    auto& entry = grouped_secondary[group_key];
    entry.primary_keys.emplace_back(op_key.primary_key);
    if (entry.max_tid < state.tid) {
      entry.max_tid = state.tid;
    }
  }

  for (auto& [group_key, entry] : grouped_secondary) {
    if (entry.primary_keys.empty()) continue;
    std::sort(entry.primary_keys.begin(), entry.primary_keys.end());
    Snapshot snapshot = {
        group_key.secondary_key, nullptr,      0, nullptr, group_key.table_name,
        group_key.index_name,    entry.max_tid,
        Index::SecondaryIndexType::FromRaw(group_key.index_type)};
    snapshot.data_item_copy.primary_keys = std::move(entry.primary_keys);
    snapshot.data_item_copy.Reset(nullptr, 0, entry.max_tid);
    recovery_set.emplace_back(std::move(snapshot));
  }
  return recovery_set;
}

}  // namespace Recovery
}  // namespace LineairDB
