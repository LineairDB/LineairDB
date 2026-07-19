/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "recovery/checkpoint_manager.hpp"

#include <algorithm>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <msgpack.hpp>
#include <mutex>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "recovery/logger.h"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "types/snapshot.hpp"
#include "util/epoch_framework.hpp"
#include "util/logger.hpp"
#include "util/thread_key_storage.h"
#include "table/table_dictionary.hpp"

namespace LineairDB {
namespace Recovery {

namespace {

std::optional<std::string> LoadFileContent(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  if (!f.good()) return std::nullopt;
  std::string buf((std::istreambuf_iterator<char>(f)),
                  std::istreambuf_iterator<char>());
  return buf.empty() ? std::nullopt : std::make_optional(std::move(buf));
}

std::vector<Logger::LogRecord> ReadLogFile(const std::string& path) {
  std::vector<Logger::LogRecord> result;
  auto buf = LoadFileContent(path);
  if (!buf) return result;
  for (size_t offset = 0; offset < buf->size();) {
    try {
      Logger::LogRecords records;
      msgpack::unpack(buf->data(), buf->size(), offset)
          .get()
          .convert(records);
      for (auto& r : records) result.push_back(std::move(r));
    } catch (...) {
      break;
    }
  }
  return result;
}

} // namespace anonymous

CPRManager::CPRManager(const Config& config, TableDictionary& /*dict*/, EpochFramework& epoch)
    : config_(config),
      epoch_manager_ref_(epoch),
      dirty_stable_empty_(true),
      checkpoint_epoch_(1),
      durable_epoch_(0),
      stop_(false),
      has_dirty_writes_(false),
      dep_file_(config.work_dir + "/cpr_durable_epoch.log"),
      dep_working_(config.work_dir + "/cpr_durable_epoch.working.log") {
  std::filesystem::create_directory(config_.work_dir);
  if (IsCheckpoint()) {
    manager_thread_ = std::thread(&CPRManager::IncrementalWorkerLoop, this);
    compactor_thread_ = std::thread(&CPRManager::CompactorLoop, this);
  }
}

CPRManager::~CPRManager() {
  Stop();
}

void CPRManager::Stop() {
  if (stop_.exchange(true)) return;
  for (auto* cv : {&worker_cv_, &compactor_cv_}) cv->notify_all();
  if (manager_thread_.joinable()) manager_thread_.join();
  if (compactor_thread_.joinable()) compactor_thread_.join();
}

void CPRManager::RotateDirtySets(EpochNumber next_checkpoint_epoch) {
  if (next_checkpoint_epoch <= checkpoint_epoch_.load()) {
    return;
  }
  if (!has_dirty_writes_.load()) {
    auto curr = durable_epoch_.load();
    while (curr < next_checkpoint_epoch &&
           !durable_epoch_.compare_exchange_weak(curr, next_checkpoint_epoch)) {
    }
    return;
  }
  has_dirty_writes_.store(false);

  checkpoint_epoch_.store(next_checkpoint_epoch);
  bool has_dirty = false;
  tls_.ForEach([&](DirtySet* ds) {
    const auto current_idx = ds->active_idx.load(std::memory_order_relaxed);
    const auto next_idx = 1 - current_idx;
    ds->active_idx.store(next_idx, std::memory_order_release);
    if (!ds->bufs[current_idx].empty()) {
      ds->dirty_stable = &ds->bufs[current_idx];
      has_dirty = true;
    } else {
      ds->dirty_stable = nullptr;
    }
  });
  if (has_dirty) {
    dirty_stable_empty_.store(false);
    worker_cv_.notify_one();
  } else {
    auto curr = durable_epoch_.load();
    while (curr < next_checkpoint_epoch &&
           !durable_epoch_.compare_exchange_weak(curr, next_checkpoint_epoch)) {
    }
  }
}

void CPRManager::PrecommitWriteSet(const WriteSetType& write_set,
                       EpochNumber current_epoch) {
  if (!IsCheckpoint() || write_set.empty()) return;
  has_dirty_writes_.store(true, std::memory_order_relaxed);
  const auto cp_epoch = checkpoint_epoch_.load(std::memory_order_acquire);
  auto* ds = tls_.Get();
  const auto idx = ds->active_idx.load(std::memory_order_acquire);
  auto* live_vec = &ds->bufs[idx];
  for (auto& snapshot : write_set) {
    auto* item = snapshot.index_cache;
    const auto stable = item->stable_epoch.load(std::memory_order_acquire);
    if (current_epoch > stable && stable >= cp_epoch) {
      if (item->checkpoint_buffer.IsEmpty()) {
        item->checkpoint_buffer.Reset(item->buffer);
      }
    }
    if (stable < current_epoch) {
      item->stable_epoch.store(current_epoch, std::memory_order_release);
    }
    if (item->dirty_epoch.load(std::memory_order_acquire) < current_epoch) {
      item->dirty_epoch.store(current_epoch, std::memory_order_release);
      live_vec->push_back({snapshot.table_name, snapshot.key, item});
    }
  }
}

void CPRManager::AbortWriteSet(const WriteSetType& /*write_set*/) {}

EpochNumber CPRManager::RecoverDurableEpoch() {
  EpochNumber epoch = 0;
  if (auto buf = LoadFileContent(dep_file_)) {
    try {
      msgpack::unpack(buf->data(), buf->size()).get().convert(epoch);
    } catch (...) {
    }
  }
  durable_epoch_.store(epoch);
  return epoch;
}

WriteSetType CPRManager::GetRecoverySetFromLogs() {
  WriteSetType recovery_set;
  if (!std::filesystem::exists(config_.work_dir)) return recovery_set;

  std::unordered_map<TableKey, size_t, TableKey::Hash> index_map;

  auto add_or_update = [&](const Logger::LogRecord::KeyValuePair& kvp) {
    const std::byte* vp =
        kvp.buffer.empty()
            ? nullptr
            : reinterpret_cast<const std::byte*>(kvp.buffer.data());
    const size_t vs = kvp.buffer.size();
    TableKey ref{kvp.table_name, kvp.key};
    auto it = index_map.find(ref);
    if (it == index_map.end()) {
      index_map[ref] = recovery_set.size();
      recovery_set.emplace_back(
          Snapshot(kvp.key, vp, vs, nullptr, kvp.table_name, kvp.tid));
    } else {
      auto& existing = recovery_set[it->second];
      if (existing.data_item_copy.transaction_id.load() < kvp.tid)
        existing.data_item_copy.Reset(vp, vs, kvp.tid);
    }
  };

  auto [base_epoch, base_file_path] = FindLatestBase();
  if (base_epoch > 0) {
    for (auto& record : ReadLogFile(base_file_path))
      for (auto& kvp : record.key_value_pairs) {
        add_or_update(kvp);
      }
  }

  auto delta_files = FindDeltaFiles();
  for (const auto& [e, path] : delta_files) {
    if (e <= base_epoch) continue;
    for (auto& log_record : ReadLogFile(path))
      for (auto& kvp : log_record.key_value_pairs) add_or_update(kvp);
  }
  return recovery_set;
}

std::vector<std::pair<EpochNumber, std::string>> CPRManager::FindLogFiles(
    std::string_view prefix,
    EpochNumber max_epoch) const {
  namespace fs = std::filesystem;
  std::vector<std::pair<EpochNumber, std::string>> result;
  if (!fs::exists(config_.work_dir)) return result;
  for (const auto& entry : fs::directory_iterator(config_.work_dir)) {
    const std::string name = entry.path().filename().string();
    if (name.rfind(prefix, 0) != 0 ||
        name.find(".log") == std::string::npos ||
        name.find(".working") != std::string::npos)
      continue;
    try {
      EpochNumber e = std::stoul(
          name.substr(prefix.size(), name.find(".log") - prefix.size()));
      if (e <= max_epoch) result.emplace_back(e, entry.path().string());
    } catch (...) {
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

std::vector<std::pair<EpochNumber, std::string>> CPRManager::FindDeltaFiles(
    EpochNumber max_epoch) const {
  return FindLogFiles(kDeltaFilePrefix, max_epoch);
}

std::pair<EpochNumber, std::string> CPRManager::FindLatestBase() const {
  auto files = FindLogFiles(kBaseFilePrefix);
  return files.empty() ? std::make_pair(EpochNumber{0}, std::string{})
                       : files.back();
}

void CPRManager::WriteDeltaCheckpoint(EpochNumber cp_epoch, bool is_shutdown) {
  Logger::LogRecord record;
  record.epoch = cp_epoch;

  tls_.ForEach([&](DirtySet* ds) {
    auto* stable_vec = ds->dirty_stable;
    ds->dirty_stable = nullptr;
    if (stable_vec) {
      for (auto& entry : *stable_vec) {
        const bool is_2pl =
            config_.concurrency_control_protocol ==
            LineairDB::Config::ConcurrencyControl::TwoPhaseLocking;
        if (is_2pl) {
          entry.item->ExclusiveLock2PL();
        } else {
          entry.item->ExclusiveLockSilo();
        }
        Logger::LogRecord::KeyValuePair kvp;
        kvp.table_name = entry.table_name;
        kvp.key = entry.key;
        if (!entry.item->IsInitialized()) {
          kvp.buffer = "";
        } else if (!entry.item->checkpoint_buffer.IsEmpty()) {
          kvp.buffer = entry.item->checkpoint_buffer.toString();
          entry.item->checkpoint_buffer.Reset(nullptr, 0);
        } else {
          kvp.buffer = entry.item->buffer.toString();
        }
        kvp.tid = {cp_epoch, 0};
        record.key_value_pairs.emplace_back(std::move(kvp));
        if (is_2pl) {
          entry.item->ExclusiveUnlock2PL();
        } else {
          entry.item->ExclusiveUnlockSilo();
        }
      }
      stable_vec->clear();
    }
  });

  bool has_new_file = false;
  if (!record.key_value_pairs.empty()) {
    std::string filename = config_.work_dir + "/" +
                           std::string(kDeltaFilePrefix) +
                           std::to_string(cp_epoch) + ".log";
    std::ofstream f(filename, std::ios_base::out | std::ios_base::binary);
    Logger::LogRecords records{std::move(record)};
    msgpack::pack(f, records);
    f.flush();
    has_new_file = true;
  }

  FlushDurableEpoch(cp_epoch, is_shutdown);

  if (has_new_file) {
    if (!is_shutdown && ++incremental_file_count_ >= kCompactionThreshold) {
      std::lock_guard<std::mutex> compactor_lock(compactor_latch_);
      compaction_requested_.store(true);
      compactor_cv_.notify_one();
    }
  }
}

void CPRManager::FlushDurableEpoch(EpochNumber cp_epoch, bool is_shutdown) {
  auto curr = durable_epoch_.load();
  while (curr < cp_epoch &&
         !durable_epoch_.compare_exchange_weak(curr, cp_epoch)) {
  }
  {
    std::ofstream f(dep_working_, std::ios_base::out | std::ios_base::binary);
    msgpack::pack(f, cp_epoch);
    f.flush();
  }
  if (rename(dep_working_.c_str(), dep_file_.c_str()) != 0) {
    SPDLOG_ERROR("durable epoch rename failed, errno: {}", errno);
    if (!is_shutdown) exit(1);
  }
}

void CPRManager::IncrementalWorkerLoop() {
  while (!stop_.load()) {
    {
      std::unique_lock<std::mutex> lock(worker_latch_);
      worker_cv_.wait_for(
          lock, std::chrono::seconds(1),
          [&] { return stop_.load() || !dirty_stable_empty_.load(); });
    }
    if (stop_.load()) break;

    if (dirty_stable_empty_.load()) {
      continue;
    }

    EpochNumber cp_epoch = checkpoint_epoch_.load();
    WriteDeltaCheckpoint(cp_epoch, false);
    dirty_stable_empty_.store(true);
  }
  WriteRemainingDirtyEntries();
}

void CPRManager::WriteRemainingDirtyEntries() {
  EpochNumber cp_epoch = epoch_manager_ref_.GetGlobalEpoch();
  checkpoint_epoch_.store(cp_epoch);
  ForceRotateBuffers();
  WriteDeltaCheckpoint(cp_epoch, true);
}

void CPRManager::ForceRotateBuffers() {
  tls_.ForEach([&](DirtySet* ds) {
    const auto idx = ds->active_idx.load(std::memory_order_relaxed);
    ds->active_idx.store(1 - idx, std::memory_order_release);
    ds->dirty_stable = ds->bufs[idx].empty() ? nullptr : &ds->bufs[idx];
  });
}

void CPRManager::CompactorLoop() {
  while (!stop_.load()) {
    std::unique_lock<std::mutex> lock(compactor_latch_);
    compactor_cv_.wait(
        lock, [&] { return stop_.load() || compaction_requested_.load(); });
    if (stop_.load()) break;
    compaction_requested_.store(false);
    lock.unlock();
    RunCompaction();
  }
}

void CPRManager::RunCompaction() {
  namespace fs = std::filesystem;
  const EpochNumber compact_up_to = durable_epoch_.load();
  if (compact_up_to == 0) return;

  auto [base_epoch, old_base_file] = FindLatestBase();
  auto delta_files = FindDeltaFiles(compact_up_to);
  if (delta_files.empty()) return;

  std::unordered_map<TableKey, const Logger::LogRecord::KeyValuePair*,
                     TableKey::Hash>
      merged;

  std::vector<std::vector<Logger::LogRecord>> all_records;
  all_records.reserve(delta_files.size() + 1);

  if (!old_base_file.empty()) {
    all_records.push_back(ReadLogFile(old_base_file));
    for (auto& record : all_records.back())
      for (const auto& kvp : record.key_value_pairs) {
        merged[TableKey{kvp.table_name, kvp.key}] = &kvp;
      }
  }

  for (const auto& [epoch, filename] : delta_files) {
    all_records.push_back(ReadLogFile(filename));
    for (auto& log_record : all_records.back()) {
      for (const auto& kvp : log_record.key_value_pairs) {
        TableKey ref{kvp.table_name, kvp.key};
        auto [it, inserted] = merged.emplace(ref, &kvp);
        if (!inserted && (it->second->key.empty() ||
                          it->second->tid.epoch < kvp.tid.epoch ||
                          (it->second->tid.epoch == kvp.tid.epoch &&
                           it->second->tid.tid < kvp.tid.tid)))
          it->second = &kvp;
      }
    }
  }

  const std::string new_base_working =
      config_.work_dir + "/" + std::string(kBaseFilePrefix) +
      std::to_string(compact_up_to) + ".log.working";
  const std::string new_base = config_.work_dir + "/" +
                               std::string(kBaseFilePrefix) +
                               std::to_string(compact_up_to) + ".log";
  {
    std::ofstream f(new_base_working,
                    std::ios_base::out | std::ios_base::binary);
    msgpack::packer<std::ofstream> pk(&f);
    pk.pack_array(1);
    pk.pack_array(2);
    pk.pack(compact_up_to);
    pk.pack_array(merged.size());
    for (auto& [k, kvp_ptr] : merged) {
      pk.pack_array(4);
      pk.pack(kvp_ptr->key);
      pk.pack(kvp_ptr->buffer);
      pk.pack(kvp_ptr->tid);
      pk.pack(kvp_ptr->table_name);
    }
    f.flush();
  }
  if (rename(new_base_working.c_str(), new_base.c_str()) != 0) {
    SPDLOG_ERROR("Compaction rename failed, errno: {}", errno);
    fs::remove(new_base_working);
    return;
  }

  for (const auto& [epoch, filename] : delta_files) {
    fs::remove(filename);
  }
  if (!old_base_file.empty() && old_base_file != new_base) {
    fs::remove(old_base_file);
  }

  incremental_file_count_.fetch_sub(delta_files.size());
}

EpochNumber CPRManager::GetCheckpointCompletedEpoch() {
  return durable_epoch_.load();
}

EpochNumber CPRManager::GetDurableEpoch() const {
  return durable_epoch_.load();
}

} // namespace Recovery
} // namespace LineairDB
