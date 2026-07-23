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
#include <memory>
#include <msgpack.hpp>
#include <mutex>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "recovery/logger.h"
#include "table/table_dictionary.hpp"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "types/snapshot.hpp"
#include "util/epoch_framework.hpp"
#include "util/logger.hpp"
#include "util/thread_key_storage.h"

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
      msgpack::unpack(buf->data(), buf->size(), offset).get().convert(records);
      for (auto& r : records) result.push_back(std::move(r));
    } catch (...) {
      break;
    }
  }
  return result;
}

}  // namespace

CPRManager::CPRManager(const Config& config, TableDictionary& /*dict*/,
                       EpochFramework& epoch)
    : config_(config),
      epoch_manager_ref_(epoch),
      dirty_stable_empty_(true),
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

CPRManager::~CPRManager() { Stop(); }

void CPRManager::Stop() {
  if (stop_.exchange(true)) return;
  for (auto* cv : {&worker_cv_, &compactor_cv_}) cv->notify_all();
  if (manager_thread_.joinable()) manager_thread_.join();
  if (compactor_thread_.joinable()) compactor_thread_.join();
}

// CPR phase state machine: REST -> IN_PROGRESS -> WAIT_PENDING -> REST.
// CPR's classic PREPARE phase (a thread handshake confirming a phase change
// before it takes effect) is omitted: EpochFramework's quiescence guarantee
// -- the global epoch never advances past an online thread's stale local
// epoch (see GetSmallestEpoch in epoch_framework.hpp) -- gives the same
// guarantee for free, one epoch tick after a round starts.
//
// Called once per epoch tick (Database::Impl::EventsOnEpochIsUpdated),
// always from the same thread.
void CPRManager::RotateDirtySets(EpochNumber new_epoch) {
  switch (phase_.load(std::memory_order_acquire)) {
    case Phase::REST: {
      const auto now = std::chrono::steady_clock::now();
      const bool period_elapsed =
          config_.checkpoint_period == 0 ||
          now - last_round_started_at_ >=
              std::chrono::milliseconds(config_.checkpoint_period);
      if (!period_elapsed) return;
      if (!has_dirty_writes_.exchange(false)) {
        // No writes since the last round -- nothing to drain, just advance
        // durable_epoch_.
        auto curr = durable_epoch_.load();
        while (curr < new_epoch &&
               !durable_epoch_.compare_exchange_weak(curr, new_epoch)) {
        }
        return;
      }
      // version_ = new_epoch protects writes with current_epoch >= new_epoch.
      version_.store(new_epoch, std::memory_order_release);
      last_round_started_at_ = now;
      phase_.store(Phase::IN_PROGRESS, std::memory_order_release);
      return;
    }
    case Phase::IN_PROGRESS: {
      // Safe to drain now -- one tick has passed (see class comment).
      phase_.store(Phase::WAIT_PENDING, std::memory_order_release);
      dirty_stable_empty_.store(false);
      worker_cv_.notify_one();
      return;
    }
    case Phase::WAIT_PENDING:
      // Drain already in flight/queued; IncrementalWorkerLoop flips this
      // back to REST when done.
      return;
  }
}

void CPRManager::ProtectStableCopy(DataItem* item, EpochNumber current_epoch) {
  if (!IsCheckpoint()) return;
  const auto phase = phase_.load(std::memory_order_acquire);
  if (phase == Phase::REST) return;
  const auto version = version_.load(std::memory_order_acquire);
  if (current_epoch < version) return;
  if (item->stable_version.load(std::memory_order_acquire) == version) return;
  item->stable_buffer.Reset(item->buffer);
  item->stable_buffer_initialized = item->IsInitialized();
  item->stable_version.store(version, std::memory_order_release);
}

void CPRManager::PrecommitWriteSet(const WriteSetType& write_set,
                                   EpochNumber current_epoch) {
  if (!IsCheckpoint() || write_set.empty()) return;
  has_dirty_writes_.store(true, std::memory_order_relaxed);

  // Silo/SiloNWR only: item->buffer isn't mutated until after this call (see
  // silo_nwr.hpp's Precommit()), so this is the right place to take the
  // stable copy. 2PL already took it earlier, from Write() itself (it
  // mutates item->buffer immediately) -- protecting again here could, if a
  // round starts in between, capture 2PL's own new value instead of the
  // pre-round one.
  if (config_.concurrency_control_protocol !=
      Config::ConcurrencyControl::TwoPhaseLocking) {
    for (auto& snapshot : write_set) {
      ProtectStableCopy(snapshot.index_cache, current_epoch);
    }
  }

  auto* ds = tls_.Get();
  for (auto& snapshot : write_set) {
    auto* item = snapshot.index_cache;
    if (item->dirty_epoch.load(std::memory_order_acquire) < current_epoch) {
      item->dirty_epoch.store(current_epoch, std::memory_order_release);
      // Lock-free push (Treiber stack): this thread is the sole producer
      // for `ds`, so it never dereferences a stale `next`, only compares
      // it -- a failed CAS (e.g. because the worker concurrently drained
      // the stack via exchange()) simply retries against the new head.
      auto* node =
          new DirtyEntry{snapshot.table_name, snapshot.key, item, nullptr};
      DirtyEntry* old_head = ds->head.load(std::memory_order_relaxed);
      do {
        node->next = old_head;
      } while (!ds->head.compare_exchange_weak(old_head, node,
                                               std::memory_order_release,
                                               std::memory_order_relaxed));
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
    std::string_view prefix, EpochNumber max_epoch) const {
  namespace fs = std::filesystem;
  std::vector<std::pair<EpochNumber, std::string>> result;
  if (!fs::exists(config_.work_dir)) return result;
  for (const auto& entry : fs::directory_iterator(config_.work_dir)) {
    const std::string name = entry.path().filename().string();
    if (name.rfind(prefix, 0) != 0 || name.find(".log") == std::string::npos ||
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

  // Two phases, deliberately kept separate: (1) detach every per-thread
  // DirtySet's chain and dedup by item identity *globally*, not per thread
  // -- two threads can each hold an undrained entry for the same item, and
  // recovery's tid-tie-break never overwrites, so a per-thread dedup could
  // let both win arbitrarily. (2) Only after every chain is detached, read
  // each deduped item's buffer once -- reading eagerly per thread could read
  // a value that a later-visited thread's still-undrained write should have
  // superseded.
  std::unordered_map<DataItem*, DirtyEntry*> deduped;
  std::vector<std::unique_ptr<DirtyEntry>> owned_entries;
  tls_.ForEach([&](DirtySet* ds) {
    // Atomically detach the entire chain of writes accumulated since the
    // last drain. The release sequence on head (see PrecommitWriteSet's
    // CAS) means this acquire sees every prior push's writes to the
    // detached nodes; any push racing with this exchange either lands in
    // the detached chain or starts a fresh chain from nullptr afterward --
    // never both, and never a partially-published node.
    DirtyEntry* chain = ds->head.exchange(nullptr, std::memory_order_acquire);
    while (chain != nullptr) {
      owned_entries.emplace_back(chain);
      chain = chain->next;
      DirtyEntry* entry = owned_entries.back().get();
      deduped[entry->item] = entry;  // last-seen wins; only identity matters
    }
  });

  const bool is_2pl = config_.concurrency_control_protocol ==
                      LineairDB::Config::ConcurrencyControl::TwoPhaseLocking;
  for (auto& [item, entry] : deduped) {
    if (is_2pl) {
      item->ExclusiveLock2PL();
    } else {
      item->ExclusiveLockSilo();
    }
    Logger::LogRecord::KeyValuePair kvp;
    kvp.table_name = entry->table_name;
    kvp.key = entry->key;
    // If this item has a stable copy taken for exactly this round (see
    // PrecommitWriteSet), emit that before-image instead of the live buffer
    // -- something wrote it at or after version_ == cp_epoch, so the live
    // value belongs to a *future* round's snapshot, not this one.
    //
    // Excluded at shutdown: cp_epoch can coincidentally equal an
    // in-progress round's version_ there, and there's no future round left
    // to defer to, so always emit the live value instead.
    bool used_stable_snapshot = false;
    if (!is_shutdown &&
        item->stable_version.load(std::memory_order_acquire) == cp_epoch) {
      kvp.buffer =
          item->stable_buffer_initialized ? item->stable_buffer.toString() : "";
      item->stable_version.store(0, std::memory_order_release);
      used_stable_snapshot = true;
    } else if (!item->IsInitialized()) {
      kvp.buffer = "";
    } else {
      kvp.buffer = item->buffer.toString();
    }
    kvp.tid = {cp_epoch, 0};
    // Reset here, under the item's lock: a write that finds dirty_epoch
    // already current skips pushing a new entry, trusting this drain to
    // read its value live. Leaving dirty_epoch stale after the drain would
    // let a later write silently skip tracking too, with nothing left to
    // prompt a future drain to revisit it.
    item->dirty_epoch.store(0, std::memory_order_release);
    if (used_stable_snapshot) {
      // We emitted the protected pre-round value, not the live buffer, so
      // any write that combined into this entry (skipped its own push,
      // above) had its result stranded -- this entry is consumed and gone.
      // Re-push a fresh one so the next drain picks up whatever the live
      // buffer holds by then.
      auto* mds = tls_.Get();
      auto* node = new DirtyEntry{entry->table_name, entry->key, item, nullptr};
      DirtyEntry* old_head = mds->head.load(std::memory_order_relaxed);
      do {
        node->next = old_head;
      } while (!mds->head.compare_exchange_weak(old_head, node,
                                                std::memory_order_release,
                                                std::memory_order_relaxed));
    }
    record.key_value_pairs.emplace_back(std::move(kvp));
    if (is_2pl) {
      item->ExclusiveUnlock2PL();
    } else {
      item->ExclusiveUnlockSilo();
    }
  }

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

  // Both resets must happen before FlushDurableEpoch makes durable_epoch_'s
  // new value observable: otherwise a caller that polls
  // GetCheckpointCompletedEpoch() and immediately starts a new round could
  // see phase_ still at WAIT_PENDING (a silent no-op) or clobber the new
  // round's dirty_stable_empty_ back to true, losing its wakeup.
  dirty_stable_empty_.store(true, std::memory_order_release);
  phase_.store(Phase::REST, std::memory_order_release);

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
      worker_cv_.wait_for(lock, std::chrono::seconds(1), [&] {
        return stop_.load() || !dirty_stable_empty_.load();
      });
    }
    if (stop_.load()) break;

    if (dirty_stable_empty_.load()) {
      continue;
    }

    EpochNumber cp_epoch = version_.load();
    // dirty_stable_empty_ and phase_ are reset inside WriteDeltaCheckpoint,
    // not here (see its comment).
    WriteDeltaCheckpoint(cp_epoch, false);
  }
  WriteRemainingDirtyEntries();
}

void CPRManager::WriteRemainingDirtyEntries() {
  // Final drain on shutdown -- tag it with the live epoch. Any stable copy
  // from an in-progress round is left orphaned; WriteDeltaCheckpoint's
  // is_shutdown=true path always reads each item's live value instead.
  EpochNumber cp_epoch = epoch_manager_ref_.GetGlobalEpoch();
  WriteDeltaCheckpoint(cp_epoch, true);
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
        if (!inserted &&
            (it->second->key.empty() || it->second->tid.epoch < kvp.tid.epoch ||
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

}  // namespace Recovery
}  // namespace LineairDB
