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

#ifndef LINEAIRDB_CHECKPOINT_MANAGER_HPP
#define LINEAIRDB_CHECKPOINT_MANAGER_HPP

#include <lineairdb/config.h>
#include <lineairdb/transaction.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "types/definitions.h"
#include "types/snapshot.hpp"
#include "util/thread_key_storage.h"

namespace LineairDB {

class TableDictionary;
class EpochFramework;
class DataItem;

namespace Recovery {

/**
 * @brief CPRManager: Consistent Prefix Recovery via Incremental Delta
 * Checkpointing
 */
class CPRManager {
  struct DirtyEntry {
    std::string table_name;
    std::string key;
    DataItem* item;
  };
  struct TableKey {
    std::string table_name;
    std::string key;
    bool operator==(const TableKey& o) const {
      return table_name == o.table_name && key == o.key;
    }
    struct Hash {
      size_t operator()(const TableKey& k) const {
        return std::hash<std::string>{}(k.table_name) ^
               (std::hash<std::string>{}(k.key) << 1);
      }
    };
  };
  struct DirtySet {
    std::vector<DirtyEntry> bufs[2];
    std::atomic<size_t> active_idx{0};
    std::vector<DirtyEntry>* dirty_stable{nullptr};

    DirtySet() {
      bufs[0].reserve(4096);
      bufs[1].reserve(4096);
    }
  };

 public:
  CPRManager(const Config& config, TableDictionary& dict,
             EpochFramework& epoch);
  ~CPRManager();

  void Stop();

  // --- Upstream compatibility API ---
  bool IsCheckpoint() const {
    return config_.durability == Config::DurabilityStrategy::Checkpoint ||
           config_.durability == Config::DurabilityStrategy::CheckpointAndWAL;
  }
  bool IsWAL() const {
    return config_.durability == Config::DurabilityStrategy::WAL ||
           config_.durability == Config::DurabilityStrategy::CheckpointAndWAL;
  }
  bool IsNone() const {
    return config_.durability == Config::DurabilityStrategy::None;
  }

  bool IsNeedToCheckpointing(const EpochNumber /*epoch*/) const {
    return false;
  }

  EpochNumber GetCheckpointCompletedEpoch();

  void RotateDirtySets(EpochNumber next_checkpoint_epoch);

  void OnPrecommit(const WriteSetType& write_set, EpochNumber current_epoch) {
    PrecommitWriteSet(write_set, current_epoch);
  }

  void OnAbort(const WriteSetType& write_set) { AbortWriteSet(write_set); }

  void PrecommitWriteSet(const WriteSetType& write_set,
                         EpochNumber current_epoch);
  void AbortWriteSet(const WriteSetType& write_set);

  // --- Recovery ---
  EpochNumber GetDurableEpoch() const;
  EpochNumber RecoverDurableEpoch();
  WriteSetType GetRecoverySetFromLogs();

 private:
  std::vector<std::pair<EpochNumber, std::string>> FindLogFiles(
      std::string_view prefix,
      EpochNumber max_epoch = std::numeric_limits<EpochNumber>::max()) const;

  std::vector<std::pair<EpochNumber, std::string>> FindDeltaFiles(
      EpochNumber max_epoch = std::numeric_limits<EpochNumber>::max()) const;

  std::pair<EpochNumber, std::string> FindLatestBase() const;

  void WriteDeltaCheckpoint(EpochNumber cp_epoch, bool is_shutdown = false);
  void FlushDurableEpoch(EpochNumber cp_epoch, bool is_shutdown);

  void IncrementalWorkerLoop();
  void WriteRemainingDirtyEntries();
  void ForceRotateBuffers();

  void CompactorLoop();
  void RunCompaction();

  const Config& config_;
  EpochFramework& epoch_manager_ref_;
  ThreadKeyStorage<DirtySet> tls_;
  std::atomic<bool> dirty_stable_empty_;
  std::mutex worker_latch_;
  std::condition_variable worker_cv_;
  std::atomic<size_t> incremental_file_count_{0};
  std::mutex compactor_latch_;
  std::condition_variable compactor_cv_;
  std::atomic<bool> compaction_requested_{false};
  std::atomic<EpochNumber> checkpoint_epoch_;
  std::atomic<EpochNumber> durable_epoch_;
  std::atomic<bool> stop_;
  std::thread manager_thread_;
  std::thread compactor_thread_;

  std::atomic<bool> has_dirty_writes_;

  static constexpr std::string_view kDeltaFilePrefix = "cpr_delta_";
  static constexpr std::string_view kBaseFilePrefix = "cpr_base_";
  static constexpr size_t kCompactionThreshold = 5;

  const std::string dep_file_;
  const std::string dep_working_;
};

using CheckpointManager = CPRManager;

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_CHECKPOINT_MANAGER_HPP */
