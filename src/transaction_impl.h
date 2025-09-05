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

#ifndef LINEAIRDB_TRANSACTION_IMPL_H
#define LINEAIRDB_TRANSACTION_IMPL_H

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "concurrency_control/concurrency_control_base.h"
#include "table/table.h"
#include "types/definitions.h"

namespace LineairDB {

/**
 * @brief
 * Transaction::Impl controls users' requests of the four operation of the page
 * model, and delegate these requests to concurrency control protocols.
 * Note that all concurrency control protocols assumes the followings:
 *   - A transaction cannot issue the same type of operation (read or write) to
 *     the same data item.
 *   - write-after-read into the same data item is valid.
 *     but read-after-write is invalid.
 * It is the important parts of the definition of "schedule" in the theory of
 * transaction proocessing. This class do not handle any correctness such as
 * serializability, but handle the operations to satisfy these two assumptions.
 * To do this end, we implement the followings:
 *   - "read-your-own-writes" (read the version written by the callee
 *      transaction itself)
 *   - "repeatable read" (read the version which has been already read by the
 *      callee transaction itself)
 */
class Transaction::Impl {
  friend class Database::Impl;

 public:
  Impl(Database::Impl*) noexcept;
  ~Impl() noexcept;

  TxStatus GetCurrentStatus();
  /*     const std::pair<const std::byte* const, const size_t> Read(
          const std::string_view key);  */
  const std::pair<const std::byte* const, const size_t> Read(
      const std::string_view table_name, const std::string_view key);

  std::vector<std::string> ReadSecondaryIndex(const std::string_view table_name,
                                              const std::string_view index_name,
                                              const std::any& key);

  /*   void Write(const std::string_view key, const std::byte value[],
               const size_t size); */
  void Write(const std::string_view table_name, const std::string_view key,
             const std::byte value[], const size_t size);
  void WriteSecondaryIndex(const std::string_view table_name,
                           const std::string_view index_name,
                           const std::any& key,
                           const std::byte primary_key_buffer[],
                           const size_t primary_key_size);

  /*   const std::optional<size_t> Scan(
        const std::string_view begin, const std::optional<std::string_view> end,
        std::function<bool(std::string_view,
                           const std::pair<const void*, const size_t>)>
            operation); */

  const std::optional<size_t> Scan(
      const std::string_view table_name, const std::string_view begin,
      const std::optional<std::string_view> end,
      std::function<bool(std::string_view,
                         const std::pair<const void*, const size_t>)>
          operation);

  const std::optional<size_t> ScanSecondaryIndex(
      const std::string_view table_name, const std::string_view index_name,
      const std::any& begin, const std::any& end,
      std::function<bool(std::string_view, const std::vector<std::string>)>
          operation);

  void UpdateSecondaryIndex(const std::string_view table_name,
                            const std::string_view index_name,
                            const std::any& old_key, const std::any& new_key,
                            const std::byte primary_key_buffer[],
                            const size_t primary_key_size);

  bool ValidateSKNotNull();

  void Abort();
  bool Precommit();

  /**
   * We assume that #PostProcessing will be invoked after #Precommit().
   */
  void PostProcessing(TxStatus);

  bool SetTable(const std::string_view table_name);

 private:
  void EnsureCurrentTable();
  bool IsAborted() { return current_status_ == TxStatus::Aborted; };

  // --- helpers for secondary index operations ---
  bool FindWriteSnapshot(const std::string& qualified_key, Snapshot** out);
  std::vector<std::string> DecodeCurrentPKList(const std::string& qualified_key,
                                               DataItem* leaf);
  void WriteEncodedPKList(const std::string& qualified_key, DataItem* leaf,
                          const std::string& encoded_value, bool mark_rmf);
  void WriteEncodedPKList(Snapshot& existing_snapshot,
                          const std::string& encoded_value, bool mark_rmf);
  bool IsKeyInReadSet(const std::string& qualified_key) const;
  std::string EncodePKBytes(const std::vector<std::string>& list) const;

 private:
  TxStatus current_status_;
  Database::Impl* dbpimpl_;
  const Config& config_ref_;
  std::unique_ptr<ConcurrencyControlBase> concurrency_control_;

  ReadSetType read_set_;
  WriteSetType write_set_;
  struct NotNullProgress {
    size_t remainingWrites;
    std::unordered_set<std::string> satisfiedIndexNames;
  };
  // Tracks, per table and per primary key, how many NOT NULL secondary-key
  // writes are still required before commit can succeed.
  std::unordered_map<std::string,
                     std::unordered_map<std::string, NotNullProgress>>
      remainingNotNullSkWrites_;

  Table* current_table_;
};
}  // namespace LineairDB
#endif /* LINEAIRDB_TRANSACTION_IMPL_H */
