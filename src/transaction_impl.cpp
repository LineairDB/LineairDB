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

#include "transaction_impl.h"

#include <lineairdb/transaction.h>

#include <memory>
#include <set>
#include <utility>

#include "concurrency_control/concurrency_control_base.h"
#include "concurrency_control/impl/silo_nwr.hpp"
#include "concurrency_control/impl/two_phase_locking.hpp"
#include "database_impl.h"
#include "types/snapshot.hpp"

namespace LineairDB {

namespace {
thread_local void* current_transaction_context = nullptr;
}

void* GetCurrentTransactionContext() { return current_transaction_context; }

Transaction::Impl::Impl(Database::Impl* db_pimpl) noexcept
    : current_status_(TxStatus::Running),
      db_pimpl_(db_pimpl),
      config_ref_(db_pimpl_->GetConfig()),
      current_table_(nullptr) {
  current_transaction_context = this;

  TransactionReferences&& tx = {read_set_, write_set_,
                                db_pimpl_->epoch_framework_, current_status_};

  // WANTFIX for performance
  // Here we allocate one (derived) concurrency control instance per
  // transactions. It may be worse on performance because of heap
  // memory allocation. Need to re-implement with composition or templates.
  switch (config_ref_.concurrency_control_protocol) {
    case Config::ConcurrencyControl::SiloNWR:
      concurrency_control_ = std::make_unique<ConcurrencyControl::SiloNWR>(
          std::forward<TransactionReferences>(tx));
      break;
    case Config::ConcurrencyControl::Silo:
      concurrency_control_ = std::make_unique<ConcurrencyControl::Silo>(
          std::forward<TransactionReferences>(tx));
      break;
    case Config::ConcurrencyControl::TwoPhaseLocking:
      concurrency_control_ =
          std::make_unique<ConcurrencyControl::TwoPhaseLocking>(
              std::forward<TransactionReferences>(tx));
      break;

    default:
      concurrency_control_ = std::make_unique<ConcurrencyControl::SiloNWR>(
          std::forward<TransactionReferences>(tx));

      break;
  }
}

Transaction::Impl::~Impl() noexcept { current_transaction_context = nullptr; }

TxStatus Transaction::Impl::GetCurrentStatus() { return current_status_; }

const std::pair<const std::byte* const, const size_t> Transaction::Impl::Read(
    const std::string_view key) {
  if (IsAborted()) return {nullptr, 0};

  EnsureCurrentTable();

  for (auto& snapshot : write_set_) {
    if (snapshot.key == key &&
        snapshot.table_name == current_table_->GetTableName()) {
      return std::make_pair(snapshot.data_item_copy.value(),
                            snapshot.data_item_copy.size());
    }
  }

  for (auto& snapshot : read_set_) {
    if (snapshot.key == key &&
        snapshot.table_name == current_table_->GetTableName()) {
      return std::make_pair(snapshot.data_item_copy.value(),
                            snapshot.data_item_copy.size());
    }
  }

  auto* index_leaf = current_table_->GetPrimaryIndex().GetOrInsert(key);
  Snapshot snapshot = {key, nullptr, 0, index_leaf,
                       current_table_->GetTableName()};

  snapshot.data_item_copy = concurrency_control_->Read(key, index_leaf);
  auto& ref = read_set_.emplace_back(std::move(snapshot));
  if (ref.data_item_copy.IsInitialized()) {
    return {ref.data_item_copy.value(), ref.data_item_copy.size()};
  } else {
    return {nullptr, 0};
  }
}

void Transaction::Impl::Insert(const std::string_view key,
                               const std::byte value[], const size_t size) {
  if (IsAborted()) return;
  EnsureCurrentTable();

  auto inserted = current_table_->GetPrimaryIndex().Insert(key);
  if (!inserted) {
    Abort();
    return;
  }

  bool is_rmf = false;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == key &&
        snapshot.table_name == current_table_->GetTableName()) {
      is_rmf = true;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  for (auto& snapshot : write_set_) {
    if (snapshot.key != key ||
        snapshot.table_name != current_table_->GetTableName())
      continue;
    snapshot.data_item_copy.Reset(value, size);
    if (is_rmf) snapshot.is_read_modify_write = true;
    return;
  }

  auto* index_leaf = current_table_->GetPrimaryIndex().Get(key);

  concurrency_control_->Write(key, value, size, index_leaf);
  Snapshot sp(key, value, size, index_leaf, current_table_->GetTableName());
  if (is_rmf) sp.is_read_modify_write = true;
  write_set_.emplace_back(std::move(sp));
}

void Transaction::Impl::Update(const std::string_view key,
                               const std::byte value[], const size_t size) {
  if (IsAborted()) return;

  // TODO: if `size` is larger than Config.internal_buffer_size,
  // then we have to abort this transaction or throw exception
  EnsureCurrentTable();

  bool is_rmf = false;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == key &&
        snapshot.table_name == current_table_->GetTableName()) {
      is_rmf = true;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  for (auto& snapshot : write_set_) {
    if (snapshot.key != key ||
        snapshot.table_name != current_table_->GetTableName())
      continue;
    snapshot.data_item_copy.Reset(value, size);
    if (is_rmf) snapshot.is_read_modify_write = true;
    return;
  }

  auto* index_leaf = current_table_->GetPrimaryIndex().Get(key);
  if (index_leaf == nullptr || !index_leaf->IsInitialized()) {
    Abort();
    return;
  }

  concurrency_control_->Write(key, value, size, index_leaf);
  Snapshot sp(key, value, size, index_leaf, current_table_->GetTableName());
  if (is_rmf) sp.is_read_modify_write = true;
  write_set_.emplace_back(std::move(sp));
}

void Transaction::Impl::Delete(const std::string_view key) {
  if (IsAborted()) return;
  EnsureCurrentTable();
  // Delete() consists of two deletions: removal from the index (physical)
  //   and initialization of the data item (logical).
  // The reason for this design is that we consider Delete() as
  //   a combination of two writes: a write to the index and a write to the data
  //   item.
  bool deleted = current_table_->GetPrimaryIndex().Delete(key);
  if (!deleted) {
    Abort();
    return;
  }
  this->Update(key, nullptr, 0);
}

const std::optional<size_t> Transaction::Impl::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  EnsureCurrentTable();

  // Note: In this Scan implementation, nullptr indicates that the key is
  // deleted or does not exist. SQL NULL values should be handled within the
  // byte array value, not by nullptr.

  // Step 1: Collect keys from index
  std::set<std::string> index_keys;
  auto index_result = current_table_->GetPrimaryIndex().Scan(
      begin, end, [&](std::string_view key) {
        index_keys.insert(std::string(key));
        return false;  // Continue to collect all keys
      });

  if (!index_result.has_value()) {
    Abort();
    return std::nullopt;
  }

  // Step 2: Collect keys from write_set
  std::set<std::string> write_set_keys;
  for (const auto& snapshot : write_set_) {
    if (snapshot.table_name != current_table_->GetTableName()) continue;
    if (snapshot.key < begin) continue;
    if (end.has_value() && snapshot.key > end.value()) continue;
    write_set_keys.insert(snapshot.key);
  }

  // Step 3: Merge and sort all keys (std::set automatically keeps them sorted)
  std::set<std::string> all_keys;
  all_keys.insert(index_keys.begin(), index_keys.end());
  all_keys.insert(write_set_keys.begin(), write_set_keys.end());

  // Step 4: Process keys in sorted order
  size_t total_count = 0;
  for (const auto& key : all_keys) {
    if (IsAborted()) return std::nullopt;

    // Check if key is in write_set
    // if the key exists, use write_set data (without Transaction#Read)
    bool found_in_write_set = false;
    for (const auto& snapshot : write_set_) {
      if (snapshot.table_name != current_table_->GetTableName()) continue;
      if (snapshot.key != key) continue;

      found_in_write_set = true;

      // If the key is deleted within this transaction, break the loop
      if (!snapshot.data_item_copy.IsInitialized()) {
        break;
      }

      std::pair<const void*, const size_t> value_pair = {
          snapshot.data_item_copy.value(), snapshot.data_item_copy.size()};
      bool stop_scan = operation(key, value_pair);
      total_count++;
      if (stop_scan) return total_count;

      break;
    }

    // If not in write_set, invoke Transaction#Read to get the value
    if (!found_in_write_set) {
      const auto read_result = Read(key);
      // The pair "nullptr, 0" means deleted (or uninitialized) data. See
      // include/lineairdb/transaction.h
      const bool is_uninitialized =
          read_result.first == nullptr && read_result.second == 0;

      // If the data item exists, continue scanning with the data
      if (!is_uninitialized) {
        bool stop_scan = operation(key, read_result);
        total_count++;
        if (stop_scan) return total_count;
      }
    }
  }

  // TODO: we now only consider the insertion, but we should consider the case
  // for deletions in write_set, when the lineairdb supports delete operation as
  // the public interface of transaction.h.

  return total_count;
};

void Transaction::Impl::Abort() {
  if (!IsAborted()) {
    current_status_ = TxStatus::Aborted;
    concurrency_control_->Abort();
    concurrency_control_->PostProcessing(TxStatus::Aborted);
  }
}
bool Transaction::Impl::Precommit() {
  if (IsAborted()) return false;

  const bool need_to_checkpoint =
      (db_pimpl_->GetConfig().enable_checkpointing &&
       db_pimpl_->IsNeedToCheckpointing(
           db_pimpl_->epoch_framework_.GetMyThreadLocalEpoch()));
  bool committed = concurrency_control_->Precommit(need_to_checkpoint);
  return committed;
}

void Transaction::Impl::PostProcessing(TxStatus status) {
  if (status == TxStatus::Aborted) current_status_ = TxStatus::Aborted;
  concurrency_control_->PostProcessing(status);
}

void Transaction::Impl::EnsureCurrentTable() {
  if (current_table_ == nullptr) {
    current_table_ =
        db_pimpl_->GetTable(config_ref_.anonymous_table_name).value();
  }
}

bool Transaction::Impl::SetTable(const std::string_view table_name) {
  auto table = db_pimpl_->GetTable(table_name);
  if (!table.has_value()) {
    return false;  // Table not found
  }
  current_table_ = table.value();
  return true;
}

TxStatus Transaction::GetCurrentStatus() {
  return tx_pimpl_->GetCurrentStatus();
}
const std::pair<const std::byte* const, const size_t> Transaction::Read(
    const std::string_view key) {
  return tx_pimpl_->Read(key);
}
/* void Transaction::Write(const std::string_view key, const std::byte value[],
                        const size_t size) {
  tx_pimpl_->Write(key, value, size);
} */
void Transaction::Insert(const std::string_view key, const std::byte value[],
                         const size_t size) {
  tx_pimpl_->Insert(key, value, size);
}
void Transaction::Update(const std::string_view key, const std::byte value[],
                         const size_t size) {
  tx_pimpl_->Update(key, value, size);
}
void Transaction::Delete(const std::string_view key) { tx_pimpl_->Delete(key); }
const std::optional<size_t> Transaction::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  return tx_pimpl_->Scan(begin, end, operation);
};
void Transaction::Abort() { tx_pimpl_->Abort(); }
bool Transaction::SetTable(const std::string_view table_name) {
  return tx_pimpl_->SetTable(table_name);
}
bool Transaction::Precommit() { return tx_pimpl_->Precommit(); }

Transaction::Transaction(void* db_pimpl) noexcept
    : tx_pimpl_(
          std::make_unique<Impl>(reinterpret_cast<Database::Impl*>(db_pimpl))) {
}
Transaction::~Transaction() noexcept = default;

}  // namespace LineairDB
