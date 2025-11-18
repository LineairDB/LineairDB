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

#include <algorithm>
#include <iostream>
#include <memory>
#include <utility>

#include "concurrency_control/concurrency_control_base.h"
#include "concurrency_control/impl/silo_nwr.hpp"
#include "concurrency_control/impl/two_phase_locking.hpp"
#include "database_impl.h"
#include "types/snapshot.hpp"

// ---- Private helpers for SecondaryIndex operations ----
namespace {
inline std::string BuildQualifiedSKKey(std::string_view table_name,
                                       std::string_view index_name,
                                       std::string_view serialized_key) {
  std::string k;
  k.reserve(table_name.size() + index_name.size() + serialized_key.size() + 2);
  k.append(table_name);
  k.push_back('#');
  k.append(index_name);
  k.push_back('#');
  k.append(serialized_key);
  return k;
}
}  // namespace

namespace LineairDB {

Transaction::Impl::Impl(Database::Impl* db_pimpl) noexcept
    : current_status_(TxStatus::Running),
      db_pimpl_(db_pimpl),
      config_ref_(db_pimpl_->GetConfig()),
      current_table_(nullptr) {
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

Transaction::Impl::~Impl() noexcept = default;

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
  Snapshot snapshot = {
      key, nullptr, 0, index_leaf, current_table_->GetTableName(), ""};

  snapshot.data_item_copy = concurrency_control_->Read(key, index_leaf);
  auto& ref = read_set_.emplace_back(std::move(snapshot));
  if (ref.data_item_copy.IsInitialized()) {
    return {ref.data_item_copy.value(), ref.data_item_copy.size()};
  } else {
    return {nullptr, 0};
  }
}

std::vector<std::pair<const std::byte* const, const size_t>>
Transaction::Impl::ReadSecondaryIndex(const std::string_view index_name,
                                      const std::string_view key) {
  if (IsAborted()) return {};
  Index::SecondaryIndex* index = current_table_->GetSecondaryIndex(index_name);

  if (index == nullptr) {
    Abort();
    return {};
  }

  EnsureCurrentTable();

  for (auto& snapshot : write_set_) {
    if (snapshot.key == key &&
        snapshot.table_name == current_table_->GetTableName() &&
        snapshot.index_name == index_name) {
      std::vector<std::pair<const std::byte* const, const size_t>> result;
      if (!snapshot.data_item_copy.primary_keys.empty()) {
        for (auto& primary_key : snapshot.data_item_copy.primary_keys) {
          result.emplace_back(reinterpret_cast<const std::byte*>(primary_key.data()),
                              primary_key.size());
        }
      }
      return result;
    }
  }

  for (auto& snapshot : read_set_) {
    if (snapshot.key == key &&
        snapshot.table_name == current_table_->GetTableName() &&
        snapshot.index_name == index_name) {
      std::vector<std::pair<const std::byte* const, const size_t>> result;
      if (!snapshot.data_item_copy.primary_keys.empty()) {
        for (auto& primary_key : snapshot.data_item_copy.primary_keys) {
          result.emplace_back(reinterpret_cast<const std::byte*>(primary_key.data()),
                              primary_key.size());
        }
      }
      return result;
    }
  }

  DataItem* index_leaf = index->GetOrInsert(key);

  Snapshot snapshot = {
      key, nullptr, 0, index_leaf, current_table_->GetTableName(), index_name};

  snapshot.data_item_copy = concurrency_control_->Read(key, index_leaf);
  auto& ref = read_set_.emplace_back(std::move(snapshot));
  if (ref.data_item_copy.IsInitialized()) {
    std::vector<std::pair<const std::byte* const, const size_t>> result;
    if (!ref.data_item_copy.primary_keys.empty()) {
      for (auto& primary_key : ref.data_item_copy.primary_keys) {
        result.emplace_back(reinterpret_cast<const std::byte*>(primary_key.data()),
                            primary_key.size());
      }
      return result;
    } else {
      return {};
    }
  }
  return {};
}

void Transaction::Impl::Write(const std::string_view key,
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

  auto* index_leaf = current_table_->GetPrimaryIndex().GetOrInsert(key);

  concurrency_control_->Write(key, value, size, index_leaf);
  Snapshot sp(key, value, size, index_leaf, current_table_->GetTableName(), "");
  if (is_rmf) sp.is_read_modify_write = true;
  write_set_.emplace_back(std::move(sp));
}

void Transaction::Impl::WriteSecondaryIndex(
    const std::string_view index_name, const std::string_view key,
    const std::byte primary_key_buffer[], const size_t primary_key_size) {
  if (IsAborted()) return;

  EnsureCurrentTable();

  // TODO: if `size` is larger than Config.internal_buffer_size,
  // then we have to abort this transaction or throw exception

  Index::SecondaryIndex* index = current_table_->GetSecondaryIndex(index_name);

  // If the index is not registered, abort the transaction
  if (index == nullptr) {
    Abort();
    return;
  }

  // existing key
  // unique constraint check out of the transaction
  DataItem* index_leaf = index->GetOrInsert(key);
  if (index_leaf->IsInitialized() && index->IsUnique()) {
    Abort();
    return;
  }

  bool is_rmf = false;
  DataItem existing_data;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == key &&
        snapshot.table_name == current_table_->GetTableName() &&
        snapshot.index_name == index_name) {
      is_rmf = true;
      existing_data = snapshot.data_item_copy;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  // unique constraint check in the transaction
  for (auto& snapshot : write_set_) {
    if (snapshot.key != key ||
        snapshot.table_name != current_table_->GetTableName() ||
        snapshot.index_name != index_name)
      continue;
    if (index->IsUnique()) {
      Abort();
      return;
    }

    snapshot.data_item_copy.AddSecondaryIndexValue(primary_key_buffer,
                                                   primary_key_size);
    if (is_rmf) snapshot.is_read_modify_write = true;
    return;
  }

  if (!is_rmf) {
    Snapshot snapshot = {
        key,       nullptr, 0, index_leaf, current_table_->GetTableName(),
        index_name};
    snapshot.data_item_copy = concurrency_control_->Read(key, index_leaf);
    snapshot.is_read_modify_write = true;
    read_set_.emplace_back(std::move(snapshot));
    existing_data = snapshot.data_item_copy;
    is_rmf = true;
  }

  concurrency_control_->Write(key, primary_key_buffer, primary_key_size,
                              index_leaf);
  Snapshot sp(key, nullptr, 0, index_leaf, current_table_->GetTableName(),
              index_name);

  if (is_rmf) sp.is_read_modify_write = true;
  sp.data_item_copy = existing_data;
  sp.data_item_copy.AddSecondaryIndexValue(primary_key_buffer,
                                           primary_key_size);

  write_set_.emplace_back(std::move(sp));
}

const std::optional<size_t> Transaction::Impl::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  EnsureCurrentTable();
  auto result = current_table_->GetPrimaryIndex().Scan(
      begin, end, [&](std::string_view key) {
        const auto read_result = Read(key);
        if (IsAborted()) return true;
        return operation(key, read_result);
      });
  if (!result.has_value()) {
    Abort();
  }
  return result;
};

const std::optional<size_t> Transaction::Impl::ScanSecondaryIndex(
    const std::string_view index_name, const std::string_view begin,
    const std::optional<std::string_view> end,
    std::function<bool(std::string_view, const std::vector<std::string>)>
        operation) {
  EnsureCurrentTable();
  Index::SecondaryIndex* index = current_table_->GetSecondaryIndex(index_name);

  if (index == nullptr) {
    Abort();
    return {};
  }

  auto result = index->Scan(begin, end, [&](std::string_view key) {
    const auto read_result = ReadSecondaryIndex(index_name, key);
    if (IsAborted()) return true;
    std::vector<std::string> primary_keys;
    primary_keys.reserve(read_result.size());
    for (const auto& sec_idx_buffer : read_result) {
      primary_keys.emplace_back(
          reinterpret_cast<const char*>(sec_idx_buffer.first),
          sec_idx_buffer.second);
    }
    return operation(key, primary_keys);
  });
  if (!result.has_value()) {
    Abort();
  }
  return result;
};

bool Transaction::Impl::Delete(const std::string_view key) {
  if (IsAborted()) return false;
  EnsureCurrentTable();
  if (!current_table_->GetPrimaryIndex().Delete(key)) {
    return false;
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
    snapshot.data_item_copy.Reset(nullptr, 0);
    if (is_rmf) snapshot.is_read_modify_write = true;
    return true;
  }

  auto* index_leaf = current_table_->GetPrimaryIndex().GetOrInsert(key);

  concurrency_control_->Write(key, nullptr, 0, index_leaf);
  Snapshot sp(key, nullptr, 0, index_leaf, current_table_->GetTableName(), "");
  if (is_rmf) sp.is_read_modify_write = true;
  write_set_.emplace_back(std::move(sp));

  return true;
}

void Transaction::Impl::DeleteSecondaryIndex(
    const std::string_view index_name, const std::string_view secondary_key,
    const std::byte primary_key_buffer[], const size_t primary_key_size) {
  if (IsAborted()) return;

  EnsureCurrentTable();

  Index::SecondaryIndex* index = current_table_->GetSecondaryIndex(index_name);
  if (index == nullptr) {
    Abort();
    return;
  }

  // delete the primary key from the data item associated
  // with the old secondary key ==========
  auto index_leaf = index->GetOrInsert(secondary_key);
  bool found_in_write_set = false;

  bool is_rmf = false;
  DataItem existing_data;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == secondary_key &&
        snapshot.table_name == current_table_->GetTableName() &&
        snapshot.index_name == index_name) {
      is_rmf = true;
      existing_data = snapshot.data_item_copy;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  // case A: old_key is in write_set
  for (auto& snapshot : write_set_) {
    if (snapshot.key != secondary_key ||
        snapshot.table_name != current_table_->GetTableName() ||
        snapshot.index_name != index_name)
      continue;

    found_in_write_set = true;
    snapshot.data_item_copy.RemoveSecondaryIndexValue(primary_key_buffer,
                                                      primary_key_size);
    if (is_rmf) snapshot.is_read_modify_write = true;
    break;
  }

  // case B: old_key is not in write_set
  if (!found_in_write_set) {
    if (!is_rmf) {
      Snapshot snapshot = {
          secondary_key, nullptr, 0, index_leaf, current_table_->GetTableName(),
          index_name};

      snapshot.data_item_copy =
          concurrency_control_->Read(secondary_key, index_leaf);
      existing_data = snapshot.data_item_copy;
      read_set_.emplace_back(std::move(snapshot));
    }
    existing_data.RemoveSecondaryIndexValue(primary_key_buffer,
                                            primary_key_size);

    Snapshot sp(secondary_key, nullptr, 0, index_leaf,
                current_table_->GetTableName(), index_name);
    sp.data_item_copy = existing_data;
    if (is_rmf) sp.is_read_modify_write = true;
    write_set_.emplace_back(std::move(sp));
  }
}

void Transaction::Impl::UpdateSecondaryIndex(
    const std::string_view index_name, const std::string_view old_secondary_key,
    const std::string_view new_secondary_key,
    const std::byte primary_key_buffer[], const size_t primary_key_size) {
  if (IsAborted()) return;

  EnsureCurrentTable();

  Index::SecondaryIndex* index = current_table_->GetSecondaryIndex(index_name);
  if (index == nullptr) {
    Abort();
    return;
  }

  // ========== Phase 1: delete the primary key from the data item
  auto old_leaf = index->GetOrInsert(old_secondary_key);
  bool old_found_in_write_set = false;

  bool is_rmf_old_key = false;
  DataItem existing_data_old_key;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == old_secondary_key &&
        snapshot.table_name == current_table_->GetTableName() &&
        snapshot.index_name == index_name) {
      is_rmf_old_key = true;
      existing_data_old_key = snapshot.data_item_copy;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  // case A: old_key is in write_set
  for (auto& snapshot : write_set_) {
    if (snapshot.key != old_secondary_key ||
        snapshot.table_name != current_table_->GetTableName() ||
        snapshot.index_name != index_name)
      continue;

    old_found_in_write_set = true;
    snapshot.data_item_copy.RemoveSecondaryIndexValue(primary_key_buffer,
                                                      primary_key_size);
    if (is_rmf_old_key) snapshot.is_read_modify_write = true;
    break;
  }

  // case B: old_key is not in write_set
  if (!old_found_in_write_set) {
    if (!is_rmf_old_key) {
      Snapshot snapshot = {old_secondary_key,
                           nullptr,
                           0,
                           old_leaf,
                           current_table_->GetTableName(),
                           index_name};

      snapshot.data_item_copy =
          concurrency_control_->Read(old_secondary_key, old_leaf);
      existing_data_old_key = snapshot.data_item_copy;
      read_set_.emplace_back(std::move(snapshot));
    }
    existing_data_old_key.RemoveSecondaryIndexValue(primary_key_buffer,
                                                    primary_key_size);
    concurrency_control_->Write(old_secondary_key, primary_key_buffer, primary_key_size, old_leaf);
    Snapshot sp(old_secondary_key, nullptr, 0, old_leaf,
                current_table_->GetTableName(), index_name);
    sp.data_item_copy = existing_data_old_key;
    if (is_rmf_old_key) sp.is_read_modify_write = true;
    write_set_.emplace_back(std::move(sp));
  }

  // ========== Phase 2: add the primary key to the data item associated with
  // the new secondary key ==========
  auto new_leaf = index->GetOrInsert(new_secondary_key);
  bool new_found_in_write_set = false;

  bool is_rmf_new_key = false;
  DataItem existing_data_new_key;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == new_secondary_key &&
        snapshot.table_name == current_table_->GetTableName() &&
        snapshot.index_name == index_name) {
      is_rmf_new_key = true;
      snapshot.is_read_modify_write = true;
      existing_data_new_key = snapshot.data_item_copy;
      break;
    }
  }

  // case: new_key is in write_set
  for (auto& snapshot : write_set_) {
    if (snapshot.key != new_secondary_key ||
        snapshot.table_name != current_table_->GetTableName() ||
        snapshot.index_name != index_name)
      continue;

    new_found_in_write_set = true;
    snapshot.data_item_copy.AddSecondaryIndexValue(primary_key_buffer,
                                                   primary_key_size);
    if (is_rmf_new_key) snapshot.is_read_modify_write = true;
    break;
  }

  // case: new_key is not in write_set
  if (!new_found_in_write_set) {
    if (!is_rmf_new_key) {
      Snapshot snapshot = {new_secondary_key,
                           nullptr,
                           0,
                           new_leaf,
                           current_table_->GetTableName(),
                           index_name};

      snapshot.data_item_copy =
          concurrency_control_->Read(new_secondary_key, new_leaf);
      existing_data_new_key = snapshot.data_item_copy;
      read_set_.emplace_back(std::move(snapshot));
    }
    existing_data_new_key.AddSecondaryIndexValue(primary_key_buffer,
                                                 primary_key_size);
    concurrency_control_->Write(new_secondary_key, primary_key_buffer, primary_key_size, new_leaf);

    Snapshot sp(new_secondary_key, nullptr, 0, new_leaf,
                current_table_->GetTableName(), index_name);
    sp.data_item_copy = existing_data_new_key;
    if (is_rmf_new_key) sp.is_read_modify_write = true;

    write_set_.emplace_back(std::move(sp));
  }
}

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

std::vector<std::pair<const std::byte* const, const size_t>>
Transaction::ReadSecondaryIndex(const std::string_view index_name,
                                const std::string_view key) {
  return tx_pimpl_->ReadSecondaryIndex(index_name, key);
}

void Transaction::Write(const std::string_view key, const std::byte value[],
                        const size_t size) {
  tx_pimpl_->Write(key, value, size);
}

void Transaction::WriteSecondaryIndex(const std::string_view index_name,
                                      const std::string_view key,
                                      const std::byte primary_key_buffer[],
                                      const size_t primary_key_size) {
  tx_pimpl_->WriteSecondaryIndex(index_name, key, primary_key_buffer,
                                 primary_key_size);
}

bool Transaction::Delete(const std::string_view key) {
  return tx_pimpl_->Delete(key);
}

void Transaction::DeleteSecondaryIndex(const std::string_view index_name,
                                       const std::string_view secondary_key,
                                       const std::byte primary_key_buffer[],
                                       const size_t primary_key_size) {
  tx_pimpl_->DeleteSecondaryIndex(index_name, secondary_key, primary_key_buffer,
                                  primary_key_size);
}

void Transaction::UpdateSecondaryIndex(const std::string_view index_name,
                                       const std::string_view old_secondary_key,
                                       const std::string_view new_secondary_key,
                                       const std::byte primary_key_buffer[],
                                       const size_t primary_key_size) {
  tx_pimpl_->UpdateSecondaryIndex(index_name, old_secondary_key,
                                  new_secondary_key, primary_key_buffer,
                                  primary_key_size);
}

const std::optional<size_t> Transaction::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  return tx_pimpl_->Scan(begin, end, operation);
};

const std::optional<size_t> Transaction::ScanSecondaryIndex(
    const std::string_view index_name, const std::string_view begin,
    const std::optional<std::string_view> end,
    std::function<bool(std::string_view, const std::vector<std::string>)>
        operation) {
  return tx_pimpl_->ScanSecondaryIndex(index_name, begin, end, operation);
}

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
