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

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <type_traits>
#include <utility>

#include "concurrency_control/concurrency_control_base.h"
#include "concurrency_control/impl/silo_nwr.hpp"
#include "concurrency_control/impl/two_phase_locking.hpp"
#include "database_impl.h"
#include "lineairdb/key_serializer.h"
#include "lineairdb/pklist_util.h"
#include "types/snapshot.hpp"

namespace LineairDB {

Transaction::Impl::Impl(Database::Impl* db_pimpl) noexcept
    : current_status_(TxStatus::Running),
      db_pimpl_(db_pimpl),
      config_ref_(db_pimpl_->GetConfig()) {
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

  for (auto& snapshot : write_set_) {
    if (snapshot.key == key) {
      return std::make_pair(snapshot.data_item_copy.value(),
                            snapshot.data_item_copy.size());
    }
  }

  for (auto& snapshot : read_set_) {
    if (snapshot.key == key) {
      return std::make_pair(snapshot.data_item_copy.value(),
                            snapshot.data_item_copy.size());
    }
  }
  auto* index_leaf = db_pimpl_->GetIndex().GetOrInsert(key);
  Snapshot snapshot = {key, nullptr, 0, index_leaf};

  snapshot.data_item_copy = concurrency_control_->Read(key, index_leaf);
  auto& ref = read_set_.emplace_back(std::move(snapshot));
  if (ref.data_item_copy.IsInitialized()) {
    return {ref.data_item_copy.value(), ref.data_item_copy.size()};
  } else {
    return {nullptr, 0};
  }
}

const std::pair<const std::byte* const, const size_t>
Transaction::Impl::ReadPrimaryIndex(const std::string_view table_name,
                                    const std::string_view key) {
  if (IsAborted()) return {nullptr, 0};

  std::string qualified_key = std::string(table_name) + "#" + std::string(key);
  for (auto& snapshot : write_set_) {
    if (snapshot.key == qualified_key) {
      return std::make_pair(snapshot.data_item_copy.value(),
                            snapshot.data_item_copy.size());
    }
  }

  for (auto& snapshot : read_set_) {
    if (snapshot.key == qualified_key) {
      return std::make_pair(snapshot.data_item_copy.value(),
                            snapshot.data_item_copy.size());
    }
  }
  auto* index_leaf =
      db_pimpl_->GetTable(table_name).GetPrimaryIndex().GetOrInsert(key);
  Snapshot snapshot = {qualified_key, nullptr, 0, index_leaf};

  snapshot.data_item_copy = concurrency_control_->Read(key, index_leaf);
  auto& ref = read_set_.emplace_back(std::move(snapshot));
  if (ref.data_item_copy.IsInitialized()) {
    return {ref.data_item_copy.value(), ref.data_item_copy.size()};
  } else {
    return {nullptr, 0};
  }
}

std::vector<std::string> Transaction::Impl::ReadSecondaryIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::any& key) {
  if (IsAborted()) return {};

  Table& table = db_pimpl_->GetTable(table_name);
  Index::ISecondaryIndex* index = table.GetSecondaryIndex(index_name);

  if (index == nullptr) {
    Abort();
    return {};
  }

  const std::type_info& key_type = index->KeyTypeInfo();
  if (key_type != key.type()) {
    Abort();
    return {};
  }

  std::string serialized_key = Util::SerializeKey(key);

  std::string qualified_key = std::string(table_name) + "#" +
                              std::string(index_name) + "#" +
                              std::string(serialized_key);

  for (auto& snapshot : write_set_) {
    if (snapshot.key == qualified_key) {
      return Util::DecodePKList(snapshot.data_item_copy.value(),
                                snapshot.data_item_copy.size());
    }
  }

  for (auto& snapshot : read_set_) {
    if (snapshot.key == qualified_key) {
      return Util::DecodePKList(snapshot.data_item_copy.value(),
                                snapshot.data_item_copy.size());
    }
  }

  DataItem* index_leaf = index->GetOrInsert(serialized_key);

  Snapshot snapshot = {qualified_key, nullptr, 0, index_leaf};

  snapshot.data_item_copy =
      concurrency_control_->Read(serialized_key, index_leaf);
  auto& ref = read_set_.emplace_back(std::move(snapshot));
  if (ref.data_item_copy.IsInitialized()) {
    return Util::DecodePKList(ref.data_item_copy.value(),
                              ref.data_item_copy.size());
  } else {
    return {};
  }
}

void Transaction::Impl::Write(const std::string_view key,
                              const std::byte value[], const size_t size) {
  if (IsAborted()) return;

  // TODO: if `size` is larger than Config.internal_buffer_size,
  // then we have to abort this transaction or throw exception

  bool is_rmf = false;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == key) {
      is_rmf = true;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  for (auto& snapshot : write_set_) {
    if (snapshot.key != key) continue;
    snapshot.data_item_copy.Reset(value, size);
    if (is_rmf) snapshot.is_read_modify_write = true;
    return;
  }

  auto* index_leaf = db_pimpl_->GetIndex().GetOrInsert(key);

  concurrency_control_->Write(key, value, size, index_leaf);
  Snapshot sp(key, value, size, index_leaf);
  if (is_rmf) sp.is_read_modify_write = true;
  write_set_.emplace_back(std::move(sp));
}

void Transaction::Impl::WritePrimaryIndex(const std::string_view table_name,
                                          const std::string_view key,
                                          const std::byte value[],
                                          const size_t size) {
  if (IsAborted()) return;

  // TODO: if `size` is larger than Config.internal_buffer_size,
  // then we have to abort this transaction or throw exception

  std::string qualified_key = std::string(table_name) + "#" + std::string(key);

  bool is_rmf = false;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == qualified_key) {
      is_rmf = true;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  for (auto& snapshot : write_set_) {
    if (snapshot.key != qualified_key) continue;
    snapshot.data_item_copy.Reset(value, size);
    if (is_rmf) snapshot.is_read_modify_write = true;
    return;
  }

  auto& table = db_pimpl_->GetTable(table_name);
  auto* index_leaf = table.GetPrimaryIndex().GetOrInsert(key);
  bool is_new_insert = !index_leaf->IsInitialized();

  concurrency_control_->Write(qualified_key, value, size, index_leaf);
  Snapshot sp(qualified_key, value, size, index_leaf);
  if (is_rmf) sp.is_read_modify_write = true;
  write_set_.emplace_back(std::move(sp));

  // Initialize pending_ only for new primary insert and when table has SK(s)
  if (is_new_insert) {
    size_t num_secondary = table.GetSecondaryIndexCount();
    if (num_secondary > 0) {
      auto& state = pending_[std::string(table_name)][std::string(key)];
      state.remaining = num_secondary;
      state.satisfied_indices.clear();
    }
  }
}

void Transaction::Impl::WriteSecondaryIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::any& key, const std::byte primary_key_buffer[],
    const size_t primary_key_size) {
  if (IsAborted()) return;

  // TODO: if `size` is larger than Config.internal_buffer_size,
  // then we have to abort this transaction or throw exception

  std::string_view primary_key_view;
  std::memcpy(&primary_key_view, primary_key_buffer, primary_key_size);

  Table& table = db_pimpl_->GetTable(table_name);
  Index::ISecondaryIndex* index = table.GetSecondaryIndex(index_name);
  auto primary_leaf = table.GetPrimaryIndex().Get(primary_key_view);

  if (primary_leaf == nullptr) {
    Abort();
    return;
  }

  // If the index is not registered, abort the transaction
  if (index == nullptr) {
    Abort();
    return;
  }

  // Key type must match the registered secondary index type
  const std::type_info& key_type = index->KeyTypeInfo();
  if (key_type != key.type()) {
    Abort();
    return;
  }

  std::string serialized_key = Util::SerializeKey(key);

  std::string qualified_key = std::string(table_name) + "#" +
                              std::string(index_name) + "#" +
                              std::string(serialized_key);
  // existing key
  DataItem* index_leaf = index->GetOrInsert(serialized_key);
  if (index_leaf->IsInitialized() && index->IsUnique()) {
    Abort();
    return;
  }

  bool is_rmf = false;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == qualified_key) {
      is_rmf = true;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  bool added = false;

  for (auto& snapshot : write_set_) {
    if (snapshot.key != qualified_key) continue;

    std::vector<std::string> new_pklist = Util::DecodePKList(
        snapshot.data_item_copy.value(), snapshot.data_item_copy.size());
    for (auto& p : new_pklist) {
      if (p == primary_key_view) {
        // already exists, do not decrement pending_
        return;
      }
    }

    std::string_view new_pk;
    std::memcpy(&new_pk, primary_key_buffer, primary_key_size);

    std::string encoded_value = Util::EncodePKList(new_pklist, new_pk);
    const std::byte* value_ptr =
        reinterpret_cast<const std::byte*>(encoded_value.data());
    size_t value_size = encoded_value.size();
    snapshot.data_item_copy.Reset(value_ptr, value_size);
    if (is_rmf) snapshot.is_read_modify_write = true;

    added = true;
    break;
  }

  if (!added) {
    std::vector<std::string> existing_pklist =
        Util::DecodePKList(index_leaf->value(), index_leaf->size());

    bool contains = false;
    for (auto& p : existing_pklist) {
      if (p == primary_key_view) {
        contains = true;
        break;
      }
    }

    if (!contains) {
      std::string encoded_value =
          Util::EncodePKList(existing_pklist, primary_key_view);

      const std::byte* value_ptr =
          reinterpret_cast<const std::byte*>(encoded_value.data());
      size_t value_size = encoded_value.size();

      concurrency_control_->Write(qualified_key, value_ptr, value_size,
                                  index_leaf);

      Snapshot sp(qualified_key, value_ptr, value_size, index_leaf);
      if (is_rmf) sp.is_read_modify_write = true;
      write_set_.emplace_back(std::move(sp));

      added = true;
    }
  }

  // decrement pending_ only if we actually added pk to SK
  if (added) {
    auto tbl_it = pending_.find(std::string(table_name));
    if (tbl_it != pending_.end()) {
      auto& per_table = tbl_it->second;
      auto pk_it = per_table.find(std::string(primary_key_view));
      if (pk_it != per_table.end()) {
        auto& state = pk_it->second;
        // decrement once per index_name for this PK
        if (state.satisfied_indices.insert(std::string(index_name)).second) {
          if (--state.remaining == 0) {
            per_table.erase(pk_it);
            if (per_table.empty()) pending_.erase(tbl_it);
          }
        }
      }
    }
  }
}

const std::optional<size_t> Transaction::Impl::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  auto result =
      db_pimpl_->GetIndex().Scan(begin, end, [&](std::string_view key) {
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
    const std::string_view table_name, const std::string_view index_name,
    const std::any& begin, const std::any& end,
    std::function<bool(std::string_view, const std::vector<std::string>)>
        operation) {
  Table& table = db_pimpl_->GetTable(table_name);
  Index::ISecondaryIndex* index = table.GetSecondaryIndex(index_name);

  if (index == nullptr) {
    Abort();
    return {};
  }

  std::string serialized_begin = Util::SerializeKey(begin);
  std::string serialized_end = Util::SerializeKey(end);

  auto result =
      index->Scan(serialized_begin, serialized_end, [&](std::string_view key) {
        const auto read_result =
            ReadSecondaryIndex(table_name, index_name, key);
        if (IsAborted()) return true;
        return operation(key, read_result);
      });
  if (!result.has_value()) {
    Abort();
  }
  return result;
};

bool Transaction::Impl::ValidateSKNotNull() {
  // 作成済みのすべてのSecondaryindexにおいて、SK→PKのマッピングが存在するかを確認する
  return pending_.empty();
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

TxStatus Transaction::GetCurrentStatus() {
  return tx_pimpl_->GetCurrentStatus();
}
const std::pair<const std::byte* const, const size_t> Transaction::Read(
    const std::string_view key) {
  return tx_pimpl_->Read(key);
}

const std::pair<const std::byte* const, const size_t>
Transaction::ReadPrimaryIndex(const std::string_view table_name,
                              const std::string_view key) {
  return tx_pimpl_->ReadPrimaryIndex(table_name, key);
}

std::vector<std::string> Transaction::ReadSecondaryIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::any& key) {
  return tx_pimpl_->ReadSecondaryIndex(table_name, index_name, key);
}

void Transaction::Write(const std::string_view key, const std::byte value[],
                        const size_t size) {
  tx_pimpl_->Write(key, value, size);
}

void Transaction::WritePrimaryIndex(const std::string_view table_name,
                                    const std::string_view key,
                                    const std::byte value[],
                                    const size_t size) {
  tx_pimpl_->WritePrimaryIndex(table_name, key, value, size);
}

void Transaction::WriteSecondaryIndex(const std::string_view table_name,
                                      const std::string_view index_name,
                                      const std::any& key,
                                      const std::byte primary_key_buffer[],
                                      const size_t primary_key_size) {
  tx_pimpl_->WriteSecondaryIndex(table_name, index_name, key,
                                 primary_key_buffer, primary_key_size);
}

const std::optional<size_t> Transaction::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  return tx_pimpl_->Scan(begin, end, operation);
};

const std::optional<size_t> Transaction::ScanSecondaryIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::any& begin, const std::any& end,
    std::function<bool(std::string_view, const std::vector<std::string>)>
        operation) {
  return tx_pimpl_->ScanSecondaryIndex(table_name, index_name, begin, end,
                                       operation);
}

bool Transaction::ValidateSKNotNull() { return tx_pimpl_->ValidateSKNotNull(); }

void Transaction::Abort() { tx_pimpl_->Abort(); }
bool Transaction::Precommit() { return tx_pimpl_->Precommit(); }

Transaction::Transaction(void* db_pimpl) noexcept
    : tx_pimpl_(
          std::make_unique<Impl>(reinterpret_cast<Database::Impl*>(db_pimpl))) {
}
Transaction::~Transaction() noexcept = default;

}  // namespace LineairDB
