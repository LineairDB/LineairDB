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
#include <memory>
#include <type_traits>
#include <utility>

#include "concurrency_control/concurrency_control_base.h"
#include "concurrency_control/impl/silo_nwr.hpp"
#include "concurrency_control/impl/two_phase_locking.hpp"
#include "database_impl.h"
#include "types/snapshot.hpp"

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

  auto table_write_set_it = write_set_.find(current_table_->GetTableName());
  if (table_write_set_it != write_set_.end()) {
    auto write_it = std::find_if(
        table_write_set_it->second.begin(), table_write_set_it->second.end(),
        [&](const Snapshot& s) { return s.key == key; });
    if (write_it != table_write_set_it->second.end()) {
      return {write_it->data_item_copy.value(),
              write_it->data_item_copy.size()};
    }
  }

  auto table_read_set_it = read_set_.find(current_table_->GetTableName());
  if (table_read_set_it != read_set_.end()) {
    auto read_it = std::find_if(
        table_read_set_it->second.begin(), table_read_set_it->second.end(),
        [&](const Snapshot& s) { return s.key == key; });
    if (read_it != table_read_set_it->second.end()) {
      return {read_it->data_item_copy.value(), read_it->data_item_copy.size()};
    }
  }

  auto* index_leaf = current_table_->GetPrimaryIndex().GetOrInsert(key);

  Snapshot snapshot = {key, nullptr, 0, index_leaf, 0};

  snapshot.data_item_copy = concurrency_control_->Read(key, index_leaf);
  auto& ref = read_set_[current_table_->GetTableName()].emplace_back(
      std::move(snapshot));
  if (ref.data_item_copy.IsInitialized()) {
    return {ref.data_item_copy.value(), ref.data_item_copy.size()};
  } else {
    return {nullptr, 0};
  }
}

void Transaction::Impl::Write(const std::string_view key,
                              const std::byte value[], const size_t size) {
  if (IsAborted()) return;

  // TODO: if `size` is larger than Config.internal_buffer_size,
  // then we have to abort this transaction or throw exception
  EnsureCurrentTable();
  auto table_read_set_it = read_set_.find(current_table_->GetTableName());
  bool is_rmf = false;
  if (table_read_set_it != read_set_.end()) {
    auto read_it = std::find_if(
        table_read_set_it->second.begin(), table_read_set_it->second.end(),
        [&](const Snapshot& s) { return s.key == key; });
    if (read_it != table_read_set_it->second.end()) {
      read_it->is_read_modify_write = true;
      is_rmf = true;
    }
  }

  auto table_write_set_it = write_set_.find(current_table_->GetTableName());
  if (table_write_set_it != write_set_.end()) {
    auto write_it = std::find_if(
        table_write_set_it->second.begin(), table_write_set_it->second.end(),
        [&](const Snapshot& s) { return s.key == key; });
    if (write_it != table_write_set_it->second.end()) {
      write_it->data_item_copy.Reset(value, size);
      if (is_rmf) write_it->is_read_modify_write = true;
      return;
    }
  }

  auto* index_leaf = current_table_->GetPrimaryIndex().GetOrInsert(key);

  concurrency_control_->Write(key, value, size, index_leaf);
  Snapshot sp(key, value, size, index_leaf, 0);
  if (is_rmf) sp.is_read_modify_write = true;
  write_set_[current_table_->GetTableName()].emplace_back(std::move(sp));
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
void Transaction::Write(const std::string_view key, const std::byte value[],
                        const size_t size) {
  tx_pimpl_->Write(key, value, size);
}
const std::optional<size_t> Transaction::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  return tx_pimpl_->Scan(begin, end, operation);
};
void Transaction::Abort() { tx_pimpl_->Abort(); }
bool Transaction::Precommit() { return tx_pimpl_->Precommit(); }

bool Transaction::SetTable(const std::string_view table_name) {
  return tx_pimpl_->SetTable(table_name);
}

Transaction::Transaction(void* db_pimpl) noexcept
    : tx_pimpl_(
          std::make_unique<Impl>(reinterpret_cast<Database::Impl*>(db_pimpl))) {
}
Transaction::~Transaction() noexcept = default;

}  // namespace LineairDB
