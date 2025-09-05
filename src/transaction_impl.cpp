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
#include "lineairdb/key_serializer.h"
#include "types/snapshot.hpp"
#include "util/pklist_util.h"

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

/* const std::pair<const std::byte* const, const size_t>
Transaction::Impl::Read( const std::string_view key) { if (IsAborted()) return
{nullptr, 0};

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
} */

const std::pair<const std::byte* const, const size_t> Transaction::Impl::Read(
    const std::string_view table_name, const std::string_view key) {
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
  Index::SecondaryIndexInterface* index = table.GetSecondaryIndex(index_name);

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
  std::string qualified_key =
      BuildQualifiedSKKey(table_name, index_name, serialized_key);

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

/* void Transaction::Impl::Write(const std::string_view key,
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
  Snapshot sp(key, value, size, index_leaf, current_table_->GetTableName());
  if (is_rmf) sp.is_read_modify_write = true;
  write_set_.emplace_back(std::move(sp));
} */

void Transaction::Impl::Write(const std::string_view table_name,
                              const std::string_view key,
                              const std::byte value[], const size_t size) {
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
      auto& state =
          remainingNotNullSkWrites_[std::string(table_name)][std::string(key)];
      state.remainingWrites = num_secondary;
      state.satisfiedIndexNames.clear();
    }
  }
}

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

bool Transaction::Impl::FindWriteSnapshot(const std::string& qualified_key,
                                          Snapshot** out) {
  for (auto& snapshot : write_set_) {
    if (snapshot.key == qualified_key) {
      *out = &snapshot;
      return true;
    }
  }
  return false;
}

std::vector<std::string> Transaction::Impl::DecodeCurrentPKList(
    const std::string& qualified_key, DataItem* leaf) {
  Snapshot* snap = nullptr;
  if (FindWriteSnapshot(qualified_key, &snap)) {
    return Util::DecodePKList(snap->data_item_copy.value(),
                              snap->data_item_copy.size());
  }
  return Util::DecodePKList(leaf->value(), leaf->size());
}

void Transaction::Impl::WriteEncodedPKList(const std::string& qualified_key,
                                           DataItem* leaf,
                                           const std::string& encoded_value,
                                           bool mark_rmf) {
  const std::byte* value_ptr =
      reinterpret_cast<const std::byte*>(encoded_value.data());
  size_t value_size = encoded_value.size();

  Snapshot* snap = nullptr;
  if (FindWriteSnapshot(qualified_key, &snap)) {
    snap->data_item_copy.Reset(value_ptr, value_size);
    if (mark_rmf) snap->is_read_modify_write = true;
    return;
  }
  concurrency_control_->Write(qualified_key, value_ptr, value_size, leaf);
  Snapshot sp(qualified_key, value_ptr, value_size, leaf);
  if (mark_rmf) sp.is_read_modify_write = true;
  write_set_.emplace_back(std::move(sp));
}

void Transaction::Impl::WriteEncodedPKList(Snapshot& existing_snapshot,
                                           const std::string& encoded_value,
                                           bool mark_rmf) {
  const std::byte* value_ptr =
      reinterpret_cast<const std::byte*>(encoded_value.data());
  size_t value_size = encoded_value.size();
  existing_snapshot.data_item_copy.Reset(value_ptr, value_size);
  if (mark_rmf) existing_snapshot.is_read_modify_write = true;
}

bool Transaction::Impl::IsKeyInReadSet(const std::string& qualified_key) const {
  for (const auto& snapshot : read_set_) {
    if (snapshot.key == qualified_key) return true;
  }
  return false;
}

std::string Transaction::Impl::EncodePKBytes(
    const std::vector<std::string>& list) const {
  if (list.empty()) {
    return LineairDB::Codec::EncodePKList({});
  }
  LineairDB::Codec::PKList tmp;
  tmp.reserve(list.size());
  for (auto& s : list) tmp.emplace_back(s);
  return LineairDB::Codec::EncodePKList(tmp);
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
  Index::SecondaryIndexInterface* index = table.GetSecondaryIndex(index_name);
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

  std::string qualified_key =
      BuildQualifiedSKKey(table_name, index_name, serialized_key);
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
      if (p == primary_key_view) return;  // already exists
    }
    if (index->IsUnique() && !new_pklist.empty()) {
      Abort();
      return;
    }
    std::string_view new_pk;
    std::memcpy(&new_pk, primary_key_buffer, primary_key_size);
    std::string encoded_value = Util::EncodePKList(new_pklist, new_pk);
    WriteEncodedPKList(snapshot, encoded_value, is_rmf);
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
      WriteEncodedPKList(qualified_key, index_leaf, encoded_value, is_rmf);
      added = true;
    }
  }

  // decrement remainingNotNullSkWrites_ only if we actually added pk to SK
  if (added) {
    auto tbl_it = remainingNotNullSkWrites_.find(std::string(table_name));
    if (tbl_it != remainingNotNullSkWrites_.end()) {
      auto& per_table = tbl_it->second;
      auto pk_it = per_table.find(std::string(primary_key_view));
      if (pk_it != per_table.end()) {
        auto& state = pk_it->second;
        // decrement once per index_name for this PK
        if (state.satisfiedIndexNames.insert(std::string(index_name)).second) {
          if (--state.remainingWrites == 0) {
            per_table.erase(pk_it);
            if (per_table.empty()) remainingNotNullSkWrites_.erase(tbl_it);
          }
        }
      }
    }
  }
}

/* const std::optional<size_t> Transaction::Impl::Scan(
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
}; */

const std::optional<size_t> Transaction::Impl::Scan(
    const std::string_view table_name, const std::string_view begin,
    const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  auto& primary = db_pimpl_->GetTable(table_name).GetPrimaryIndex();
  auto result = primary.Scan(begin, end, [&](std::string_view key) {
    const auto read_result = Read(table_name, key);
    if (IsAborted()) return true;
    return operation(key, read_result);
  });
  if (!result.has_value()) {
    Abort();
  }
  return result;
}

const std::optional<size_t> Transaction::Impl::ScanSecondaryIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::any& begin, const std::any& end,
    std::function<bool(std::string_view, const std::vector<std::string>)>
        operation) {
  Table& table = db_pimpl_->GetTable(table_name);
  Index::SecondaryIndexInterface* index = table.GetSecondaryIndex(index_name);

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

// Overview of operations
// Variables: oldSK, newSK

void Transaction::Impl::UpdateSecondaryIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::any& old_key, const std::any& new_key,
    const std::byte primary_key_buffer[], const size_t primary_key_size) {
  if (IsAborted()) return;

  // 1) Preconditions
  //   - Resolve table and index
  //   - Check that the target primary key (PK) exists
  //   - Check that the secondary key types match the registered index type
  //   - If old_key and new_key are identical, this operation is a no-op
  std::string_view primary_key_view;
  std::memcpy(&primary_key_view, primary_key_buffer, primary_key_size);

  Table& table = db_pimpl_->GetTable(table_name);
  Index::SecondaryIndexInterface* index = table.GetSecondaryIndex(index_name);
  auto primary_leaf = table.GetPrimaryIndex().Get(primary_key_view);
  if (primary_leaf == nullptr || index == nullptr) {
    Abort();
    return;
  }
  if (index->KeyTypeInfo() != old_key.type() ||
      index->KeyTypeInfo() != new_key.type()) {
    Abort();
    return;
  }

  // no-op if old_key == new_key
  if (old_key.type() == new_key.type() &&
      Util::SerializeKey(old_key) == Util::SerializeKey(new_key)) {
    return;  // nothing to do
  }

  // 2) Remove PK from oldSK
  //    - If oldSK exists in write_set_, update the in-flight snapshot
  //    - Otherwise, decode from the current leaf, remove PK, re-encode and
  //    write
  //    shanpshot.Reset or write_set_.emplace_back
  std::string old_serialized = Util::SerializeKey(old_key);
  std::string new_serialized = Util::SerializeKey(new_key);

  std::string old_qualified =
      BuildQualifiedSKKey(table_name, index_name, old_serialized);
  std::string new_qualified =
      BuildQualifiedSKKey(table_name, index_name, new_serialized);

  DataItem* old_leaf = index->GetOrInsert(old_serialized);
  DataItem* new_leaf = index->GetOrInsert(new_serialized);

  // UNIQUE constraint check on the newSK side
  if (index->IsUnique()) {
    // If there is an in-flight write for newSK in write_set_, consult it first;
    // otherwise consult the current leaf value.
    std::vector<std::string> current_new_list;
    bool found_snapshot_new = false;
    for (auto& snapshot : write_set_) {
      if (snapshot.key == new_qualified) {
        current_new_list = Util::DecodePKList(snapshot.data_item_copy.value(),
                                              snapshot.data_item_copy.size());
        found_snapshot_new = true;
        break;
      }
    }
    if (!found_snapshot_new) {
      current_new_list =
          Util::DecodePKList(new_leaf->value(), new_leaf->size());
    }
    // Abort if another PK already exists for newSK.
    // For an update that moves the same PK (old != new), the list should be
    // empty.
    bool another_exists = false;
    for (auto& p : current_new_list) {
      if (p != primary_key_view) {
        another_exists = true;
        break;
      }
    }
    if (another_exists) {
      Abort();
      return;
    }
  }

  // 2-a) If oldSK exists in write_set_, remove PK from that snapshot
  bool old_updated = false;
  for (auto& snapshot : write_set_) {
    if (snapshot.key != old_qualified) continue;
    auto lst = Util::DecodePKList(snapshot.data_item_copy.value(),
                                  snapshot.data_item_copy.size());
    size_t before = lst.size();
    lst.erase(
        std::remove(lst.begin(), lst.end(), std::string(primary_key_view)),
        lst.end());
    if (lst.size() != before) {
      std::string encoded = EncodePKBytes(lst);
      const std::byte* v = reinterpret_cast<const std::byte*>(encoded.data());
      size_t n = encoded.size();
      snapshot.data_item_copy.Reset(v, n);
      for (auto& read_snapshot : read_set_) {
        if (read_snapshot.key == old_qualified) {
          read_snapshot.is_read_modify_write = true;
          break;
        }
      }
    }
    old_updated = true;
    break;
  }

  // 2-b) Otherwise, remove PK from the current leaf and write it into
  // write_set_
  if (!old_updated) {
    auto lst = Util::DecodePKList(old_leaf->value(), old_leaf->size());
    size_t before = lst.size();
    lst.erase(
        std::remove(lst.begin(), lst.end(), std::string(primary_key_view)),
        lst.end());
    if (lst.size() != before) {
      std::string encoded = EncodePKBytes(lst);
      WriteEncodedPKList(old_qualified, old_leaf, encoded,
                         IsKeyInReadSet(old_qualified));
    }
  }

  // 2-c) If oldSK's PK list becomes empty, delete the SK from the range index
  {
    // Get the latest view of oldSK (prefer write_set_ if present)
    std::vector<std::string> cur;
    bool found = false;
    for (auto& snapshot : write_set_) {
      if (snapshot.key == old_qualified) {
        cur = Util::DecodePKList(snapshot.data_item_copy.value(),
                                 snapshot.data_item_copy.size());
        found = true;
        break;
      }
    }
    if (!found) {
      cur = Util::DecodePKList(old_leaf->value(), old_leaf->size());
    }
    if (cur.empty()) {
      if (!index->DeleteKey(old_serialized)) {
        Abort();
        return;
      }
    }
  }

  // 3) Add PK to newSK (equivalent to WriteSecondaryIndex semantics)
  {
    bool is_rmf_new = IsKeyInReadSet(new_qualified);

    bool added = false;
    for (auto& snapshot : write_set_) {
      if (snapshot.key != new_qualified) continue;
      auto new_pklist = Util::DecodePKList(snapshot.data_item_copy.value(),
                                           snapshot.data_item_copy.size());
      for (auto& p : new_pklist) {
        if (p == primary_key_view) {
          // Already registered in this transaction; no further action needed
          return;
        }
      }
      if (index->IsUnique() && !new_pklist.empty()) {
        Abort();
        return;
      }
      std::string_view new_pk;
      std::memcpy(&new_pk, primary_key_buffer, primary_key_size);
      std::string encoded_value = Util::EncodePKList(new_pklist, new_pk);
      WriteEncodedPKList(snapshot, encoded_value, is_rmf_new);
      added = true;
      break;
    }
    if (!added) {
      auto existing_pklist =
          Util::DecodePKList(new_leaf->value(), new_leaf->size());
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
        WriteEncodedPKList(new_qualified, new_leaf, encoded_value, is_rmf_new);
      }
    }
  }
}

bool Transaction::Impl::ValidateSKNotNull() {
  // Ensure that, for every created secondary index, a SK→PK mapping exists
  // (NOT NULL constraint). This is tracked via the per-transaction
  // 'remainingNotNullSkWrites_' state.
  return remainingNotNullSkWrites_.empty();
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
/* const std::pair<const std::byte* const, const size_t> Transaction::Read(
    const std::string_view key) {
  return tx_pimpl_->Read(key);
} */

const std::pair<const std::byte* const, const size_t> Transaction::Read(
    const std::string_view table_name, const std::string_view key) {
  return tx_pimpl_->Read(table_name, key);
}

std::vector<std::string> Transaction::ReadSecondaryIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::any& key) {
  return tx_pimpl_->ReadSecondaryIndex(table_name, index_name, key);
}

/* void Transaction::Write(const std::string_view key, const std::byte value[],
                        const size_t size) {
  tx_pimpl_->Write(key, value, size);
} */

void Transaction::Write(const std::string_view table_name,
                        const std::string_view key, const std::byte value[],
                        const size_t size) {
  tx_pimpl_->Write(table_name, key, value, size);
}

void Transaction::WriteSecondaryIndex(const std::string_view table_name,
                                      const std::string_view index_name,
                                      const std::any& key,
                                      const std::byte primary_key_buffer[],
                                      const size_t primary_key_size) {
  tx_pimpl_->WriteSecondaryIndex(table_name, index_name, key,
                                 primary_key_buffer, primary_key_size);
}

void Transaction::UpdateSecondaryIndex(const std::string_view table_name,
                                       const std::string_view index_name,
                                       const std::any& old_key,
                                       const std::any& new_key,
                                       const std::byte primary_key_buffer[],
                                       const size_t primary_key_size) {
  tx_pimpl_->UpdateSecondaryIndex(table_name, index_name, old_key, new_key,
                                  primary_key_buffer, primary_key_size);
}

/* const std::optional<size_t> Transaction::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  return tx_pimpl_->Scan(begin, end, operation);
}; */

const std::optional<size_t> Transaction::Scan(
    const std::string_view table_name, const std::string_view begin,
    const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  return tx_pimpl_->Scan(table_name, begin, end, operation);
}

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
