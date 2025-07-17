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

void Transaction::Impl::WritePrimaryIndex(const std::string_view table_name,
                                          const std::string_view primary_key,
                                          const std::byte value[],
                                          const size_t size) {
  if (IsAborted()) return;

  // TODO: if `size` is larger than Config.internal_buffer_size,
  // then we have to abort this transaction or throw exception

  std::string qualified_key =
      std::string(table_name) + "\x1F" + std::string(primary_key);
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

  auto* index_leaf = db_pimpl_->GetTable(table_name)
                         .GetPrimaryIndex()
                         .GetOrInsert(primary_key);

  concurrency_control_->Write(qualified_key, value, size, index_leaf);
  Snapshot sp(qualified_key, value, size, index_leaf);
  if (is_rmf) sp.is_read_modify_write = true;
  write_set_.emplace_back(std::move(sp));
}

std::optional<std::pair<const std::byte* const, size_t>>
Transaction::Impl::ReadPrimaryIndex(const std::string_view table_name,
                                    const std::string_view primary_key) {
  if (IsAborted()) return std::nullopt;

  std::string qualified_key =
      std::string(table_name) + "\x1F" + std::string(primary_key);

  for (auto& snapshot : write_set_) {
    if (snapshot.key == qualified_key) {
      return std::make_optional(std::make_pair(snapshot.data_item_copy.value(),
                                               snapshot.data_item_copy.size()));
    }
  }

  for (auto& snapshot : read_set_) {
    if (snapshot.key == qualified_key) {
      return std::make_optional(std::make_pair(snapshot.data_item_copy.value(),
                                               snapshot.data_item_copy.size()));
    }
  }
  auto* index_leaf = db_pimpl_->GetTable(table_name)
                         .GetPrimaryIndex()
                         .GetOrInsert(primary_key);
  Snapshot snapshot = {qualified_key, nullptr, 0, index_leaf};

  snapshot.data_item_copy =
      concurrency_control_->Read(qualified_key, index_leaf);
  auto& ref = read_set_.emplace_back(std::move(snapshot));
  if (ref.data_item_copy.IsInitialized()) {
    return std::make_optional(
        std::make_pair(ref.data_item_copy.value(), ref.data_item_copy.size()));
  } else {
    return std::nullopt;
  }
}

void Transaction::Impl::WriteSecondaryIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view encoded_secondary_key,
    const std::byte primary_key_bytes[], const size_t primary_key_size) {
  if (IsAborted()) {
    return;
  }

  // Note: この実装は、以下の前提設計に基づいています。
  // 1. SecondaryIndex は、キー（encoded_secondary_key）ごとに専用の DataItem
  // を保持する。
  // 2. その DataItem のペイロード（値）には、プライマリキーのリスト（PKList）が
  //    シリアライズされて格納される。
  // 3. ロックと並行性制御は、このセカンダリキー専用の DataItem
  // に対して行われる。
  //
  // このため、Table::GetSecondaryIndex や SecondaryIndex::GetOrInsertDataItem
  // などの補助メソッドが将来実装される必要があります。

  try {
    auto& table = db_pimpl_->GetTable(table_name);

    // TODO: Table::GetSecondaryIndex(index_name) を実装し、SecondaryIndex
    // のインスタンスを取得する。 auto* sidx =
    // table.GetSecondaryIndex(index_name); if (!sidx) {
    //   Abort();
    //   return;
    // }

    // プライマリキーの存在をチェックする。
    const std::string_view primary_key(
        reinterpret_cast<const char*>(primary_key_bytes), primary_key_size);
    auto* pk_data_item = table.GetPrimaryIndex().Get(primary_key);
    if (pk_data_item == nullptr) {
      // 参照先のプライマリキーが存在しないため、アボートする。
      Abort();
      return;
    }

    // トランザクションの read/write set で使用する一意なキーを生成する。
    std::string qualified_key = std::string(table_name) + "\x1F" +
                                std::string(index_name) + "\x1E" +
                                std::string(encoded_secondary_key);

    // TODO: セカンダリキーに対応する DataItem を取得または新規作成する。
    // SecondaryIndex に GetOrInsertDataItem のようなメソッドが必要。
    // auto* sk_data_item = sidx->GetOrInsertDataItem(encoded_secondary_key);

    // --- ここから Read-Modify-Write のロジック ---
    // 1. 現在の PKList を読み出す (既存の write_set またはストレージから)。
    // 2. 新しいプライマリキーを PKList に追加する (重複や UNIQUE 制約も考慮)。
    // 3. 更新後の PKList をシリアライズし、新しい値とする。

    // (以下は仮の実装です。シリアライズライブラリ(例:msgpack)と上記ヘルパーが必要です)
    const std::byte* new_value = primary_key_bytes;
    size_t new_size = primary_key_size;

    // RMWのため、一度write_setの中を探す
    for (auto& snapshot : write_set_) {
      if (snapshot.key == qualified_key) {
        // すでに更新予定がある場合、その内容をさらに更新する。
        // TODO: snapshot.data_item_copy からPKListをデシリアライズし、
        //       新しいPKを追加後、再シリアライズして data_item_copy
        //       を更新する。 現状は単純に上書きするだけ。
        // snapshot.data_item_copy.Reset(new_value, new_size);
        return;
      }
    }

    // TODO: 本来は sk_data_item
    // を使うべきだが、今はダミーとしてプライマリ行のものを流用。
    //       これによりロックはプライマリ行にかかるが、コミット時に値が上書きされる問題が残る。
    //       この問題はセカンダリキー専用のDataItemを導入することで解決される。
    auto* index_leaf_for_lock = pk_data_item;

    // 新しい書き込み操作として Snapshot を作成する。
    Snapshot snapshot(qualified_key, new_value, new_size, index_leaf_for_lock);

    // Read-Modify-Write のために read_set を確認
    for (const auto& read_snap : read_set_) {
      if (read_snap.key == qualified_key) {
        snapshot.is_read_modify_write = true;
        break;
      }
    }

    write_set_.emplace_back(std::move(snapshot));

    // Concurrency Control には通知するが、Silo/NWR
    // ではこの時点での実操作はない。
    concurrency_control_->Write(qualified_key, new_value, new_size,
                                index_leaf_for_lock);

  } catch (const std::out_of_range&) {
    // GetTable でテーブルが見つからなかった場合。
    Abort();
    return;
  }
}

/*
void Transaction::Impl::ReadPrimaryIndex(const std::string_view table_name,
                                         const std::string_view primary_key) {
  // TODO: implement this
}

void Transaction::Impl::ReadSecondaryIndex(const std::string_view table_name,
                                           const std::string_view index_name,
                                           const std::string_view secondary_key)
{
  // TODO: implement this
} */
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
void Transaction::Write(const std::string_view key, const std::byte value[],
                        const size_t size) {
  tx_pimpl_->Write(key, value, size);
}

void Transaction::WritePrimaryIndex(const std::string_view table_name,
                                    const std::string_view primary_key,
                                    const std::byte value[],
                                    const size_t size) {
  tx_pimpl_->WritePrimaryIndex(table_name, primary_key, value, size);
}

void Transaction::WriteSecondaryIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view encoded_secondary_key, const std::byte value[],
    const size_t size) {
  tx_pimpl_->WriteSecondaryIndex(table_name, index_name, encoded_secondary_key,
                                 value, size);
}

std::optional<std::pair<const std::byte* const, size_t>>
Transaction::ReadPrimaryIndex(const std::string_view table_name,
                              const std::string_view primary_key) {
  return tx_pimpl_->ReadPrimaryIndex(table_name, primary_key);
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

Transaction::Transaction(void* db_pimpl) noexcept
    : tx_pimpl_(
          std::make_unique<Impl>(reinterpret_cast<Database::Impl*>(db_pimpl))) {
}
Transaction::~Transaction() noexcept = default;

}  // namespace LineairDB
