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
#ifndef LINEAIRDB_TRANSACTION_H
#define LINEAIRDB_TRANSACTION_H

#include <lineairdb/tx_status.h>

#include <cstddef>
#include <cstring>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>

namespace LineairDB {

/**
 * @brief
 * We adopt "the page model" [Vossen95] as the model of transaction processing.
 * For each transaction, the page model provides us four operations: two *data
 * operations* and two *termination operations*.
 *
 * Data operations:
 *   read: read a data item.
 *   write: generate a new version of a data item.
 * Termination operations:
 *   commit: commit this transaction.
 *   abort:  abort this transaction.
 *
 * Note that the commit operation is implicitly executed by the worker threads
 * running in LineairDB, only when the transaction satisfies Strict
 * Serializability and Recoverability.
 * All methods are thread-safe.
 * @see [Vossen95] https://doi.org/10.1007/BFb0015267
 **/
class Transaction {
 public:
  /**
   * @brief Get the current transaction status.
   * For transactions such that GetCurrentStatus() returns TxStatus::Aborted,
   * it is not guaranteed for subsequent read operations returns the correct
   * values.
   * @return TxStatus
   */
  TxStatus GetCurrentStatus();
  bool IsRunning() { return GetCurrentStatus() == TxStatus::Running; }
  bool IsCommitted() { return GetCurrentStatus() == TxStatus::Aborted; }
  bool IsAborted() { return GetCurrentStatus() == TxStatus::Aborted; }

  /**
   * @brief If the database contains a data item for "key", returns a pair
   * (a pointer of value, the size of value).
   * @param key An identifier for a data item
   * @return std::pair<void*, size_t>
   * A pointer to the value of the requested data item and the size of the
   * value.
   * If there does not exists the data item of given key, it returns the pair
   * (nullptr, 0).
   *
   */
  const std::pair<const std::byte* const, const size_t> Read(
      const std::string_view key);

  /**
   * @brief
   * Reads an user-defined value with a given key. T must be same as one on
   * writing with Write().
   * @tparam T
   * T must be Trivially Copyable and Constructable.
   * This is because LineairDB is a Key-value storage but not a object storage.
   *
   * @param key
   * @return const std::optional<T>
   */
  template <typename T>
  const std::optional<T> Read(const std::string_view key) {
    static_assert(std::is_trivially_copyable<T>::value == true,
                  "LineairDB expects to read/write trivially copyable types.");
    auto result = Read(key);
    if (result.second != 0) {
      const T copy_constructed_result =
          *reinterpret_cast<const T*>(result.first);
      return copy_constructed_result;
    } else {
      return std::nullopt;
    }
  }

  /**
   * @brief
   * Writes a value with a given key.
   *
   * @param key
   * @param value
   * @param size
   */
  void Write(const std::string_view key, const std::byte value[],
             const size_t size);

  /**
   * @brief
   * Writes an user-defined value with a given key.
   *
   * @tparam T
   * T must be Trivially Copyable.
   * This is because LineairDB is a Key-value storage but not a object storage.
   * @param key
   * @param value
   */
  template <typename T>
  void Write(const std::string_view key, const T& value) {
    static_assert(std::is_trivially_copyable<T>::value == true,
                  "LineairDB expects to read/write trivially copyable types.");
    std::byte buffer[sizeof(T)];
    std::memcpy(buffer, &value, sizeof(T));
    Write(key, buffer, sizeof(T));
  };

  void Abort();

 private:
  Transaction(void*) noexcept;
  ~Transaction() noexcept;
  bool Precommit();

 private:
  class Impl;
  const std::unique_ptr<Impl> tx_pimpl_;
  friend class Database;
};

}  // namespace LineairDB
#endif /** LINEAIRDB_TRANSACTION_H **/
