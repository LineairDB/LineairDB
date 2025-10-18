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

#include <any>
#include <cstddef>
#include <cstring>
#include <functional>
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
 *   read: read a data item. #Scan consists of multiple read operations.
 *
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
   * @brief
   * If the database contains a data item for "key", returns a pair
   * (a pointer of value, the size of value).
   * @param key An identifier for a data item
   * @return std::pair<void*, size_t>
   * A pointer to the value of the requested data item and the size of the
   * value.
   * If there does not exists the data item of given key, it returns the pair
   * (nullptr, 0).
   *
   */
  /*   const std::pair<const std::byte* const, const size_t> Read(
        const std::string_view key); */

  /**
   * @brief
   * Reads a value as user-defined type. T must be same as one on
   * writing the value with Write().
   * @tparam T
   * T must be Trivially Copyable and Constructable.
   * This is because LineairDB is a Key-value storage but not a object storage.
   *
   * @param key
   * @return const std::optional<T>
   */
  /*   template <typename T>
    const std::optional<T> Read(const std::string_view key) {
      static_assert(std::is_trivially_copyable<T>::value == true,
                    "LineairDB expects to read/write trivially copyable
    types."); auto result = Read(key); if (result.second != 0) { const T
    copy_constructed_result = *reinterpret_cast<const T*>(result.first); return
    copy_constructed_result; } else { return std::nullopt;
      }
    } */

  const std::pair<const std::byte* const, const size_t> Read(
      const std::string_view key);

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

  std::vector<std::pair<const std::byte* const, const size_t>>
  ReadSecondaryIndex(const std::string_view index_name,
                     const std::string_view key);

  template <typename T>
  std::vector<std::pair<const std::byte* const, const size_t>>
  ReadSecondaryIndex(const std::string_view index_name,
                     const std::string_view key) {
    auto result = ReadSecondaryIndex(index_name, key);
    return result;
  }

  /**
   * @brief
   * Writes a value with a given key.
   *
   * @param key
   * @param value
   * @param size
   */
  /*   void Write(const std::string_view key, const std::byte value[],
               const size_t size); */

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
  /*   template <typename T>
    void Write(const std::string_view key, const T& value) {
      static_assert(std::is_trivially_copyable<T>::value == true,
                    "LineairDB expects to read/write trivially copyable
    types."); std::byte buffer[sizeof(T)]; std::memcpy(buffer, &value,
    sizeof(T)); Write(key, buffer, sizeof(T));
    }; */

  void Write(const std::string_view key, const std::byte value[],
             const size_t size);

  template <typename T>
  void Write(const std::string_view key, const T& value) {
    static_assert(std::is_trivially_copyable<T>::value == true,
                  "LineairDB expects to read/write trivially copyable types.");
    std::byte buffer[sizeof(T)];
    std::memcpy(buffer, &value, sizeof(T));
    Write(key, buffer, sizeof(T));
  }

  void WriteSecondaryIndex(const std::string_view index_name,
                           const std::string_view key,
                           const std::byte primary_key_buffer[],
                           const size_t primary_key_size);

  template <typename T>
  void WriteSecondaryIndex(const std::string_view index_name,
                           const std::string_view key, const T& primary_key) {
    static_assert(std::is_trivially_copyable<T>::value == true,
                  "LineairDB expects to read/write trivially copyable types.");
    std::byte buffer[sizeof(T)];
    std::memcpy(buffer, &primary_key, sizeof(T));
    WriteSecondaryIndex(index_name, key, buffer, sizeof(T));
  }
  /**
   * @brief
   * Get all data items that match the range from the "begin" key to the "end"
   * key in the lexical order.
   * The term "get" here means that it is equivalent to the #Read
   * operation: this operation performs read operations for all the keys.
   * @param begin
   *  An identifier of the starting point of the range search.
   *  This key is included in the range: if there exists a data item with this
   *  key, LineairDB returns the data item.
   * @param end
   *  An identifier of the ending point of the range search.
   *  This key is included in the range: if there exists a data item with this
   *  key, LineairDB returns the data item.
   * @param operation
   *  A bool function to be executed iteratively on data items matching the
   * input range. The arguments of the function are key (std::string_view) and
   * value (std::pair). Return value (boolean) forces LineairDB to cancel the
   * scanning operation; when a function returns true at some key, this function
   * will never be invoked with the next key.
   * @return std::optional<size_t>
   *  returns the total number of rows that match the inputted range, if
   * succeed. Concurrent transactions may aborts this scan operation and returns
   * std::nullopt_t.
   *
   */
  /*   const std::optional<size_t> Scan(
        const std::string_view begin, const std::optional<std::string_view> end,
        std::function<bool(std::string_view,
                           const std::pair<const void*, const size_t>)>
            operation); */

  /**
   * @brief
   * #Scan operation with user-defined template type.
   * @tparam T
   * T must be Trivially Copyable and Constructable.
   * This is because LineairDB is a Key-value storage but not a object storage.
   *
   * @param begin
   * @param end
   * @param operation
   * @return std::optional<size_t>
   */
  /*   template <typename T>
    const std::optional<size_t> Scan(
        const std::string_view begin, const std::optional<std::string_view> end,
        std::function<bool(std::string_view, T)> operation) {
      static_assert(std::is_trivially_copyable<T>::value == true,
                    "LineairDB expects to trivially copyable types.");
      return Scan(begin, end, [&](auto key, auto pair) {
        const T copy_constructed = *reinterpret_cast<const T*>(pair.first);
        return operation(key, copy_constructed);
      });
    } */

  // Scan primary index in a specific table
  const std::optional<size_t> Scan(
      const std::string_view begin, const std::optional<std::string_view> end,
      std::function<bool(std::string_view,
                         const std::pair<const void*, const size_t>)>
          operation);

  template <typename T>
  const std::optional<size_t> Scan(
      const std::string_view begin, const std::optional<std::string_view> end,
      std::function<bool(std::string_view, T)> operation) {
    static_assert(std::is_trivially_copyable<T>::value == true,
                  "LineairDB expects to trivially copyable types.");
    return Scan(begin, end, [&](auto key, auto pair) {
      const T copy_constructed = *reinterpret_cast<const T*>(pair.first);
      return operation(key, copy_constructed);
    });
  }

  const std::optional<size_t> ScanSecondaryIndex(
      const std::string_view index_name, const std::string_view begin,
      const std::optional<std::string_view> end,
      std::function<bool(std::string_view, const std::vector<std::string>)>
          operation);

  template <typename T>
  const std::optional<size_t> ScanSecondaryIndex(
      const std::string_view index_name, const std::string_view begin,
      const std::optional<std::string_view> end,
      std::function<bool(std::string_view, const std::vector<T>)> operation) {
    static_assert(std::is_trivially_copyable<T>::value == true,
                  "LineairDB expects to trivially copyable types.");
    return ScanSecondaryIndex(
        index_name, begin, end,
        [&](auto key, std::vector<std::string> primary_keys) {
          std::vector<T> copy_constructed_results;
          for (auto& primary_key : primary_keys) {
            copy_constructed_results.push_back(
                *reinterpret_cast<const T*>(primary_key.data()));
          }
          return operation(key, copy_constructed_results);
        });
  }

  void UpdateSecondaryIndex(const std::string_view index_name,
                            const std::any& old_key, const std::any& new_key,
                            const std::byte primary_key_buffer[],
                            const size_t primary_key_size);

  template <typename T>
  void UpdateSecondaryIndex(const std::string_view index_name,
                            const std::any& old_key, const std::any& new_key,
                            const T& primary_key) {
    static_assert(std::is_trivially_copyable<T>::value == true,
                  "LineairDB expects to read/write trivially copyable types.");
    std::byte buffer[sizeof(T)];
    std::memcpy(buffer, &primary_key, sizeof(T));
    UpdateSecondaryIndex(index_name, old_key, new_key, buffer, sizeof(T));
  }

  bool ValidateSKNotNull();

  /**
   * @brief
   * Abort this transaction manually.
   */
  void Abort();

  /**
   * @brief
   * Sets the table to read/write/scan from this transaction.
   * If the table is not set, this transaction uses the `__anonymous_table`,
   * such that defined in the `Config::anonymous_table_name`. See
   * include/lineairdb/config.h.
   * @param[in] table_name The table name to select.
   * @return true when the specified table exists and becomes the current table.
   * @return false when the specified table does not exist; the current
   * selection remains unchanged.
   * @note This function does not create tables; use Database::CreateTable.
   */
  bool SetTable(const std::string_view table_name);

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
