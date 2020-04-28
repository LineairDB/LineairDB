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

#ifndef LINEAIRDB_DATABASE_H
#define LINEAIRDB_DATABASE_H

#include <lineairdb/transaction.h>

#include <functional>
#include <memory>

#include "config.h"
#include "tx_status.h"

namespace LineairDB {

class Database {
 public:
  /**
   * @brief Construct a new Database object. Thread-unsafe.
   * Note that a default-constructed Config object will be passed.
   */
  Database() noexcept;

  /**
   * @brief Construct a new Database object. Thread-unsafe.
   * @param config See Config for more details of configuration.
   */
  Database(const Config& config) noexcept;

  ~Database() noexcept;
  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;
  Database(Database&&)                 = delete;
  Database& operator=(Database&&) = delete;

  /**
   * @brief Return the Config object set by constructor.
   * Note that the returned Config object is a value not an lvalue reference
   * and you can not reset your config given to the database.
   * Thread-safe.
   */
  const Config GetConfig() const noexcept;

  using ProcedureType = std::function<void(Transaction&)>;
  using CallbackType  = std::function<void(const TxStatus)>;
  /**
   * @brief
   *  LineairDB processes a transaction given by a transaction procedure proc,
   * then returns the result typed TxStatus via the callback fucntion clbk.
   * Thread-safe.
   * @param[in] proc A transaction procedure processed by LineairDB.
   * @param[out] clbk A callback function accepts a result(Committed or
   * Aborted).
   */
  void ExecuteTransaction(ProcedureType proc, CallbackType clbk);

  /**
   * @brief
   * Fence() waits termination of transactions which is currently in progress.
   * You can execute transactions in the order you want by interleaving Fence()
   * between ExecuteTransaction functions. Note that no Fence() call may result
   * in a execution sequence which is not same as the program (invoking) order
   * of the ExecuteTransaction functions. If you know some dependency of
   * transactions (e.g., database population), use this method to order
   * them. Thread-safe.
   */
  void Fence() const noexcept;

 private:
  class Impl;
  const std::unique_ptr<Impl> db_pimpl_;
  friend class Transaction;
};

};  // namespace LineairDB

#endif
