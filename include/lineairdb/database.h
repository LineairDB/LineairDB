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
#include <optional>

#include "config.h"
#include "tx_status.h"

namespace LineairDB {

class Database {
 public:
  /**
   * @brief Construct a new Database object. Thread-safe.
   * Note that a default-constructed Config object will be passed.
   */
  Database() noexcept;

  /**
   * @brief Construct a new Database object. Thread-safe.
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
   * @return Config object. Note that it is not an lvalue reference
   * and you cannot change the configuration with it.
   * Thread-safe.
   */
  const Config GetConfig() const noexcept;

  using ProcedureType = std::function<void(Transaction&)>;
  using CallbackType  = std::function<void(const TxStatus)>;
  /**
   * @brief
   * Processes a transaction given by a transaction procedure proc,
   * and afterwards process callback function with the resulting TxStatus.
   * It enqueues these two functions into LineairDB's thread pool.
   * Thread-safe.
   * @param[in] proc A transaction procedure processed by LineairDB.
   * @param[out] commit_clbk A callback function accepts a result (Committed or
   * Aborted) of this transaction.
   * @param[out] precommit_clbk A callback function accepts a result
   * (Precommitted or Aborted) of this transaction.
   * Note that pre-committed transactions have not been committed. Since the
   * recovery log has not been persisted, this transaction may be aborted. The
   * callback is good for describing  transaction dependencies. If a transaction
   * is aborted, it is guaranteed that the other  transactions, that are
   executed
   * after checking the pre-commit of the transaction, will abort.
   */
  void ExecuteTransaction(
      ProcedureType proc, CallbackType commit_clbk,
      std::optional<CallbackType> precommit_clbk = std::nullopt);

  /**
   * @brief
   * Creates a new transaction.
   * Via this interface, the callee thread of this method can manipulate
   * LineairDB's key-value storage directly. Note that the behavior and
   * performance characteristics will affect from the selected callback manager
   * and log manager; For example, if you have set Config::CallbackManager to
   * ThreadLocal, the callee thread of this method may have to call
   * Database::RequestCommit more frequently, in order to resolve the congestion
   * of the thread-local commit callback queue.
   *
   * @return Transaction
   */
  Transaction& BeginTransaction();

  /**
   * @brief
   * Terminates the transaction.
   * If Transaction::Abort has not been called, LineairDB tries to commit `tx`.
   * @pre To achieve user abort, Transaction::Abort must be called before this
   * method.
   * @post The first argument `tx` might have been deleted.
   * @param[in] tx A transaction wants to terminate.
   * @param[out] clbk A callback function accepts a result (Committed or
   * @return true if the LineairDB's concurrency control protocol **decides** to
   * commit the given `tx`. Note that it does not mean that `tx has been
   * committed`; `tx` will be committed if it goes without crash, disaster, or
   * something accident, however, the commit cannot be determined until
   * recoverability is guaranteed by persistent logs. Use clbk to find out
   * whether you have really committed or not.
   * @return false if the LineairDB's concurrency control protocol decides to
   * abort the given `tx`. In contrast with the true case, this result will not
   * be overturned.
   */
  bool EndTransaction(Transaction& tx, CallbackType clbk);

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

  /**
   * @brief
   * WaitForCheckpoint
   * Waits for the completion of the next checkpoint.
   * Note that this method may take longer than the time specified in
   * `LineairDB::Config.checkpoint_period (default value: 30 seconds)` in the
   * worst cases. When the WAL (logging) is disabled and durability is
   * guaranteed by only checkpointing, this interface is preferable; it ensures
   *that the currently active transactions are durable.
   */
  void WaitForCheckpoint() const noexcept;

  /**
   * @brief
   * Requests executions of callback functions of already completed (committed
   * or (aborted) transactions. Note that LineairDB's callback queues may be
   * overloading in some combination of configurations (e.g., too long epoch
   * size, too small thread-pool size, or weird implementation of the selected
   * CallbackManager).
   */
  void RequestCallbacks();

 private:
  class Impl;
  const std::unique_ptr<Impl> db_pimpl_;
  friend class Transaction;
};

};  // namespace LineairDB

#endif
