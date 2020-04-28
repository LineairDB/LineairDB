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

#ifndef LINEAIRDB_TRANSACTION_IMPL_H
#define LINEAIRDB_TRANSACTION_IMPL_H

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>

#include <memory>
#include <string_view>

#include "concurrency_control/concurrency_control_base.h"
#include "types.h"

namespace LineairDB {

/**
 * @brief
 * Transaction::Impl controls users' requests of the four operation of the page
 * model, and delegate these requests to concurrency control protocols.
 * Note that all concurrency control protocols assumes the followings:
 *   - A transaction's read or write operation into the same data item are
 *     never be duplicated.
 *   - write-after-read into the same data item is valid.
 *     but read-after-write is invalid.
 * It is the important parts of the definition of "schedule" in the theory of
 * transaction proocessing. This class do not handle any correctness such as
 * serializability, but handle the operations to satisfy these two assumptions.
 * To do this end, we implement the followings:
 *   - "read-your-own-writes" (read the version written by the callee
 *      transaction itself)
 *   - "repeatable read" (read the version which has been already read by the
 *      callee transaction itself)
 */
class Transaction::Impl {
  friend class Database::Impl;

 public:
  Impl(Database::Impl*) noexcept;
  ~Impl() noexcept;

  const std::pair<const std::byte* const, const size_t> Read(
      const std::string_view key);
  void Write(const std::string_view key, const std::byte value[],
             const size_t size);
  void Abort();
  bool Precommit();

 private:
  bool user_aborted_;
  Database::Impl* db_pimpl_;
  const Config& config_ref_;
  std::unique_ptr<ConcurrencyControlBase> concurrency_control_;

  ReadSetType read_set_;
  WriteSetType write_set_;
};
}  // namespace LineairDB
#endif /* LINEAIRDB_TRANSACTION_IMPL_H */
