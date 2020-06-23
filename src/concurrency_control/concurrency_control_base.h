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

#ifndef LINEAIRDB_CONCURRENCY_CONTROL_BASE_H
#define LINEAIRDB_CONCURRENCY_CONTROL_BASE_H

#include <lineairdb/tx_status.h>

#include <cstddef>
#include <string>
#include <string_view>

#include "index/concurrent_table.h"
#include "types.h"

namespace LineairDB {
struct TransactionReferences {
  ReadSetType& read_set_ref_;
  WriteSetType& write_set_ref_;
  const EpochNumber& my_epoch_ref_;
};
class ConcurrencyControlBase {
 public:
  ConcurrencyControlBase(TransactionReferences&& tx) : tx_ref_(tx) {}
  virtual ~ConcurrencyControlBase(){};
  virtual const DataItem Read(std::string_view, DataItem*) = 0;
  virtual void Write(const std::string_view key, const std::byte* const value,
                     const size_t size, DataItem*)         = 0;
  virtual void Abort()                                     = 0;
  virtual bool Precommit()                                 = 0;
  virtual void PostProcessing(TxStatus)                    = 0;

  bool IsReadOnly() { return (0 == tx_ref_.write_set_ref_.size()); }
  bool IsWriteOnly() { return (0 == tx_ref_.read_set_ref_.size()); }

 protected:
  TransactionReferences tx_ref_;
};
}  // namespace LineairDB

#endif /* LINEAIRDB_CONCURRENCY_CONTROL_BASE_H */
