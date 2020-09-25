/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.

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

#ifndef LINEAIRDB_TRANSACTION_ID_HPP
#define LINEAIRDB_TRANSACTION_ID_HPP

#include "types/definitions.h"

namespace LineairDB {

struct TransactionId {
  EpochNumber epoch;
  uint32_t tid;

  TransactionId() noexcept : epoch(0), tid(0) {}
  TransactionId(const EpochNumber e, uint32_t t) : epoch(e), tid(t) {}
  // TransactionId(const TransactionId& rhs) : epoch(rhs.epoch), tid(rhs.tid) {}
  TransactionId(const TransactionId& rhs) = default;
  bool IsEmpty() { return (epoch == 0 && tid == 0); }
  TransactionId(uint64_t n) : epoch(n >> 32), tid(n & ~1llu >> 32) {}
  bool operator==(const TransactionId& rhs) {
    return (epoch == rhs.epoch && tid == rhs.tid);
  }
  bool operator!=(const TransactionId& rhs) { return !(*this == rhs); }
  bool operator<(const TransactionId& rhs) {
    if (epoch == rhs.epoch) {
      return tid < rhs.tid;
    } else {
      return epoch < rhs.epoch;
    }
  }
  MSGPACK_DEFINE(epoch, tid);
};

}  // namespace LineairDB
#endif /* LINEAIRDB_TRANSACTION_ID_HPP */
