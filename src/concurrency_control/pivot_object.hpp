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

#ifndef LINEAIRDB_CONCURRENCY_CONTROL_NWR_PIVOT_OBJECT_HPP
#define LINEAIRDB_CONCURRENCY_CONTROL_NWR_PIVOT_OBJECT_HPP

#include <assert.h>

#include "util/32bit_set.hpp"

namespace LineairDB {

enum NWRValidationResult {
  ACYCLIC = 0,  // LineairDB can execute commit with NWR's version order.
  RW = 2,  // LineairDB should abort with NWR's version order: there may exist
           // a path T_k <<(rw)-> T_j.
  WR = 3,  //  LineairDB should abort with NWR's version order : there may
           //  exist a path T_k wr-> T_j.
  ANTI_DEPENDENCY = 4,  //  LineairDB should abort with NWR's version order :
                        //  there may exist a path T_k -> T-j <<(rw)-> T_k.
  LINEARIZABILITY = 5,  // LineairDB should abort with NWR's version order:
                        // this version order violates strict serializability.
  NOT_YET_VALIDATED = 6
};

/**
 * @brief PivotObject used by NWR-protocols.
 * @ref index
 * @note On some environment that does not provide 128-bits atomic operations
 *  such as CMPXCHG16B, manipulating the PivotObject instances may result in
 *  the pefromance degradation.
 *
 */
class NWRPivotObject {
 public:
  using VersionedSet = HalfWordSet<4>;

  struct Versions {
    uint32_t target_id;
    uint32_t epoch;
    Versions() noexcept : target_id(0), epoch(0) {}

    bool operator==(const Versions& rhs) noexcept {
      return (target_id == rhs.target_id && epoch == rhs.epoch);
    }
  };

  struct MergedSets {
    VersionedSet rset;
    VersionedSet wset;
    MergedSets() noexcept : rset(0), wset(0) {}

    bool operator==(const MergedSets& rhs) noexcept {
      return (rset == rhs.rset && wset == rhs.wset);
    }
  };

  // Check whether T_k in successors is reachable into Tj.
  // Validation of overwriters depends on baseline protocols.
  NWRValidationResult IsReachableInto(NWRPivotObject& rhs) {
    NWRPivotObject& lhs = *this;
    if (rhs.msets.rset.IsGreaterOrEqualThan(lhs.msets.wset)) {
      return WR;
    }

    if (rhs.msets.wset.IsGreaterThan(lhs.msets.rset)) {
      return RW;
    }
    return ACYCLIC;
  }

  NWRValidationResult IsConcurrentWith(NWRPivotObject& rhs) {
    NWRPivotObject& lhs = *this;
    if (rhs.versions.epoch != lhs.versions.epoch) {
      return LINEARIZABILITY;
    }
    return ACYCLIC;
  }

 public:
  Versions versions;
  MergedSets msets;

  NWRPivotObject() noexcept : versions(), msets() {}
};

}  // namespace LineairDB
#endif
