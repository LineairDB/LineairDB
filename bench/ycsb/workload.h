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

#ifndef LINEAIRDB_YCSB_WORKLOAD_H
#define LINEAIRDB_YCSB_WORKLOAD_H

#include <atomic>
#include <iostream>
#include <string_view>

namespace YCSB {

enum Distribution { Uniform, Zipfian, Latest };

struct Workload {
  // NOTE: all `proportion` is percentage
  const size_t read_proportion;
  const size_t update_proportion;
  const size_t insert_proportion;
  const size_t scan_proportion;
  const size_t rmw_proportion; // RMW: Read-Modify-Write
  const Distribution distribution;
  size_t recordcount;
  double zipfian_theta;
  bool has_insert;

  // following members are not apper in original YCSB paper
  size_t reps_per_txn;
  size_t payload_size;
  size_t client_thread_size;
  size_t measurement_duration;

  Workload(size_t r, size_t u, size_t i, size_t s, size_t m, Distribution d)
      : read_proportion(r), update_proportion(u), insert_proportion(i), scan_proportion(s),
        rmw_proportion(m), distribution(d) {
    assert((r + u + i + s + m) == 100);
    has_insert = 0 < insert_proportion;
  }

  static Workload GeneratePredefinedWorkload(std::string_view w) {
    if (w == "a") {
      return Workload(50, 50, 0, 0, 0, Distribution::Zipfian);
    } else if (w == "b") {
      return Workload(95, 5, 0, 0, 0, Distribution::Zipfian);
    } else if (w == "c") {
      return Workload(100, 0, 0, 0, 0, Distribution::Zipfian);
    } else if (w == "d") {
      return Workload(95, 0, 5, 0, 0, Distribution::Latest);
    } else if (w == "e") {
      return Workload(0, 0, 5, 95, 0, Distribution::Zipfian);
    } else if (w == "f") {
      return Workload(50, 0, 0, 0, 50, Distribution::Zipfian);
    } else if (w == "f+bw") {
      return Workload(0, 50, 0, 0, 50, Distribution::Zipfian);
    } else if (w == "wo") {
      return Workload(0, 100, 0, 0, 0, Distribution::Zipfian);
    } else {
      std::cerr << "workload " << w << " is not yet impl" << std::endl;
      exit(1);
    }
  }
};
} // namespace YCSB
#endif /* LINEAIRDB_YCSB_WORKLOAD_H */
