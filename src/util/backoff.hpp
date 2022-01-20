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

#ifndef LINEAIRDB_BACKOFF_HPP
#define LINEAIRDB_BACKOFF_HPP

#include <chrono>
#include <functional>
#include <thread>

namespace LineairDB {
namespace Util {
static inline bool RetryWithExponentialBackoff(std::function<bool()>&& f,
                                               size_t sleep_ns         = 100,
                                               size_t yield_threshold  = 100,
                                               size_t retire_threshold = 0) {
  size_t try_count = 0;
  for (;;) {
    if (f()) return true;
    try_count++;
    if (0 < retire_threshold && retire_threshold < try_count) { return false; }
    if (yield_threshold < try_count) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_ns));
      sleep_ns *= 2;
    } else {
      std::this_thread::yield();
    }
  }
}

}  // namespace Util
}  // namespace LineairDB

#endif /* LINEAIRDB_BACKOFF_HPP */
