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

#ifndef LINEAIRDB_DATA_BUFFER_HPP
#define LINEAIRDB_DATA_BUFFER_HPP

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include "util/logger.hpp"

namespace LineairDB {

struct DataBuffer {
  // TODO enable to change this parameter at the compile time
  constexpr static size_t ValueBufferSize = 512;

  std::byte value[ValueBufferSize];
  size_t size = 0;

  void Reset(const std::byte* v, const size_t s) {
    if (ValueBufferSize < s) {
      SPDLOG_ERROR("write buffer overflow. expected: {0}, capacity: {1}", s,
                   ValueBufferSize);
      exit(EXIT_FAILURE);
    }
    size = s;
    std::memcpy(value, v, s);
  }
  void Reset(const DataBuffer& rhs) { Reset(rhs.value, rhs.size); }
};
}  // namespace LineairDB
#endif /* LINEAIRDB_DATA_BUFFER_HPP */
