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
#include <iostream>

#include "util/logger.hpp"

namespace LineairDB {

struct DataBuffer {
  inline static size_t DefaultBufferSize = 512;

  std::byte* value;
  size_t size;

  // WARNING: thread unsafe
  static void SetDefaultBufferSize(size_t buf_size) {
    DefaultBufferSize = buf_size;
  }

  DataBuffer() : size(0) { value = new std::byte[DefaultBufferSize]; }

  void Reset(const std::byte* v, const size_t s) {
    if (DefaultBufferSize < s) {
      SPDLOG_ERROR("write buffer overflow. expected: {0}, capacity: {1}", s,
                   DefaultBufferSize);
      std::cerr << "ERROR in DataBuffer: The size of the write value is "
                   "greater than DefaultBufferSize";
      // TODO: use realloc to prevent failure
      exit(EXIT_FAILURE);
    }
    size = s;
    std::memcpy(value, v, s);
  }
  void Reset(const DataBuffer& rhs) { Reset(rhs.value, rhs.size); }
  bool IsEmpty() const { return size == 0; }
};
}  // namespace LineairDB
#endif /* LINEAIRDB_DATA_BUFFER_HPP */
