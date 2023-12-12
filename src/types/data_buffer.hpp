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
#include <new>

#include "util/logger.hpp"

namespace LineairDB {

struct DataBuffer {
  std::byte* value;
  size_t size;

  DataBuffer() : size(0) { value = nullptr; }
  ~DataBuffer() {
    if (value != nullptr) delete[] value;
  }

  void Reset(const std::byte* v, const size_t s) {
    if (v == nullptr){
      delete[] value;
      value = nullptr;
      size = 0;
    }
    if (size < s) {
      if (value == nullptr) {
        value = new std::byte[s];
      } else {
        value = static_cast<decltype(value)>(
            std::realloc(reinterpret_cast<void*>(value), s));
        if (value == nullptr) { throw std::bad_alloc(); }
      }
    }
    size = s;
    std::memcpy(value, v, s);
  }
  void Reset(const DataBuffer& rhs) { Reset(rhs.value, rhs.size); }
  void Reset(const std::string& rhs) {
    Reset(reinterpret_cast<const std::byte*>(rhs.data()), rhs.size());
  }
  bool IsEmpty() const { return size == 0; }

  std::string toString() const {
    return std::string(reinterpret_cast<char*>(value), size);
  }
};
}  // namespace LineairDB
#endif /* LINEAIRDB_DATA_BUFFER_HPP */
