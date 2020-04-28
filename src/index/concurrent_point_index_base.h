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

#ifndef LINEAIRDB_CONCURRENT_POINT_INDEX_BASE_H
#define LINEAIRDB_CONCURRENT_POINT_INDEX_BASE_H

#include <functional>
#include <string_view>

#include "types.h"

namespace LineairDB {
namespace Index {

class ConcurrentPointIndexBase {
 public:
  virtual ~ConcurrentPointIndexBase() {}
  virtual DataItem* Get(const std::string_view key)                     = 0;
  virtual bool Put(const std::string_view key, const DataItem* const v) = 0;
  virtual void ForAllWithExclusiveLock(
      std::function<void(const std::string_view, const DataItem*)> f) = 0;
  virtual void Clear()                                                = 0;
};
}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_CONCURRENT_POINT_INDEX_BASE_H */
