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

#ifndef LINEAIRDB_SECONDARY_INDEX_H
#define LINEAIRDB_SECONDARY_INDEX_H

#include <lineairdb/config.h>
#include <lineairdb/i_secondary_index.h>

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "index/precision_locking_index/index.hpp"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "types/snapshot.hpp"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

template <typename K>
class SecondaryIndex : public ISecondaryIndex {
 public:
  using KeyType = K;

  SecondaryIndex(EpochFramework& epoch_framework, Config config = Config(),
                 bool is_unique = false,
                 [[maybe_unused]] WriteSetType recovery_set = WriteSetType())
      : secondary_index_(
            std::make_unique<HashTableWithPrecisionLockingIndex<PKList>>(
                config, epoch_framework)),
        epoch_manager_ref_(epoch_framework),
        is_unique_(is_unique) {}

  const std::type_info& KeyTypeInfo() const override { return typeid(KeyType); }

  PKList* GetPKList(std::string_view serialized_key) override {
    return secondary_index_->Get(serialized_key);
  }

  // TODO: implement unique constraint check
  bool AddPK(std::string_view serialized_key, std::string_view pk) override {
    if (is_unique_) {
      // check if the key is already in the index
      auto* pk_list = secondary_index_->Get(serialized_key);
      if (pk_list != nullptr) {
        return false;
      }
    }

    bool result = secondary_index_->Put(serialized_key, {pk});
    if (!result) {
      auto* pk_list = secondary_index_->Get(serialized_key);
      if (pk_list != nullptr) {
        pk_list->push_back(pk);
      } else {
        return false;
      }
    }
    return true;
  }

  bool IsUnique() const override { return is_unique_; }

 private:
  std::unique_ptr<HashTableWithPrecisionLockingIndex<PKList>> secondary_index_;
  LineairDB::EpochFramework& epoch_manager_ref_;
  bool is_unique_;
};

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_SECONDARY_INDEX_H */
