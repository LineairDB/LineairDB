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

#include <functional>
#include <string>
#include <string_view>

#include "index/precision_locking_index/index.hpp"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "types/snapshot.hpp"
#include "util/epoch_framework.hpp"

namespace LineairDB
{
    namespace Index
    {
        class SecondaryIndex
        {
            using PrimaryKey = std::string_view;
            using PKList = std::vector<PrimaryKey>;

        public:
            SecondaryIndex(EpochFramework &epoch_framework, Config config = Config(),
                           WriteSetType recovery_set = WriteSetType());

            PKList *GetPKList(const std::string_view key);

            bool AddPK(const std::string_view key, const std::string_view pk);

        private:
            // HashTableWithPrecisionLockingIndexこれは無駄かもしれない、secondary, primary両方だと重いのでシンプルにB+treeのみでいいかも
            std::unique_ptr<HashTableWithPrecisionLockingIndex<PKList>> secondary_index_;
            LineairDB::EpochFramework &epoch_manager_ref_;
        };
    } // namespace Index
} // namespace LineairDB

#endif /* LINEAIRDB_CONCURRENT_TABLE_H */
