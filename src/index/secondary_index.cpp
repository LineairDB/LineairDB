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

#include "index/secondary_index.h"

namespace LineairDB
{
    namespace Index
    {
        SecondaryIndex::SecondaryIndex(EpochFramework &epoch_framework, Config config,
                                       WriteSetType /*recovery_set*/)
            : secondary_index_(std::make_unique<HashTableWithPrecisionLockingIndex<PKList>>(config, epoch_framework)),
              epoch_manager_ref_(epoch_framework) {}

        SecondaryIndex::PKList *SecondaryIndex::GetPKList(const std::string_view key)
        {
            return secondary_index_->Get(key);
        }

        bool SecondaryIndex::AddPK(const std::string_view key, const std::string_view pk)
        {
            bool result = secondary_index_->Put(key, {pk});
            if (!result)
            {
                auto *pk_list = secondary_index_->Get(key);
                if (pk_list != nullptr)
                {
                    pk_list->push_back(pk);
                }
                else
                {
                    return false;
                }
            }
            return true;
        }

    }
}
