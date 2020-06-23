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

#ifndef LINEAIRDB_EPOCH_FRAMEWORK_H_
#define LINEAIRDB_EPOCH_FRAMEWORK_H_

#include <assert.h>

#include <atomic>
#include <thread>

#include "spdlog/spdlog.h"
#include "util/thread_key_storage.h"

namespace LineairDB {

/**
 * @brief
 * Generic framework for thread-safely synchronizing objects.
 * It provides the concept of epoch, the monotonically increasing number which
 * is shared by all threads.
 * This number ensures the thread-safely object deletion.
 * When a thread see that an epoch number of an object is same with the return
 * value of #current_epoch, it means that the object may be accessed
 * simultaneously by the other threads and it is dangerous to do free/delete
 * into the object.
 * @see [Silo]: https://dl.acm.org/doi/10.1145/2517349.2522713
 * @see [FASTER]:
 * https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf
 */
class EpochFramework {
 public:
  static constexpr EpochNumber THREAD_OFFLINE = UINT32_MAX;

 public:
  EpochFramework(size_t epoch_duration_ms = 40)
      : start_(false),
        stop_(false),
        global_epoch_(1),
        epoch_writer_([=]() { EpochWriterJob(epoch_duration_ms); }) {}
  EpochFramework(size_t epoch_duration_ms,
                 std::function<void(EpochNumber)>&& pt)
      : start_(false),
        stop_(false),
        global_epoch_(1),
        publish_target_(pt),
        epoch_writer_([=]() { EpochWriterJob(epoch_duration_ms); }) {}

  ~EpochFramework() { Stop(); }

  void SetGlobalEpoch(const EpochNumber epoch) { global_epoch_.store(epoch); }

  EpochNumber GetGlobalEpoch() { return global_epoch_.load(); }
  EpochNumber& GetMyThreadLocalEpoch() {
    EpochNumber* my_epoch =
        tls_.Get<EpochNumber>([]() { return THREAD_OFFLINE; });
    return *my_epoch;
  }

  void MakeMeOnline() {
    EpochNumber* my_epoch =
        tls_.Get<EpochNumber>([]() { return THREAD_OFFLINE; });
    assert(*my_epoch == THREAD_OFFLINE);
    *my_epoch = GetGlobalEpoch();
  }

  void MakeMeOffline() {
    EpochNumber* my_epoch =
        tls_.Get<EpochNumber>([]() { return THREAD_OFFLINE; });
    assert(*my_epoch != THREAD_OFFLINE);
    *my_epoch = THREAD_OFFLINE;
  }

  EpochNumber Sync() {
    assert(GetMyThreadLocalEpoch() == THREAD_OFFLINE);
    size_t reload_count = 0;
    for (;;) {
      auto current_epoch = global_epoch_.load();
      auto reload_epoch  = global_epoch_.load();
      while (current_epoch == reload_epoch) {
        std::this_thread::yield();
        reload_epoch = global_epoch_.load();
      }
      reload_count++;

      // Note that each thread always belongs to either one of the two epochs,
      // the old one and the one that matches the global epoch.
      // The first while loop waits for all threads that are in the old epoch
      // when Sync() is called. The next while loop waits for all threads to
      // progress to the next epoch.

      if (reload_count == 2) return reload_epoch;
    }
  }

  void Start() { start_.store(true); }
  void Stop() {
    stop_.store(true);
    if (epoch_writer_.joinable()) epoch_writer_.join();
  }

 public:
  uint32_t GetSmallestEpoch() {
    uint32_t min_epoch = THREAD_OFFLINE;
    tls_.ForEach([&](const EpochNumber* local_epoch) {
      const EpochNumber e = *local_epoch;
      if (0 < e && e < min_epoch) { min_epoch = e; }
    });

    return min_epoch;
  }

  void EpochWriterJob(size_t epoch_duration_ms) {
    const uint64_t epoch_duration = epoch_duration_ms * 1000 * 1000;
    while (!start_.load()) std::this_thread::yield();

    for (;;) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(epoch_duration));
      EpochNumber min_epoch = GetSmallestEpoch();
      EpochNumber old_epoch = global_epoch_;
      if (min_epoch == THREAD_OFFLINE || min_epoch == old_epoch) {
        EpochNumber updated = global_epoch_.fetch_add(1);
        if (publish_target_) publish_target_(updated);
      }
      if (stop_.load() && min_epoch == UINT32_MAX) break;
    }
  }

 private:
  std::atomic<bool> start_;
  std::atomic<bool> stop_;
  std::atomic<EpochNumber> global_epoch_;
  const std::function<void(EpochNumber)> publish_target_;
  std::thread epoch_writer_;
  ThreadKeyStorage<EpochNumber> tls_;
};

}  // namespace LineairDB
#endif
