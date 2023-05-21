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

/**
 * YCSB Benchmark: workload [a, b, c, d, e]
 *
 * implementation references:
 * https://github.com/brianfrankcooper/YCSB/wiki
 * https://www.cs.duke.edu/courses/fall13/cps296.4/838-CloudPapers/ycsb.pdf
 *
 * NOTE:
 * in this repository, `table` and `key` in YCSB Benchmark are concatenated,
 * because LineairDB provides only key/value storage access interfaces
 * like `put` and `get` .
 * Some previous also works do the same modification:
 * Silo(SoSP '13), FOEDUS(SIGMOD '15), and RedisClient in
 * github.com/brianfrankcooper/YCSB.
 *
 * NOTE:
 * `field` in YCSB Benchmark is also concatenated with `buffer`,
 * for the same reason as described above.
 * As the same with github.com/YCSB/memcached,
 * we assumes that fields are converted to strings such as JSON/XML/YAML, and
 * stored into `buffer`.
 */

#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>
#include <rapidjson/document.h>

#include <atomic>
#include <chrono>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "interface.h"
#include "random_generator.hpp"
#include "spdlog/spdlog.h"
#include "util/thread_key_storage.h"
#include "workload.h"

namespace YCSB {

void PopulateDatabase(LineairDB::Database& db, Workload& workload,
                      size_t worker_threads) {
  std::vector<int> failed;
  std::vector<std::thread> workers;

  worker_threads = std::min(workload.recordcount, worker_threads);
  workers.reserve(worker_threads);
  for (size_t i = 0; i < worker_threads; ++i) {
    size_t from = workload.recordcount * i / worker_threads;
    size_t to   = workload.recordcount * (i + 1) / worker_threads;
    failed.emplace_back(0);
    workers.emplace_back([&, from, to]() {
      db.ExecuteTransaction(
          [&, from, to](LineairDB::Transaction& tx) {
            std::byte ptr[workload.payload_size];
            for (size_t idx = from; idx < to; idx++) {
              tx.Write(std::to_string(idx), ptr, workload.payload_size);
            }
          },
          [](LineairDB::TxStatus status) {
            if (status != LineairDB::TxStatus::Committed) {
              SPDLOG_ERROR("YCSB: a database population query is aborted");
              exit(1);
            }
          });
    });
  }
  SPDLOG_INFO("YCSB: Database population queries are enqueued");
  for (auto& w : workers) { w.join(); }
  db.Fence();
  SPDLOG_INFO("YCSB: Database population is completed");
}

struct ThreadLocalResult {
  size_t commits = 0;
  size_t aborts  = 0;
};
ThreadKeyStorage<ThreadLocalResult> thread_local_result;
std::atomic<bool> finish_flag{false};

void ExecuteWorkload(LineairDB::Database& db, Workload& workload,
                     RandomGenerator* rand, void* payload,
                     bool use_handler = true) {
  std::function<void(LineairDB::Transaction&, std::string_view,
                     std::string_view, void*, size_t)>
      operation;

  bool is_scan = false;
  bool is_insert = false;
  {  // choose operation what I do
    size_t what_i_do  = rand->UniformRandom(99);
    size_t proportion = 0;

    if (what_i_do < (proportion += workload.read_proportion)) {
      operation = YCSB::Interface::Read;
    } else if (what_i_do < (proportion += workload.update_proportion)) {
      operation = YCSB::Interface::Update;
    } else if (what_i_do < (proportion += workload.insert_proportion)) {
      operation = YCSB::Interface::Insert;
      is_insert = true;
    } else if (what_i_do < (proportion += workload.scan_proportion)) {
      operation = YCSB::Interface::Scan;
      is_scan   = true;
    } else if (what_i_do < (proportion += workload.rmw_proportion)) {
      operation = YCSB::Interface::ReadModifyWrite;
    } else {
      SPDLOG_ERROR("No operation has found");
      exit(1);
    }
  }

  std::vector<std::string> keys(workload.reps_per_txn);

  // choose target key
  for (size_t i = 0; i < workload.reps_per_txn; i++) {
    if (is_insert) {
      keys.emplace_back(std::to_string(RandomGenerator::XAdd()));
    } else {
      if (workload.distribution == Distribution::Uniform) {
        keys.emplace_back(std::to_string(rand->UniformRandom()));
      } else if (workload.distribution == Distribution::Zipfian) {
        keys.emplace_back(std::to_string(rand->Next(workload.has_insert)));
      } else if (workload.distribution == Distribution::Latest) {
        keys.emplace_back(std::to_string(RandomGenerator::LatestNext(rand)));
      }
    }
  }

  if (use_handler) {
    auto& tx = db.BeginTransaction();
    if (is_scan) {
      operation(tx, keys.front(), keys.back(), payload, workload.payload_size);
    } else {
      for (auto& key : keys) {
        operation(tx, key, "", payload, workload.payload_size);
      }
    }
    bool precommitted = db.EndTransaction(tx, [&](LineairDB::TxStatus) {});
    auto* result      = thread_local_result.Get();
    if (!finish_flag.load(std::memory_order_relaxed)) {
      if (precommitted) {
        result->commits++;
      } else {
        result->aborts++;
      }
    }
  } else {
    db.ExecuteTransaction(
        [is_scan, operation, keys, payload,
         workload](LineairDB::Transaction& tx) {
          if (is_scan) {
            operation(tx, keys.front(), keys.back(), payload,
                      workload.payload_size);

          } else {
            for (auto& key : keys) {
              operation(tx, key, "", payload, workload.payload_size);
            }
          }
        },
        [](LineairDB::TxStatus) {},
        [&](LineairDB::TxStatus status) {
          if (!finish_flag.load(std::memory_order_relaxed)) {
            auto* result = thread_local_result.Get();

            if (status == LineairDB::TxStatus::Committed) {
              result->commits++;
            } else {
              result->aborts++;
            }
          }
        });
  }
}

rapidjson::Document RunBenchmark(LineairDB::Database& db, Workload& workload,
                                 bool use_handler = true) {
  std::vector<std::thread> clients;
  std::vector<std::array<std::byte, 512>> buffers(workload.client_thread_size);
  ThreadKeyStorage<RandomGenerator> thread_local_random;

  std::atomic<bool> start_flag(false);
  std::atomic<size_t> waits_count(0);
  for (size_t i = 0; i < workload.client_thread_size; ++i) {
    clients.emplace_back(std::thread([&, i]() {
      RandomGenerator* rand = thread_local_random.Get();
      rand->Init(workload.recordcount, workload.zipfian_theta);

      waits_count.fetch_add(1);

      while (finish_flag.load() == false) {
        ExecuteWorkload(db, workload, rand, &buffers[i][0], use_handler);
      }
    }));
  }

  // start measurement
  while (waits_count.load() != clients.size()) { std::this_thread::yield(); }

  SPDLOG_INFO("YCSB: Benchmark start.");
  auto begin = std::chrono::high_resolution_clock::now();
  start_flag.store(true);
  std::this_thread::sleep_for(
      std::chrono::milliseconds(workload.measurement_duration));
  finish_flag.store(true);
  for (auto& worker : clients) { worker.join(); }
  auto end = std::chrono::high_resolution_clock::now();
  SPDLOG_INFO("YCSB: Benchmark end.");
  db.Fence();
  SPDLOG_INFO("YCSB: DB Fenced.");

  uint64_t total_commits = 0;
  uint64_t total_aborts  = 0;
  thread_local_result.ForEach([&](const ThreadLocalResult* res) {
    total_commits += res->commits;
    total_aborts += res->aborts;
  });

  auto elapsed = end - begin;
  uint64_t milliseconds =
      std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
  uint64_t tps = total_commits * 1000 / milliseconds;

  SPDLOG_INFO(
      "YCSB: Benchmark completed. elapsed time: {3}ms, commits: {0}, aborts: "
      "{1}, tps: {2}",
      total_commits, total_aborts, tps, milliseconds);

  rapidjson::Document result_json(rapidjson::kObjectType);
  auto& allocator = result_json.GetAllocator();
  result_json.AddMember("etime", milliseconds, allocator);
  result_json.AddMember("commits", total_commits, allocator);
  result_json.AddMember("aborts", total_aborts, allocator);
  result_json.AddMember("tps", tps, allocator);

  return result_json;
}

}  // namespace YCSB
