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

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cxxopts.hpp>
#include <experimental/filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <set>
#include <thread>
#include <variant>

#include "lock/impl/readers_writers_lock.hpp"
#include "lock/impl/ttas_lock.hpp"
#include "spdlog/spdlog.h"

template <typename T>
size_t benchmark(size_t threads, size_t duration) {
  T lock;
  std::atomic<bool> end_flag(false);
  std::atomic<size_t> total_succeed(0);
  std::vector<std::future<void>> futures;
  for (size_t i = 0; i < threads; i++) {
    futures.push_back(std::async(std::launch::async, [&]() {
      size_t operation_succeed = 0;
      for (;;) {
        if (end_flag.load()) {
          total_succeed.fetch_add(operation_succeed);
          break;
        };
        auto lock_type = T::LockType::Exclusive;
        if constexpr (T::IsReadersWritersLockingAlgorithm()) {
          const bool half_random  = operation_succeed % 2;
          half_random ? lock_type = T::LockType::Shared
                      : lock_type = T::LockType::Exclusive;
        }
        lock.Lock(lock_type);
        std::this_thread::yield();
        lock.UnLock();
        operation_succeed++;
      }
    }));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(duration));
  end_flag.store(true);
  for (auto& fut : futures) { fut.wait(); }
  return total_succeed.load();
}

int main(int argc, char** argv) {
  cxxopts::Options options("lockbench",
                           "Microbenchmark of various locking algortihms");

  options.add_options()          //
      ("h,help", "Print usage")  //
      ("t,thread", "The number of threads working on LineairDB",
       cxxopts::value<size_t>()->default_value(
           std::to_string(std::thread::hardware_concurrency())))  //
      ("a,algorithm", "Locking algorithm",
       cxxopts::value<std::string>()->default_value("TTASLock"))  //
      ("d,duration", "Measurement duration of this benchmark (milliseconds)",
       cxxopts::value<size_t>()->default_value("2000"))  //
      ("o,output", "Output JSON filename",
       cxxopts::value<std::string>()->default_value(
           "lockbench_result.json"))  //
      ;

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    std::cout << options.help() << std::endl;
    exit(0);
  }

  const uint64_t threads          = result["thread"].as<size_t>();
  const auto measurement_duration = result["duration"].as<size_t>();
  const auto algorithm            = result["algorithm"].as<std::string>();

  /** run benchmark **/
  auto ops = 0;

  {
    using namespace LineairDB::Lock;

    if (algorithm == "TTASLock") {
      ops = benchmark<TTASLock>(threads, measurement_duration);
    } else if (algorithm == "TTASLockBO") {
      ops = benchmark<TTASLockBO>(threads, measurement_duration);
    } else if (algorithm == "TTASLockCO") {
      ops = benchmark<TTASLockBO>(threads, measurement_duration);
    } else if (algorithm == "TTASLockBOCO") {
      ops = benchmark<TTASLockBOCO>(threads, measurement_duration);
    } else if (algorithm == "ReadersWritersLock") {
      ops = benchmark<ReadersWritersLock>(threads, measurement_duration);
    } else if (algorithm == "ReadersWritersLockBO") {
      ops = benchmark<ReadersWritersLockBO>(threads, measurement_duration);
    } else if (algorithm == "ReadersWritersLockCO") {
      ops = benchmark<ReadersWritersLockCO>(threads, measurement_duration);
    } else if (algorithm == "ReadersWritersLockBOCO") {
      ops = benchmark<ReadersWritersLockBOCO>(threads, measurement_duration);
    } else {
      std::cout << "invalid algorithm name." << std::endl
                << options.help() << std::endl;
      return EXIT_FAILURE;
    }
  }
  SPDLOG_INFO("Lockbench: measurement has finisihed.");
  SPDLOG_INFO(
      "Algorithm: {0} Operations per "
      "seconds (ops): {1}",
      algorithm, ops);

  /** Output result as json format **/
  rapidjson::Document result_json(rapidjson::kObjectType);
  auto& allocator = result_json.GetAllocator();
  result_json.AddMember(
      "algorithm", rapidjson::Value(algorithm.c_str(), allocator), allocator);
  result_json.AddMember("threads", threads, allocator);
  result_json.AddMember("ops", ops, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  result_json.Accept(writer);
  writer.Flush();

  auto result_string   = buffer.GetString();
  auto output_filename = result["output"].as<std::string>();
  std::ofstream output_f(output_filename,
                         std::ofstream::out | std::ofstream::trunc);
  output_f << result_string;
  if (!output_f.good()) {
    std::cerr << "Unable to write output file" << output_filename << std::endl;
    exit(1);
  }
  std::cout << "This benchmark result is saved into " << output_filename
            << std::endl;
  return 0;
}
