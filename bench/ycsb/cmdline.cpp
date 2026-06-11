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

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cxxopts.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <thread>

#include "workload.h"

namespace YCSB {

void PopulateDatabase(LineairDB::Database&, YCSB::Workload&, size_t);
rapidjson::Document RunBenchmark(LineairDB::Database&, YCSB::Workload&, bool);

}  // namespace YCSB

const std::map<std::string, LineairDB::Config::ConcurrencyControl> Protocols = {
    {"Silo", LineairDB::Config::ConcurrencyControl::Silo},
    {"SiloNWR", LineairDB::Config::ConcurrencyControl::SiloNWR},
    {"2PL", LineairDB::Config::ConcurrencyControl::TwoPhaseLocking},
};

int main(int argc, char** argv) {
  cxxopts::Options options(
      "ycsb",
      "YCSB: Yahoo! Cloud serving benchmark for multi-key transactions");

  options.add_options()          //
      ("h,help", "Print usage")  //
      ("R,records", "Scale factor of YCSB: the number of records in the table",
       cxxopts::value<size_t>()->default_value("100000"))  //
      ("C,contention", "Skew parameter for zipfian distribution",
       cxxopts::value<double>()->default_value("0.5"))  //
      ("w,workload", "Workload",
       cxxopts::value<std::string>()->default_value("a"))  //
      ("c,cc", "Concurrency control protocol",
       cxxopts::value<std::string>()->default_value("SiloNWR"))  //
      ("l,log", "Enable logging",
       cxxopts::value<bool>()->default_value("false"))  //
      ("P,checkpoint", "Enable checkpointing",
       cxxopts::value<bool>()->default_value("false"))  //
      ("i,checkpoint_interval", "Checkpoint interval",
       cxxopts::value<size_t>()->default_value("30"))  //
      ("r,rehash_threshold", "Rehash threshold of the hash index (percent)",
       cxxopts::value<double>()->default_value("0.75"))  //
      ("s,ws", "Size of working set for each transaction",
       cxxopts::value<size_t>()->default_value("4"))  //
      ("e,epoch", "Size of epoch duration",
       cxxopts::value<size_t>()->default_value("40"))  //
      ("p,payload", "Size (bytes) of each record",
       cxxopts::value<size_t>()->default_value("8"))  //
      ("t,thread", "The number of threads working on LineairDB",
       cxxopts::value<size_t>()->default_value("1"))  //
      ("q,clients", "The number of threads queueing the jobs into LineairDB",
       cxxopts::value<size_t>()->default_value("1"))  //
      // std::to_string(std::thread::hardware_concurrency())))  //
      ("H,handler",
       "Use handler interface: queueing threads also execute transactions",
       cxxopts::value<bool>()->default_value("false"))  //
      ("d,duration", "Measurement duration of this benchmark (milliseconds)",
       cxxopts::value<size_t>()->default_value("2000"))  //
      ("L,latency", "Measure transaction latency",
       cxxopts::value<bool>()->default_value("false"))  //
      ("o,output", "Output JSON filename",
       cxxopts::value<std::string>()->default_value("ycsb_result.json"))  //
      ;

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    std::cout << options.help() << std::endl;
    exit(0);
  }

  std::filesystem::remove_all("lineairdb_logs");

  /** Initialize LineairDB **/
  LineairDB::Config config;
  auto protocol = result["cc"].as<std::string>();
  config.concurrency_control_protocol = Protocols.find(protocol)->second;
  config.enable_recovery = false;
  config.enable_logging = result["log"].as<bool>();
  config.max_thread = result["thread"].as<size_t>();
  config.epoch_duration_ms = result["epoch"].as<size_t>();
  config.checkpoint_period = result["checkpoint_interval"].as<size_t>();
  config.rehash_threshold = result["rehash_threshold"].as<double>();

  const auto use_handler = result["handler"].as<bool>();

  /** Configure the workload **/
  auto workload_type = result["workload"].as<std::string>();
  YCSB::Workload workload =
      YCSB::Workload::GeneratePredefinedWorkload(workload_type);

  workload.recordcount = result["records"].as<size_t>();
  workload.zipfian_theta = result["contention"].as<double>();
  workload.reps_per_txn = result["ws"].as<size_t>();
  workload.payload_size = result["payload"].as<size_t>();
  workload.client_thread_size = result["clients"].as<size_t>();
  workload.measurement_duration = result["duration"].as<size_t>();
  workload.measure_latency = result["latency"].as<bool>();

  rapidjson::Document result_json(rapidjson::kObjectType);

  {
    // Scope the Database instance so that its destructor (which joins the
    // checkpoint manager thread and WAL flush thread) completes *before* we
    // read /proc/self/io for write_bytes.  This is critical for Checkpoint
    // durability where writes happen on a background thread.
    LineairDB::Database db(config);

    /** Populate the table **/
    YCSB::PopulateDatabase(db, workload, std::thread::hardware_concurrency());

    /** Run the benchmark **/
    result_json = YCSB::RunBenchmark(db, workload, use_handler);

  }  // <-- db destructs here: checkpoint thread joined, WAL flushed

  // Now read write_bytes from /proc/self/io after all background I/O is done.
  uint64_t write_bytes = 0;
  {
    std::ifstream proc_io("/proc/self/io");
    if (proc_io.is_open()) {
      std::string line;
      while (std::getline(proc_io, line)) {
        if (line.rfind("write_bytes:", 0) == 0) {
          write_bytes = std::stoull(line.substr(12));
          break;
        }
      }
    }
  }

  // Update the JSON with the final write_bytes and derived metrics.
  auto& allocator = result_json.GetAllocator();
  uint64_t total_commits = result_json["commits"].GetUint64();
  uint64_t payload_size  = workload.payload_size;
  uint64_t recordcount   = workload.recordcount;

  // Compute average key length dynamically based on record count
  double avg_key_len = 0.0;
  if (recordcount > 0) {
    uint64_t total_key_chars = 0;
    for (uint64_t i = 0; i < recordcount; ++i) {
      total_key_chars += std::to_string(i).length();
    }
    avg_key_len = static_cast<double>(total_key_chars) / recordcount;
  } else {
    avg_key_len = 5.0; // fallback
  }

  // Calculate the average number of write operations per transaction based on workload
  double write_proportion = (static_cast<double>(workload.update_proportion) +
                             static_cast<double>(workload.insert_proportion) +
                             static_cast<double>(workload.rmw_proportion)) / 100.0;
  double avg_writes_per_tx = workload.reps_per_txn * write_proportion;
  double logical_bytes_per_tx = avg_writes_per_tx * (avg_key_len + payload_size);

  // write_bytes_per_commit: bytes written to storage per committed transaction
  double write_bytes_per_commit = (total_commits > 0)
      ? static_cast<double>(write_bytes) / total_commits
      : 0.0;

  // write_amplification_factor: bytes written per commit / logical bytes written per commit
  double write_amplification_factor = (logical_bytes_per_tx > 0.0)
      ? write_bytes_per_commit / logical_bytes_per_tx
      : 0.0;

  // Overwrite the sentinel write_bytes value set in RunBenchmark
  result_json["write_bytes"].SetUint64(write_bytes);
  result_json.AddMember("write_bytes_per_commit", write_bytes_per_commit, allocator);
  result_json.AddMember("write_amplification_factor", write_amplification_factor, allocator);

  result_json.AddMember("workload",
                        rapidjson::Value(workload_type.c_str(), allocator),
                        allocator);
  result_json.AddMember(
      "protocol", rapidjson::Value(protocol.c_str(), allocator), allocator);
  result_json.AddMember("threads", static_cast<uint64_t>(config.max_thread),
                        allocator);
  result_json.AddMember(
      "clients", static_cast<uint64_t>(workload.client_thread_size), allocator);
  result_json.AddMember("handler", use_handler, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  result_json.Accept(writer);
  writer.Flush();

  auto result_string = buffer.GetString();
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
