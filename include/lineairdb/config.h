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

#ifndef LINEAIRDB_CONFIG_H
#define LINEAIRDB_CONFIG_H

#include <cstddef>
#include <cstdlib>
#include <string>
#include <thread>

namespace LineairDB {

/**
 * @brief
 * Configuration and options for LineairDB instances.
 */
struct Config {
  /**
   * @brief
   * The size of thread pool.
   *
   * Default: LineairDB allocates threads as many the return value of
   * std::thread::hardware_concurrency().
   */
  size_t max_thread = std::thread::hardware_concurrency();
  /**
   * @brief
   * The size of epoch duration (milliseconds). See [Tu13, Chandramouli18] to
   * get more details of epoch-based group commit. Briefly, LineairDB
   * concurrently processes transactions included in the same epoch duration. As
   * you set the larger duration to this paramter, you may get a higher
   * throughput. However, this setting results in the increase of average
   * response time.
   *
   * Default: 40ms.
   * @see [Tu13] https://dl.acm.org/doi/10.1145/2517349.2522713
   * @see [Chandramouli18]
   * https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf
   */
  size_t epoch_duration_ms = 40;

  enum ConcurrencyControl { Silo, SiloNWR, TwoPhaseLocking };
  /**
   * @brief
   * Set a concurrency control algorithm.
   * See LineairDB::Config::ConcurrencyControl for the enum options of this
   * configuration.
   *
   * Default: SiloNWR
   */
  ConcurrencyControl concurrency_control_protocol = SiloNWR;

  enum Logger { ThreadLocalLogger };
  /**
   * @brief
   * Set a logging algorithm.
   * See LineairDB::Config::Logger for the enum options of this
   * configuration.
   *
   * Default: ThreadLocalLogger
   */
  Logger logger = ThreadLocalLogger;

  enum IndexStructure { HashTableWithPrecisionLockingIndex };
  /**
   * @brief
   * Set the type of index.
   * See LineairDB::Config::IndexStructure for the enum options of this
   * configuration.
   *
   * Default: Hash table with precision locking index
   */
  IndexStructure index_structure = HashTableWithPrecisionLockingIndex;

  enum CallbackEngine { ThreadLocal };
  /**
   * @brief
   * Set the type of callback engine.
   * See LineairDB::Config::CallBackEngine for the enum options of this
   * configuration.
   *
   * Default: ThreadLocal
   */
  CallbackEngine callback_engine = ThreadLocal;

  /**
   * @brief
   * If true, LineairDB processes recovery at the instantiation.
   *
   * Default: true
   */
  bool enable_recovery = true;

  /**
   * @brief
   * Durability strategy for LineairDB.
   *
   * - None:                  No durability guarantee.
   * - WAL:                   Write-ahead logging only. Unbounded log file size.
   * - Checkpoint:            Full-scan checkpointing only (CPR-consistency).
   * - CheckpointAndWAL:      Full-scan checkpointing + WAL (default).
   *
   * Default: CheckpointAndWAL
   */
  enum class DurabilityStrategy {
    None,
    WAL,
    Checkpoint,
    CheckpointAndWAL,
  };
  DurabilityStrategy durability = DurabilityStrategy::CheckpointAndWAL;

  /**
   * @brief
   * It uses as the interval time (milliseconds) for checkpointing.
   * The longer is the better for the performance but the larger interval time
   * causes the increasing of log file size.
   *
   * Default: 5000 (5 seconds)
   */
  size_t checkpoint_period = 5000;

  /**
   * @brief
   * If true, CPR durability strategy executes a final checkpoint during
   * database destruction. If false, it simulates an unclean shutdown/crash on
   * destruction (does not write a final checkpoint).
   *
   * Default: false
   */
  bool enable_graceful_shutdown_checkpoint = false;

  /**
   * @brief
   * It uses as the threshold (percentage) for rehashing of the hash index.
   * A large value (e.g., 99) will not easily rehash the index and thus reduce
   * memory consumption because leaving less room in the index. On the other
   * hand, a problem with open addressing hash indexes (current implementation)
   * is that the computational cost of an insert increases on the less room.
   *
   * Default: 75 (percent)
   */
  double rehash_threshold = 0.75;

  /**
   * @brief
   * The directory path that lineardb use as working directory.
   * All of data, logs and related files are stored in the directory.
   *
   * Default: "lineairdb_logs"
   */
  std::string work_dir = std::getenv("LINEAIRDB_WORK_DIR")
                             ? std::getenv("LINEAIRDB_WORK_DIR")
                             : "./lineairdb_logs";

  /**
   * @brief
   * The name of the anonymous table.
   * Anonymous table is used to store the data that is not associated with any
   * table.
   *
   * Default: "__anonymous_table"
   */
  std::string anonymous_table_name = "__anonymous_table";
};
}  // namespace LineairDB

#endif
