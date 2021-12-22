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

  enum ConcurrentPointIndex { MPMCConcurrentHashSet };
  /**
   * @brief
   * Set the type of concurrent point index.
   * See LineairDB::Config::ConcurrentPointIndex for the enum options of this
   * configuration.
   *
   * Default: MPMCConcurrentHashSet
   */
  ConcurrentPointIndex concurrent_point_index = MPMCConcurrentHashSet;

  enum RangeIndex { EpochROWEX };
  /**
   * @brief
   * Set the type of range index.
   * See LineairDB::Config::RangeIndex for the enum options of this
   * configuration.
   *
   * Default: ROWEX (epoch-based read-optimized write-exclusive index)
   */
  RangeIndex range_index = EpochROWEX;

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
   * If true, LineairDB performs logging for recovery.
   *
   * Default: true
   */
  bool enable_logging = true;

  /**
   * @brief
   * If true, LineairDB performs logging for checkpoint-recovery.
   * Checkpointing prevents the log file size from increasing monotonically.
   * i.e, if this parameter is set to false, The disk space used by LineairDB is
   * unbounded.
   * You can also turn off enable_logging and use only Checkpointing.
   * This configure gives the recoverability property called CPR-consistency
   * [1]. CPR-consitency may volatilize the data of committed transactions at
   * last the number of seconds specified at checkpoint_period; however, the
   * persistence of the data before that time is guaranteed.
   *
   * Default: true
   * @ref [1]:
   * https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf
   */
  bool enable_checkpointing = true;

  /**
   * @brief
   * It uses as the interval time (seconds) for checkpointing.
   * The longer is the better for the performance but the larger interval time
   * causes the increasing of log file size.
   *
   * Default: 30
   */
  size_t checkpoint_period = 30;
  /**
   * @brief
   * The directory path that lineardb use as working directory.
   * All of data, logs and related files are stored in the directory.
   *
   * Default: "lineairdb_logs"
   */
  // TODO: think of variable name, perhaps, sort of "work_dir" would be intuitive
  std::string lineairdb_logs_dir = "./lineairdb_logs";
};
}  // namespace LineairDB

#endif
