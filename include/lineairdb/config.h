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
  size_t max_thread;
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
  size_t epoch_duration_ms;

  enum ConcurrencyControl { Silo, SiloNWR, TwoPhaseLocking };
  /**
   * @brief
   * Set a concurrency control algorithm.
   * See LineairDB::Config::ConcurrencyControl for the enum options of this
   * configuration.
   *
   * Default: SiloNWR
   */
  ConcurrencyControl concurrency_control_protocol;

  enum Logger { ThreadLocalLogger };
  /**
   * @brief
   * Set a logging algorithm.
   * See LineairDB::Config::Logger for the enum options of this
   * configuration.
   *
   * Default: ThreadLocalLogger
   */
  Logger logger;

  enum ConcurrentPointIndex { MPMCConcurrentHashSet };
  /**
   * @brief
   * Set the type of concurrent point index.
   * See LineairDB::Config::ConcurrentPointIndex for the enum options of this
   * configuration.
   *
   * Default: MPMCConcurrentHashSet
   */
  ConcurrentPointIndex concurrent_point_index;

  enum CallbackEngine { ThreadLocal };
  /**
   * @brief
   * Set the type of callback engine.
   * See LineairDB::Config::CallBackEngine for the enum options of this
   * configuration.
   *
   * Default: ThreadLocal
   */
  CallbackEngine callback_engine;

  /**
   * @brief
   * If true, LineairDB processes recovery at the instantiation.
   *
   * Default: true
   */
  bool enable_recovery;

  /**
   * @brief
   * If true, the db logs processed operations for recovery.
   *
   * Default: true
   */
  bool enable_logging;

  Config(const size_t m = std::thread::hardware_concurrency(),
         const size_t e = 40, const ConcurrencyControl cc = SiloNWR,
         const Logger lg               = ThreadLocalLogger,
         const ConcurrentPointIndex in = MPMCConcurrentHashSet,
         const CallbackEngine cb = ThreadLocal, const bool r = true,
         const bool l = true)
      : max_thread(m),
        epoch_duration_ms(e),
        concurrency_control_protocol(cc),
        logger(lg),
        concurrent_point_index(in),
        callback_engine(cb),
        enable_recovery(r),
        enable_logging(l){};
};
}  // namespace LineairDB

#endif
