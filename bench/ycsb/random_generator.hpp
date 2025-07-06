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

#ifndef LINEAIRDB_RANDOM_GENERATOR_HPP_
#define LINEAIRDB_RANDOM_GENERATOR_HPP_

#include <atomic>
#include <cmath>
#include <cstdint>
#include <random>

class RandomGenerator {
public:
  RandomGenerator() : max_(0xdeadbeef) {}

  void Init(uint64_t items, double theta) {
    engine_ = std::mt19937(seeder_());
    uniform_ = std::uniform_int_distribution<>(0, items);
    uniform_real_ = std::uniform_real_distribution<>(0.0, 1.0);
    max_ = items - 1;
    theta_ = theta;
    alpha_ = 1.0 / (1.0 - theta_);
    countforzeta_ = items - 1;
    zeta2theta_ = zeta(2);
    eta_ = (1 - std::pow(2.0 / max_, 1 - theta_)) / (1 - zeta2theta_ / zetan_);

    Next();
    uint64_t expected = 0;
    latest.compare_exchange_strong(expected, items);
  }

  uint64_t Random() { return engine_(); }
  uint64_t BoundedRandom() { return engine_() % max_; }
  uint64_t UniformRandom() { return uniform_(engine_); }
  double UniformReal() { return uniform_real_(engine_); }
  uint64_t UniformRandom(uint64_t lower_bound, uint64_t upper_bound) {
    return std::uniform_int_distribution<>(lower_bound, upper_bound)(engine_);
  }
  uint64_t UniformRandom(uint64_t upper_bound) { return UniformRandom(0, upper_bound); }
  bool IsIntialized() { return max_ != 0xdeadbeef; }

  uint64_t Next(uint64_t max) {
    if (max != countforzeta_) { // recompute
      if (max > countforzeta_) {
        zetan_ = zeta(countforzeta_, max, zetan_);
        eta_ = (1 - std::pow(2.0 / max, 1 - theta_)) / (1 - zeta2theta_ / zetan_);
      } else {
        zetan_ = zeta(0, max, 0);
        eta_ = (1 - std::pow(2.0 / max, 1 - theta_)) / (1 - zeta2theta_ / zetan_);
      }
    }
    assert(max >= countforzeta_);

    double u = UniformReal();
    double uz = u * zetan_;

    if (uz < 1.0) {
      return 0;
    }

    if (uz < 1.0 + std::pow(0.5, theta_)) {
      return 1;
    }

    uint64_t ret = static_cast<uint64_t>(max * std::pow(eta_ * u - eta_ + 1, alpha_));

    return ret;
  }

  uint64_t Next(bool is_insert_occurs = false) {
    uint64_t max = is_insert_occurs ? latest.load(std::memory_order_relaxed) : max_;
    return Next(max);
  }

  /* Latest Distribution */
  // Reference Implementation:
  // https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/generator/SkewedLatestGenerator.java
  static std::atomic<uint64_t> latest;
  static uint64_t XAdd() {
    // SPDLOG_ERROR("insert {}", latest.load());
    return latest.fetch_add(1);
  }

private:
  double zeta(uint64_t st, uint64_t n, uint64_t initialsum) {
    double sum = initialsum;
    countforzeta_ = n;
    for (uint64_t i = st; i < n; i++) {
      sum += 1 / std::pow(i + 1, theta_);
    }
    return sum;
  }

  double zeta(uint64_t n) { return zeta(0, n, 0); }

private:
  std::random_device seeder_;
  std::mt19937 engine_;
  std::uniform_int_distribution<> uniform_;
  std::uniform_real_distribution<> uniform_real_;

  uint64_t max_;
  uint64_t countforzeta_;
  double theta_;
  double zetan_;
  double zeta2theta_;
  double alpha_;
  double eta_;
};

std::atomic<uint64_t> RandomGenerator::latest = std::atomic<uint64_t>(0);

#endif
