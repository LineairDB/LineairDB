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

#include <cmath>
#include <cstdint>
#include <random>
#include <atomic>

class RandomGenerator {
 public:
  RandomGenerator() : max_(0xdeadbeef) {}

  void Init(uint64_t items, double theta) {
    engine_  = std::mt19937(seeder_());
    uniform_ = std::uniform_int_distribution<>(0, items);
    max_     = items - 1;
    theta_   = theta;
    zetan_   = zeta(items);
    alpha_   = 1.0 / (1.0 - theta_);
    eta_     = (1 - std::pow(2.0 / items, 1 - theta_)) / (1 - zeta(2) / zetan_);
  }

  uint64_t Random() { return engine_(); }
  uint64_t BoundedRandom() { return engine_() % max_; }
  uint64_t UniformRandom() { return uniform_(engine_); }
  uint64_t UniformRandom(uint64_t lower_bound, uint64_t upper_bound) {
    return std::uniform_int_distribution<>(lower_bound, upper_bound)(engine_);
  }
  uint64_t UniformRandom(uint64_t upper_bound) {
    return UniformRandom(0, upper_bound);
  }
  bool IsIntialized() { return max_ != 0xdeadbeef; }

  uint64_t Next() {
    double u  = UniformRandom() / static_cast<double>(max_);
    double uz = u * zetan_;
    if (uz < 1.0) { return 0; }

    if (uz < 1.0 + std::pow(0.5, theta_)) { return 1; }

    uint64_t ret =
        static_cast<uint64_t>(max_ * std::pow(eta_ * u - eta_ + 1, alpha_));
    return ret;
  }

  static std::atomic<uint64_t> latest;
  static void SetLatest(uint64_t l) {
    latest.store(l, std::memory_order_relaxed);
  }

  // Reference Implementation: https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/generator/SkewedLatestGenerator.java
  static uint64_t LatestNext(RandomGenerator* rand) {
    const auto lt = latest.load(std::memory_order_relaxed);
    const auto next = lt - rand->Next();
    SetLatest(next);
    return next;
  }

 private:
  double zeta(uint64_t n) {
    double sum = 0;
    for (uint64_t i = 0; i < n; i++) { sum += 1 / std::pow(i + 1, theta_); }
    return sum;
  }

 private:
  std::random_device seeder_;
  std::mt19937 engine_;
  std::uniform_int_distribution<> uniform_;

  uint64_t max_;
  double theta_;
  double zetan_;
  double alpha_;
  double eta_;
};

std::atomic<uint64_t> RandomGenerator::latest = std::atomic<uint64_t>(0);

#endif
