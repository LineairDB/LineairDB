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

#ifndef LINEAIRDB_CONCURRENCY_CONTROL_32BIT_SET_HPP
#define LINEAIRDB_CONCURRENCY_CONTROL_32BIT_SET_HPP

#include <assert.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <sstream>
#include <utility>

template <const uint32_t CounterSize = 1> class HalfWordSet {
  static_assert((CounterSize <= 32) && (32 % CounterSize == 0),
                "HalfWordSet requires CounterSize <= 32-bit and 8-bit "
                "divisible");

  constexpr static size_t ArraySize = 32 / CounterSize;
  constexpr static uint32_t Max = (2 << (CounterSize - 1)) - 1;

  constexpr static uint32_t MakeMask(const size_t i) {
    uint32_t r = 0;
    for (uint32_t j = i * CounterSize; j < ((i + 1) * CounterSize); j++) {
      r |= 1llu << j;
    }
    return r;
  }

  template <size_t... i> constexpr static auto MaskBuilder(std::index_sequence<i...>) {
    return std::array<uint32_t, sizeof...(i)>{{MakeMask(i)...}};
  }

  constexpr static std::array<uint32_t, ArraySize> GetMasks() {
    return MaskBuilder(std::make_index_sequence<ArraySize>{});
  }

  constexpr static std::array<uint32_t, ArraySize> Masks = GetMasks();

private:
  uint32_t bitarray_;

public:
  HalfWordSet() noexcept : bitarray_(0) {}
  HalfWordSet(uint32_t s) : bitarray_(s) {}

  void Put(const uint32_t seed, uint32_t version = 1) {
    const size_t slot = HalfWordSet::Hash(seed) % ArraySize;
    Set(slot, version);
  }

  void Put(const void* seedptr, uint32_t version = 1) { return Put(Hashptr(seedptr), version); }

  void PutHigherside(const void* seedptr, const uint32_t version) {
    const uint32_t current = Get(seedptr);
    if (current >= version)
      return;
    Put(seedptr, version);
  }

  void PutLowerside(const void* seedptr, const uint32_t version) {
    const uint32_t current = Get(seedptr);
    if (current <= version)
      return;
    Put(seedptr, version);
  }

  uint32_t Get(const uint32_t seed) {
    const size_t slot = HalfWordSet::Hash(seed) % ArraySize;
    return GetBySlot(slot);
  }
  uint32_t Get(const void* seedptr) { return Get(Hashptr(seedptr)); }

  bool IsEmpty() const { return bitarray_ == 0; }

  HalfWordSet Merge(const HalfWordSet& rhs) const {
    if (CounterSize == 1) {
      return HalfWordSet(bitarray_ | rhs.bitarray_);
    } else { // @NOTE: merge with choosing lower side

      HalfWordSet bf;
      for (size_t i = 0; i < ArraySize; i++) {
        const uint32_t lhs_slot = bitarray_ & Masks[i];
        const uint32_t rhs_slot = rhs.bitarray_ & Masks[i];
        if (lhs_slot == 0 && rhs_slot == 0)
          continue;

        if (lhs_slot == 0 || rhs_slot == 0) {
          bf.bitarray_ |= std::max(lhs_slot, rhs_slot);
        } else {
          bf.bitarray_ |= std::min(lhs_slot, rhs_slot);
        }
      }

      return bf;
    }
  }

  // #greater_than: compare the slots.
  // if value of any side is zero, it ignores the slot.
  // @NOTE SIMD(MMX)instruction cannot above: ignoring zero.
  // and it does not better for geq: greater or equal than.
  constexpr inline bool IsGreaterThan(const HalfWordSet& rhs) const {
    if (bitarray_ == 0llu || rhs.bitarray_ == 0llu)
      return false;
    for (size_t i = 0; i < ArraySize; i++) {
      const uint32_t lhs_slot = bitarray_ & Masks[i];
      if (lhs_slot == 0)
        continue;
      const uint32_t rhs_slot = rhs.bitarray_ & Masks[i];
      if (rhs_slot == 0)
        continue;
      if (lhs_slot == Masks[i])
        return true; // counter is saturated

      if (rhs_slot < lhs_slot) {
        return true;
      }
    }
    return false;
  }

  // #greater_or_eq_than: compare the slots.
  // if value of any side is zero, it ignores the slot.
  constexpr inline bool IsGreaterOrEqualThan(const HalfWordSet& rhs) const {
    if (bitarray_ == 0llu || rhs.bitarray_ == 0llu)
      return false;
    for (size_t i = 0; i < ArraySize; i++) {
      const uint32_t lhs_slot = bitarray_ & Masks[i];
      if (lhs_slot == 0)
        continue;
      const uint32_t rhs_slot = rhs.bitarray_ & Masks[i];
      if (rhs_slot == 0)
        continue;
      if (lhs_slot == Masks[i])
        return true; // counter is saturated

      if (rhs_slot <= lhs_slot) {
        return true;
      }
    }
    return false;
  }

  bool IsSameWith(const HalfWordSet& rhs) { return bitarray_ == rhs.bitarray_; }

  void Copy(HalfWordSet& rhs) { bitarray_ = rhs.bitarray_; }
  void Copy(HalfWordSet&& rhs) { bitarray_ = rhs.bitarray_; }

  friend std::ostream& operator<<(std::ostream& o, const HalfWordSet& f) {
    std::stringstream ss;
    for (size_t i = 0; i < ArraySize; i++) {
      o << f.GetBySlot(i);
      o << " ";
    }
    o << ss.str();
    return o;
  }

  bool operator==(const HalfWordSet& rhs) noexcept { return IsSameWith(rhs); }

  inline bool Empty() const { return bitarray_ == 0; }

private:
  inline void Reset(const uint32_t slot) {
    bitarray_ &= ~(Max << (CounterSize * slot));
    assert((bitarray_ & (Max << (CounterSize * slot))) == 0);
  }

  inline uint32_t GetBySlot(const size_t slot) const {
    return (bitarray_ >> (CounterSize * slot)) & Max;
  }

  inline void Set(const uint32_t slot, uint32_t version) {
    Reset(slot);
    if (Max < version)
      version = Max;
    bitarray_ |= version << (CounterSize * slot);
  }

  static constexpr uint32_t FNV = 2166136261lu;
  static constexpr uint32_t FNV_PRIME = 16777619lu;
  inline static uint32_t Hash(uint32_t seed) {
    uint32_t hash = FNV;
    uint8_t* begin = reinterpret_cast<uint8_t*>(&seed);
    for (size_t i = 0; i < 4; ++i) {
      hash = (FNV_PRIME * hash) ^ begin[i];
    }

    return hash;
  }

  inline static uint32_t Hashptr(const void* seed) {
    return static_cast<uint32_t>((reinterpret_cast<uint64_t>(seed) >> 4));
  }
};

template <uint32_t CounterSize>
constexpr std::array<uint32_t, HalfWordSet<CounterSize>::ArraySize> HalfWordSet<CounterSize>::Masks;

#endif
