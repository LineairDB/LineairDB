#pragma once

#include <cstdint>

namespace LineairDB::Index {

class SecondaryIndexType {
 public:
  using RawType = uint32_t;

  static constexpr RawType kNone = 0;
  static constexpr RawType kDictUnique = 2;

  constexpr SecondaryIndexType() : raw_(kNone) {}
  constexpr explicit SecondaryIndexType(RawType raw) : raw_(raw) {}

  static constexpr SecondaryIndexType FromRaw(RawType raw) {
    return SecondaryIndexType(raw);
  }

  constexpr RawType Raw() const { return raw_; }
  constexpr bool IsUnique() const { return (raw_ & kDictUnique) != 0; }

 private:
  RawType raw_;
};

}  // namespace LineairDB::Index
