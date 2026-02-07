#include "index/secondary_index_type.h"

#include "gtest/gtest.h"

namespace {

using LineairDB::Index::SecondaryIndexType;

TEST(SecondaryIndexTypeTest, RawRoundTripPreservesBits) {
  constexpr SecondaryIndexType::RawType kRaw = 0x12u;
  const auto index_type = SecondaryIndexType::FromRaw(kRaw);
  EXPECT_EQ(index_type.Raw(), kRaw);
}

TEST(SecondaryIndexTypeTest, DictUniqueBitControlsUniqueCheck) {
  const auto non_unique = SecondaryIndexType::FromRaw(0x08u);
  EXPECT_FALSE(non_unique.IsUnique());

  const auto unique = SecondaryIndexType::FromRaw(SecondaryIndexType::kDictUnique);
  EXPECT_TRUE(unique.IsUnique());

  const auto unique_with_extra_bits =
      SecondaryIndexType::FromRaw(0x18u | SecondaryIndexType::kDictUnique);
  EXPECT_TRUE(unique_with_extra_bits.IsUnique());
}

}  // namespace
