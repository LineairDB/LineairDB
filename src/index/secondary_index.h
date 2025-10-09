#pragma once

#include "index/concurrent_table.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

// Forward declaration of key serializer
namespace Util {
/**
 * @brief Serialize a key to a format that is comparable using string comparison
 *
 * For numeric types (INT, DATETIME), keys are serialized to a fixed-length
 * big-endian binary format with sign bit flipped. This ensures that
 * lexicographic string comparison produces the same ordering as numeric
 * comparison.
 *
 * @param key The input key as string_view
 * @param field_type The type of the field (0=INT, 1=STRING, 2=DATETIME,
 * 3=OTHER)
 * @return Serialized key as std::string
 */
inline std::string SerializeKeyByType(std::string_view key, uint field_type) {
  switch (field_type) {
    case 0:  // LINEAIRDB_INT
    case 2:  // LINEAIRDB_DATETIME
    {
      // Parse the string as a signed 64-bit integer
      int64_t value = 0;
      try {
        value = std::stoll(std::string(key));
      } catch (...) {
        // If parsing fails, return the key as-is
        return std::string(key);
      }

      // XOR with 0x8000'0000'0000'0000 to flip the sign bit
      // This makes negative numbers sort before positive numbers
      uint64_t unsigned_value =
          static_cast<uint64_t>(value) ^ 0x8000'0000'0000'0000ULL;

      // Convert to big-endian (most significant byte first)
      char buf[8];
      for (int i = 0; i < 8; ++i) {
        buf[i] = static_cast<char>((unsigned_value >> (56 - i * 8)) & 0xFF);
      }

      return std::string(buf, 8);
    }

    case 1:  // LINEAIRDB_STRING
      return std::string(key);

    case 3:  // LINEAIRDB_OTHER
    default:
      return std::string(key);
  }
}
}  // namespace Util
class SecondaryIndex {
 public:
  SecondaryIndex(EpochFramework& epoch_framework, Config config = Config(),
                 uint index_type = 0, uint data_type = 0,
                 [[maybe_unused]] WriteSetType recovery_set = WriteSetType())
      : index_type_(index_type), data_type_(data_type) {}

  /**
   * @brief Generate a serialized key that is comparable using string comparison
   *
   * This function converts the input key to a format suitable for range index
   * operations. For numeric types, it converts to a fixed-length big-endian
   * binary format. For string types, it returns the key as-is.
   *
   * @param key The input key
   * @return Serialized key as std::string_view
   */
  std::string_view GenerateSerializedKey(std::string_view key) {
    // Store the serialized key in the buffer to keep it alive
    // since we're returning a string_view
    serialized_key_buffer_ = Util::SerializeKeyByType(key, data_type_);
    return serialized_key_buffer_;
  }

  DataItem* Get(std::string_view key) {
    return secondary_index_->Get(GenerateSerializedKey(key));
  }

  DataItem* GetOrInsert(std::string_view key) {
    auto serialized_key = GenerateSerializedKey(key);
    auto* item = secondary_index_->Get(serialized_key);
    if (item == nullptr) {
      secondary_index_->ForcePutBlankEntry(serialized_key);
      item = secondary_index_->Get(serialized_key);
      assert(item != nullptr);
    }
    return item;
  }

  std::optional<size_t> Scan(std::string_view begin, std::string_view end,
                             std::function<bool(std::string_view)> operation) {
    return secondary_index_->Scan(GenerateSerializedKey(begin),
                                  GenerateSerializedKey(end), operation);
  }

  bool DeleteKey(std::string_view key) {
    // Range-only deletion to hide the key from scans.
    // Point index does not support erase currently.
    // Implement as a method on HashTableWithPrecisionLockingIndex when
    // available.
    return secondary_index_->EraseRangeOnly(GenerateSerializedKey(key));
  }

 private:
  uint index_type_;
  uint data_type_;
  std::unique_ptr<HashTableWithPrecisionLockingIndex<DataItem>>
      secondary_index_;

  // Buffer to hold the serialized key (needed because GenerateSerializedKey
  // returns string_view)
  std::string serialized_key_buffer_;
};
}  // namespace Index
}  // namespace LineairDB