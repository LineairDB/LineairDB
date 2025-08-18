#pragma once
#include <any>
#include <string_view>
#include <typeinfo>
#include <vector>

#include "types/data_item.hpp"

namespace LineairDB {
namespace Index {

class SecondaryIndexInterface {
 public:
  virtual ~SecondaryIndexInterface() = default;

  virtual const std::type_info& KeyTypeInfo() const = 0;

  virtual DataItem* Get(std::string_view serialized_key) = 0;

  virtual DataItem* GetOrInsert(std::string_view serialized_key) = 0;

  virtual std::optional<size_t> Scan(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view)> operation) = 0;

  virtual bool IsUnique() const = 0;

  // delete a secondary key from range index (and optionally point index)
  // return false if rejected by phantom-avoidance
  virtual bool DeleteKey(std::string_view serialized_key) = 0;
};
}  // namespace Index
}  // namespace LineairDB