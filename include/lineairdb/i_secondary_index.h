#pragma once
#include <string_view>
#include <typeinfo>
#include <vector>

#include "types/data_item.hpp"

namespace LineairDB {
namespace Index {

class ISecondaryIndex {
 public:
  virtual ~ISecondaryIndex() = default;

  virtual const std::type_info& KeyTypeInfo() const = 0;

  virtual DataItem* Get(std::string_view serialized_key) = 0;

  virtual DataItem* GetOrInsert(std::string_view serialized_key) = 0;

  virtual bool IsUnique() const = 0;
};
}  // namespace Index
}  // namespace LineairDB