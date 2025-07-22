#pragma once
#include <string_view>
#include <typeinfo>
#include <vector>

namespace LineairDB {
namespace Index {  // 小文字の index から大文字の Index に修正

// 後続のクラスで使う型エイリアスをここで定義
using PrimaryKey = std::string_view;
using PKList = std::vector<PrimaryKey>;

class ISecondaryIndex {
 public:
  virtual ~ISecondaryIndex() = default;

  virtual const std::type_info& KeyTypeInfo() const = 0;

  virtual bool AddPK(std::string_view serialized_key, std::string_view pk) = 0;

  virtual PKList* GetPKList(std::string_view serialized_key) = 0;

  virtual bool IsUnique() const = 0;
};
}  // namespace Index
}  // namespace LineairDB