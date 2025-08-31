#pragma once

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "lineairdb/pklist_codec.h"

namespace LineairDB {
namespace Util {

inline std::vector<std::string> DecodePKList(const std::byte* ptr,
                                             std::size_t size) {
  if (ptr == nullptr || size == 0) {
    return {};
  }
  std::string_view bytes{reinterpret_cast<const char*>(ptr), size};
  return LineairDB::Codec::DecodePKListOwned(bytes);
}

inline std::string EncodePKList(const std::vector<std::string>& list,
                                std::string_view new_pk) {
  std::vector<std::string> tmp = list;
  auto found = std::find(tmp.begin(), tmp.end(), new_pk);
  if (found == tmp.end()) {
    tmp.emplace_back(new_pk);
  }
  LineairDB::Codec::PKList pk_sv;
  pk_sv.reserve(tmp.size());
  for (const auto& s : tmp) pk_sv.emplace_back(s);
  return LineairDB::Codec::EncodePKList(pk_sv);
}

}  // namespace Util
}  // namespace LineairDB