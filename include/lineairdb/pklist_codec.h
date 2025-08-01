#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace LineairDB {
namespace Codec {

using PKList = std::vector<std::string_view>;
using PKListView = std::vector<std::string_view>;  // alias for clarity

// ---------------------- Helper ------------------------------
inline void AppendUint32BE(std::string& dst, uint32_t v) {
  char buf[4];
  buf[0] = static_cast<char>((v >> 24) & 0xFF);
  buf[1] = static_cast<char>((v >> 16) & 0xFF);
  buf[2] = static_cast<char>((v >> 8) & 0xFF);
  buf[3] = static_cast<char>(v & 0xFF);
  dst.append(buf, 4);
}

inline uint32_t ReadUint32BE(const char* p) {
  return (static_cast<uint32_t>(static_cast<unsigned char>(p[0])) << 24) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 8) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[3])));
}

// ------------------------------------------------------------
// EncodePKList : PKList -> contiguous byte string (len|data)*
// Throws std::runtime_error if total size exceeds max_size (optional)
// ------------------------------------------------------------
inline std::string EncodePKList(const PKList& list,
                                size_t max_size = SIZE_MAX) {
  std::string out;
  out.reserve(8 * list.size());  // rough reserve

  for (auto sv : list) {
    if (sv.size() > 0xFFFF'FFFF) {
      throw std::runtime_error("PK string too long to encode ( >4GiB )");
    }
    AppendUint32BE(out, static_cast<uint32_t>(sv.size()));
    out.append(sv.data(), sv.size());
  }
  if (out.size() > max_size) {
    throw std::runtime_error("Encoded PKList exceeds buffer limit");
  }
  return out;
}

// ------------------------------------------------------------
// DecodePKList : contiguous bytes -> vector<string_view>
// ------------------------------------------------------------
inline PKListView DecodePKList(std::string_view bytes,
                               std::vector<std::string>& storage) {
  PKListView views;
  const char* p = bytes.data();
  const char* end = p + bytes.size();
  while (p < end) {
    if (end - p < 4) {
      throw std::runtime_error("PKList decode error: truncated length field");
    }
    uint32_t len = ReadUint32BE(p);
    p += 4;
    if (p + len > end) {
      throw std::runtime_error("PKList decode error: truncated payload");
    }
    storage.emplace_back(p, len);        // copy contents
    views.emplace_back(storage.back());  // string_view 指す
    p += len;
  }
  return views;
}

// -----------------------------------------------------------------
// DecodePKListOwned : contiguous bytes -> vector<std::string>
// 呼び出し側が PKList のライフタイムを気にせず済む安全版 API。
// -----------------------------------------------------------------
inline std::vector<std::string> DecodePKListOwned(std::string_view bytes) {
  std::vector<std::string> out;
  const char* p = bytes.data();
  const char* end = p + bytes.size();

  while (p < end) {
    if (end - p < 4) {
      throw std::runtime_error("PKList decode error: truncated length field");
    }
    uint32_t len = ReadUint32BE(p);
    p += 4;
    if (p + len > end) {
      throw std::runtime_error("PKList decode error: truncated payload");
    }
    out.emplace_back(p, len);  // copy constructor stores bytes
    p += len;
  }
  return out;
}

}  // namespace Codec
}  // namespace LineairDB