#include <any>
#include <cstdint>
#include <ctime>
#include <string>

namespace LineairDB {
namespace Util {
template <typename T>
std::string SerializeKey(const T&) = delete;

// string
template <>
inline std::string SerializeKey<std::string>(const std::string& v) {
  return v;
}

// int (64bit想定)
template <>
inline std::string SerializeKey<int>(const int& v) {
  uint64_t bias = static_cast<int64_t>(v) ^ 0x8000'0000'0000'0000ULL;
  char buf[8];
  for (int i = 7; i >= 0; --i) {
    buf[i] = static_cast<char>(bias & 0xFF);
    bias >>= 8;
  }
  return std::string(buf, 8);
}

// time_t
template <>
inline std::string SerializeKey<std::time_t>(const std::time_t& v) {
  uint64_t t = static_cast<uint64_t>(v);
  char buf[8];
  for (int i = 7; i >= 0; --i) {
    buf[i] = static_cast<char>(t & 0xFF);
    t >>= 8;
  }
  return std::string(buf, 8);
}

// std::any
inline std::string SerializeKey(const std::any& value) {
  if (value.type() == typeid(std::string)) {
    return SerializeKey(std::any_cast<const std::string&>(value));
  } else if (value.type() == typeid(int)) {
    return SerializeKey(std::any_cast<int>(value));
  } else if (value.type() == typeid(std::time_t)) {
    return SerializeKey(std::any_cast<std::time_t>(value));
  } else {
    throw std::runtime_error("Unsupported key type for SerializeKey");
  }
}

}  // namespace Util
}  // namespace LineairDB