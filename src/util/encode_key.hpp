// encode_key.hpp
#pragma once
#include <charconv>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>

namespace LineairDB {
namespace Util {

// ──────────────────────────────────────────────
// 汎用（std::string 系）──────────────────────────
inline std::string EncodeKey(std::string_view sv) {
  return std::string(sv);  // そのままコピー
}

// ──────────────────────────────────────────────
// 整数型（符号付き／符号なしどちらも対応）─────
template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
std::string EncodeKey(T v) {
  using UInt = std::make_unsigned_t<T>;

  // ① 2 の補数前提で [-2ⁿ⁻¹, 2ⁿ⁻¹-1] → [0, 2ⁿ-1] へ単調写像
  constexpr UInt bias = static_cast<UInt>(std::numeric_limits<T>::min());
  UInt shifted = static_cast<UInt>(v) - bias;  // INT_MIN → 0, INT_MAX → 2ⁿ-1

  // ② 型ごとに必要な十進桁数を固定幅とする
  constexpr int width =
      std::numeric_limits<UInt>::digits10;  // 例: uint64_t → 20

  // ③ ヒープを使わずに文字列化
  char buf[width];  // to_chars は終端の NUL を要さない
  auto [p, ec] = std::to_chars(std::begin(buf), std::end(buf), shifted);
  (void)ec;  // 幅は十分なので失敗しない

  // ④ 左側ゼロ埋めで固定長にそろえる
  std::string s(buf, p);
  s.insert(s.begin(), width - s.size(), '0');
  return s;
}



}  // namespace Util
}  // namespace LineairDB
