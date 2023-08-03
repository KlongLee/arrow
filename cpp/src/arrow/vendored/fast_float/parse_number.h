#ifndef FASTFLOAT_PARSE_NUMBER_H
#define FASTFLOAT_PARSE_NUMBER_H

#include "ascii_number.h"
#include "decimal_to_binary.h"
#include "digit_comparison.h"

#include <cmath>
#include <cstring>
#include <limits>
#include <system_error>

namespace arrow_vendored {
namespace fast_float {


namespace detail {
/**
 * Special case +inf, -inf, nan, infinity, -infinity.
 * The case comparisons could be made much faster given that we know that the
 * strings a null-free and fixed.
 **/
template <typename T>
from_chars_result parse_infnan(const char *first, const char *last, T &value)  noexcept  {
  from_chars_result answer;
  answer.ptr = first;
  answer.ec = std::errc(); // be optimistic
  bool minusSign = false;
  if (*first == '-') { // assume first < last, so dereference without checks; C++17 20.19.3.(7.1) explicitly forbids '+' here
      minusSign = true;
      ++first;
  }
#if FASTFLOAT_ALLOWS_LEADING_PLUS // disabled by default
  if (*first == '+') {
      ++first;
  }
#endif
  if (last - first >= 3) {
    if (fastfloat_strncasecmp(first, "nan", 3)) {
      answer.ptr = (first += 3);
      value = minusSign ? -std::numeric_limits<T>::quiet_NaN() : std::numeric_limits<T>::quiet_NaN();
      // Check for possible nan(n-char-seq-opt), C++17 20.19.3.7, C11 7.20.1.3.3. At least MSVC produces nan(ind) and nan(snan).
      if(first != last && *first == '(') {
        for(const char* ptr = first + 1; ptr != last; ++ptr) {
          if (*ptr == ')') {
            answer.ptr = ptr + 1; // valid nan(n-char-seq-opt)
            break;
          }
          else if(!(('a' <= *ptr && *ptr <= 'z') || ('A' <= *ptr && *ptr <= 'Z') || ('0' <= *ptr && *ptr <= '9') || *ptr == '_'))
            break; // forbidden char, not nan(n-char-seq-opt)
        }
      }
      return answer;
    }
    if (fastfloat_strncasecmp(first, "inf", 3)) {
      if ((last - first >= 8) && fastfloat_strncasecmp(first + 3, "inity", 5)) {
        answer.ptr = first + 8;
      } else {
        answer.ptr = first + 3;
      }
      value = minusSign ? -std::numeric_limits<T>::infinity() : std::numeric_limits<T>::infinity();
      return answer;
    }
  }
  answer.ec = std::errc::invalid_argument;
  return answer;
}

/**
 * Returns true if the floating-pointing rounding mode is to 'nearest'.
 * It is the default on most system. This function is meant to be inexpensive.
 * Credit : @mwalcott3
 */
fastfloat_really_inline bool rounds_to_nearest() noexcept {
  // https://lemire.me/blog/2020/06/26/gcc-not-nearest/
#if (FLT_EVAL_METHOD != 1) && (FLT_EVAL_METHOD != 0)
  return false;
#endif
  // See
  // A fast function to check your floating-point rounding mode
  // https://lemire.me/blog/2022/11/16/a-fast-function-to-check-your-floating-point-rounding-mode/
  //
  // This function is meant to be equivalent to :
  // prior: #include <cfenv>
  //  return fegetround() == FE_TONEAREST;
  // However, it is expected to be much faster than the fegetround()
  // function call.
  //
  // The volatile keywoard prevents the compiler from computing the function
  // at compile-time.
  // There might be other ways to prevent compile-time optimizations (e.g., asm).
  // The value does not need to be std::numeric_limits<float>::min(), any small
  // value so that 1 + x should round to 1 would do (after accounting for excess
  // precision, as in 387 instructions).
  static volatile float fmin = std::numeric_limits<float>::min();
  float fmini = fmin; // we copy it so that it gets loaded at most once.
  //
  // Explanation:
  // Only when fegetround() == FE_TONEAREST do we have that
  // fmin + 1.0f == 1.0f - fmin.
  //
  // FE_UPWARD:
  //  fmin + 1.0f > 1
  //  1.0f - fmin == 1
  //
  // FE_DOWNWARD or  FE_TOWARDZERO:
  //  fmin + 1.0f == 1
  //  1.0f - fmin < 1
  //
  // Note: This may fail to be accurate if fast-math has been
  // enabled, as rounding conventions may not apply.
  #if FASTFLOAT_VISUAL_STUDIO
  #   pragma warning(push)
  //  todo: is there a VS warning?
  //  see https://stackoverflow.com/questions/46079446/is-there-a-warning-for-floating-point-equality-checking-in-visual-studio-2013
  #elif defined(__clang__)
  #   pragma clang diagnostic push
  #   pragma clang diagnostic ignored "-Wfloat-equal"
  #elif defined(__GNUC__)
  #   pragma GCC diagnostic push
  #   pragma GCC diagnostic ignored "-Wfloat-equal"
  #endif
  return (fmini + 1.0f == 1.0f - fmini);
  #if FASTFLOAT_VISUAL_STUDIO
  #   pragma warning(pop)
  #elif defined(__clang__)
  #   pragma clang diagnostic pop
  #elif defined(__GNUC__)
  #   pragma GCC diagnostic pop
  #endif
}

} // namespace detail

template<typename T>
from_chars_result from_chars(const char *first, const char *last,
                             T &value, chars_format fmt /*= chars_format::general*/)  noexcept  {
  return from_chars_advanced(first, last, value, parse_options{fmt});
}

template<typename T>
from_chars_result from_chars_advanced(const char *first, const char *last,
                                      T &value, parse_options options)  noexcept  {

  static_assert (std::is_same<T, double>::value || std::is_same<T, float>::value, "only float and double are supported");


  from_chars_result answer;
#if FASTFLOAT_SKIP_WHITE_SPACE  // disabled by default
  while ((first != last) && fast_float::is_space(uint8_t(*first))) {
    first++;
  }
#endif
  if (first == last) {
    answer.ec = std::errc::invalid_argument;
    answer.ptr = first;
    return answer;
  }
  parsed_number_string pns = parse_number_string(first, last, options);
  if (!pns.valid) {
    return detail::parse_infnan(first, last, value);
  }
  answer.ec = std::errc(); // be optimistic
  answer.ptr = pns.lastmatch;
  // The implementation of the Clinger's fast path is convoluted because
  // we want round-to-nearest in all cases, irrespective of the rounding mode
  // selected on the thread.
  // We proceed optimistically, assuming that detail::rounds_to_nearest() returns
  // true.
  if (binary_format<T>::min_exponent_fast_path() <= pns.exponent && pns.exponent <= binary_format<T>::max_exponent_fast_path() && !pns.too_many_digits) {
    // Unfortunately, the conventional Clinger's fast path is only possible
    // when the system rounds to the nearest float.
    //
    // We expect the next branch to almost always be selected.
    // We could check it first (before the previous branch), but
    // there might be performance advantages at having the check
    // be last.
    if(detail::rounds_to_nearest())  {
      // We have that fegetround() == FE_TONEAREST.
      // Next is Clinger's fast path.
      if (pns.mantissa <=binary_format<T>::max_mantissa_fast_path()) {
        value = T(pns.mantissa);
        if (pns.exponent < 0) { value = value / binary_format<T>::exact_power_of_ten(-pns.exponent); }
        else { value = value * binary_format<T>::exact_power_of_ten(pns.exponent); }
        if (pns.negative) { value = -value; }
        return answer;
      }
    } else {
      // We do not have that fegetround() == FE_TONEAREST.
      // Next is a modified Clinger's fast path, inspired by Jakub Jelínek's proposal
      if (pns.exponent >= 0 && pns.mantissa <=binary_format<T>::max_mantissa_fast_path(pns.exponent)) {
#if defined(__clang__)
        // Clang may map 0 to -0.0 when fegetround() == FE_DOWNWARD
        if(pns.mantissa == 0) {
          value = 0;
          return answer;
        }
#endif
        value = T(pns.mantissa) * binary_format<T>::exact_power_of_ten(pns.exponent);
        if (pns.negative) { value = -value; }
        return answer;
      }
    }
  }
  adjusted_mantissa am = compute_float<binary_format<T>>(pns.exponent, pns.mantissa);
  if(pns.too_many_digits && am.power2 >= 0) {
    if(am != compute_float<binary_format<T>>(pns.exponent, pns.mantissa + 1)) {
      am = compute_error<binary_format<T>>(pns.exponent, pns.mantissa);
    }
  }
  // If we called compute_float<binary_format<T>>(pns.exponent, pns.mantissa) and we have an invalid power (am.power2 < 0),
  // then we need to go the long way around again. This is very uncommon.
  if(am.power2 < 0) { am = digit_comp<T>(pns, am); }
  to_float(pns.negative, am, value);
  return answer;
}

} // namespace fast_float
} // namespace arrow_vendored

#endif
