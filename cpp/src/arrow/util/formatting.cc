// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/formatting.h"
#include "arrow/util/config.h"
#include "arrow/util/double_conversion.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace internal {
namespace detail {

const char digit_pairs[] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

using util::double_conversion::DoubleToStringConverter;

static DoubleToStringConverter g_double_converter(
    DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN, "inf", "nan", 'e', -6, 10, 6,
    0);

int FormatFloat(float v, size_t buffer_size, char* buffer) {
  // StringBuilder checks bounds in debug mode for us
  util::double_conversion::StringBuilder builder(buffer, static_cast<int>(buffer_size));
  bool result = g_double_converter.ToShortestSingle(v, &builder);
  DCHECK(result);
  ARROW_UNUSED(result);
  return builder.position();
}

int FormatFloat(double v, size_t buffer_size, char* buffer) {
  util::double_conversion::StringBuilder builder(buffer, static_cast<int>(buffer_size));
  bool result = g_double_converter.ToShortest(v, &builder);
  DCHECK(result);
  ARROW_UNUSED(result);
  return builder.position();
}

}  // namespace detail
}  // namespace internal
}  // namespace arrow
