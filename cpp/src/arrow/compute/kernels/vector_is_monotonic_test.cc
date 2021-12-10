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

#include <gtest/gtest.h>

#include "arrow/compute/api_vector.cc"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/datum.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

void CheckIsMonotonic(Datum input, bool increasing, bool strictly_increasing,
                      bool decreasing, bool strictly_decreasing,
                      IsMonotonicOptions options = IsMonotonicOptions::Defaults()) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction("is_monotonic", {input}, &options));
  const StructScalar& output = out.scalar_as<StructScalar>();

  auto out_increasing = std::static_pointer_cast<BooleanScalar>(output.value[0]);
  ASSERT_EQ(increasing, out_increasing->value);
  auto out_strictly_increasing = std::static_pointer_cast<BooleanScalar>(output.value[1]);
  ASSERT_EQ(strictly_increasing, out_strictly_increasing->value);
  auto out_decreasing = std::static_pointer_cast<BooleanScalar>(output.value[2]);
  ASSERT_EQ(decreasing, out_decreasing->value);
  auto out_strictly_decreasing = std::static_pointer_cast<BooleanScalar>(output.value[3]);
  ASSERT_EQ(strictly_decreasing, out_strictly_decreasing->value);
}

TEST(TestIsMonotonicKernel, VectorFunction) {
  // Primitive arrays

  // These tests should early exit (based on length).
  CheckIsMonotonic(ArrayFromJSON(int8(), "[]"), true, true, true, true);
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[null]"), true, true, true, true);
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[0]"), true, true, true, true);

  // // Both monotonic increasing and decreasing when all values are the same.
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[0, 0, 0, 0]"), true, false, true, false);

  // CheckIsMonotonic(ArrayFromJSON(int8(), "[null, 0, 0, 0]"), true, false, false,
  // false); CheckIsMonotonic(ArrayFromJSON(int8(), "[0, 0, 0, null]"), false, false,
  // true, false); CheckIsMonotonic(ArrayFromJSON(int8(), "[0, null, 0, 0]"), false,
  // false, false, false); CheckIsMonotonic(ArrayFromJSON(int8(), "[null, null, null]"),
  // true, false, true, false);

  // // Monotonic (strictly) increasing
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[1, 1, 3, 4]"), true, false, false, false);
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[null, 1, 1, 4]"), true, false, false,
  // false); CheckIsMonotonic(ArrayFromJSON(int8(), "[1, 1, null, 4]"), false, false,
  // false, false); CheckIsMonotonic(ArrayFromJSON(int8(), "[1, 1, 3, null]"), false,
  // false, false, false);

  // CheckIsMonotonic(ArrayFromJSON(int8(), "[-1, 2, 3, 4]"), true, true, false, false);
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[null, 2, 3, 4]"), true, true, false, false);
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[null, null, 3, 4]"), true, false, false,
  //                  false);
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[1, null, 3, 4]"), false, false, false,
  // false); CheckIsMonotonic(ArrayFromJSON(int8(), "[1, 2, 3, null]"), false, false,
  // false, false); CheckIsMonotonic(ArrayFromJSON(int8(), "[1, 2, 1, 2]"), false, false,
  // false, false);

  // // Monotonic (strictly) decreasing
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[4, 4, 2, 1]"), false, false, true, false);
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[4, 4, 2, null]"), false, false, true,
  // false); CheckIsMonotonic(ArrayFromJSON(int8(), "[4, 4, null, 1]"), false, false,
  // false, false); CheckIsMonotonic(ArrayFromJSON(int8(), "[null, 4, 2, 1]"), false,
  // false, false, false);

  // CheckIsMonotonic(ArrayFromJSON(int8(), "[4, 3, 2, 1]"), false, false, true, true);
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[4, 3, 2, null]"), false, false, true, true);
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[4, 3, null, null]"), false, false, true,
  //                  false);
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[4, null, 2, 1]"), false, false, false,
  // false); CheckIsMonotonic(ArrayFromJSON(int8(), "[null, 3, 2, 1]"), false, false,
  // false, false); CheckIsMonotonic(ArrayFromJSON(int8(), "[null, null, 2, 1]"), false,
  // false, false,
  //                  false);
  // CheckIsMonotonic(ArrayFromJSON(int8(), "[4, 3, 4, 3]"), false, false, false, false);
}

}  // namespace compute
}  // namespace arrow
