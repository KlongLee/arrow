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

#pragma once

#include <vector>

#include "gandiva/native_function.h"

namespace gandiva {

std::vector<NativeFunction> GetNullFunctionRegistry() {
  static std::vector<NativeFunction> null_fn_registry_ = {
      NativeFunction("equal",
                     {"not_equal", "less_than", "less_than_or_equal_to", "greater_than",
                      "greater_than_or_equal_to"},
                     DataTypeVector{null(), null()}, boolean(), kResultNullNever,
                     "compare_null_null"),
      NativeFunction("isnull", {}, DataTypeVector{null()}, boolean(), kResultNullNever,
                     "isnull_null"),
      NativeFunction("isnotnull", {}, DataTypeVector{null()}, boolean(), kResultNullNever,
                     "isnotnull_null")};
  return null_fn_registry_;
}

}  // namespace gandiva
