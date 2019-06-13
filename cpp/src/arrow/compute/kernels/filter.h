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

#include <memory>

#include "arrow/compute/kernel.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;

namespace compute {

class FunctionContext;

/// \brief Filter an array with a boolean selection filter
///
/// The output array will be populated with values from the input at positions
/// where the selection filter is not 0. Nulls in the filter will result in nulls
/// in the output.
///
/// For example given values = ["a", "b", "c", null, "e", "f"] and
/// filter = [0, 1, 1, 0, null, 1], the output will be
/// = ["b", "c", null, "f"]
///
/// \param[in] context the FunctionContext
/// \param[in] values array from which to take
/// \param[in] filter indicates which values should be filtered out
/// \param[out] out resulting array
ARROW_EXPORT
Status Filter(FunctionContext* context, const Array& values, const Array& filter,
              std::shared_ptr<Array>* out);

/// \brief Filter an array with a boolean selection filter
///
/// \param[in] context the FunctionContext
/// \param[in] values datum from which to take
/// \param[in] filter indicates which values should be filtered out
/// \param[out] out resulting datum
ARROW_EXPORT
Status Filter(FunctionContext* context, const Datum& values, const Datum& filter,
              Datum* out);

}  // namespace compute
}  // namespace arrow
