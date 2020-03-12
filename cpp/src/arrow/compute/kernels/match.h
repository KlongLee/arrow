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

#include "arrow/array.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

/// \brief Match returns position index of first occurence
// of a value from left array in right array.
///
/// If null occurs in left and right,
/// it returns the index, else returns null.
///
/// \param[in] context the FunctionContext
/// \param[in] left array-like input
/// \param[in] right array-like input
/// \param[out] out resulting datum
///
/// \since 2.0.0
/// \note API not yet finalized
ARROW_EXPORT
Status Match(FunctionContext* context, const Datum& left, const Datum& right, Datum* out);

}  // namespace compute
}  // namespace arrow
