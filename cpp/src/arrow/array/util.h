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

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array/data.h"
#include "arrow/compare.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \brief Create a strongly-typed Array instance from generic ArrayData
/// \param[in] data the array contents
/// \return the resulting Array instance
ARROW_EXPORT
std::shared_ptr<Array> MakeArray(const std::shared_ptr<ArrayData>& data);

/// \brief Create a strongly-typed Array instance with all elements null
/// \param[in] type the array type
/// \param[in] length the array length
/// \param[in] pool the memory pool to allocate memory from
ARROW_EXPORT
Result<std::shared_ptr<Array>> MakeArrayOfNull(const std::shared_ptr<DataType>& type,
                                               int64_t length,
                                               MemoryPool* pool = default_memory_pool());

/// \brief Create an Array instance whose slots are the given scalar
/// \param[in] scalar the value with which to fill the array
/// \param[in] length the array length
/// \param[in] pool the memory pool to allocate memory from
ARROW_EXPORT
Result<std::shared_ptr<Array>> MakeArrayFromScalar(
    const Scalar& scalar, int64_t length, MemoryPool* pool = default_memory_pool());

/// \brief Create a strongly-typed Array instance with all elements null
/// \param[in] type the array type
/// \param[in] length the array length
/// \param[out] out resulting Array instance
ARROW_DEPRECATED("Use Result-returning version")
ARROW_EXPORT
Status MakeArrayOfNull(const std::shared_ptr<DataType>& type, int64_t length,
                       std::shared_ptr<Array>* out);

/// \brief Create a strongly-typed Array instance with all elements null
/// \param[in] pool the pool from which memory for this array will be allocated
/// \param[in] type the array type
/// \param[in] length the array length
/// \param[out] out resulting Array instance
ARROW_DEPRECATED("Use Result-returning version")
ARROW_EXPORT
Status MakeArrayOfNull(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                       int64_t length, std::shared_ptr<Array>* out);

/// \brief Create an Array instance whose slots are the given scalar
/// \param[in] scalar the value with which to fill the array
/// \param[in] length the array length
/// \param[out] out resulting Array instance
ARROW_DEPRECATED("Use Result-returning version")
ARROW_EXPORT
Status MakeArrayFromScalar(const Scalar& scalar, int64_t length,
                           std::shared_ptr<Array>* out);

/// \brief Create a strongly-typed Array instance with all elements null
/// \param[in] pool the pool from which memory for this array will be allocated
/// \param[in] scalar the value with which to fill the array
/// \param[in] length the array length
/// \param[out] out resulting Array instance
ARROW_DEPRECATED("Use Result-returning version")
ARROW_EXPORT
Status MakeArrayFromScalar(MemoryPool* pool, const Scalar& scalar, int64_t length,
                           std::shared_ptr<Array>* out);

namespace internal {

/// Given a number of ArrayVectors, treat each ArrayVector as the
/// chunks of a chunked array.  Then rechunk each ArrayVector such that
/// all ArrayVectors are chunked identically.  It is mandatory that
/// all ArrayVectors contain the same total number of elements.
ARROW_EXPORT
std::vector<ArrayVector> RechunkArraysConsistently(const std::vector<ArrayVector>&);

}  // namespace internal
}  // namespace arrow
