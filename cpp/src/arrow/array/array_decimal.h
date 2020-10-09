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
#include <string>

#include "arrow/array/array_binary.h"
#include "arrow/util/decimal_type_traits.h"
#include "arrow/array/data.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// Template Array class for decimal data
template<uint32_t width>
class BaseDecimalArray : public FixedSizeBinaryArray {
 public:
  using TypeClass = typename DecimalTypeTraits<width>::TypeClass;
  using ValueType = typename DecimalTypeTraits<width>::ValueType;

  using FixedSizeBinaryArray::FixedSizeBinaryArray;

  /// \brief Construct DecimalArray from ArrayData instance
  explicit BaseDecimalArray(const std::shared_ptr<ArrayData>& data);

  std::string FormatValue(int64_t i) const;
};

/// Array class for decimal 128-bit data                                       
class ARROW_EXPORT Decimal128Array : public BaseDecimalArray<128> { 
  using BaseDecimalArray<128>::BaseDecimalArray;                          
};

/// Array class for decimal 256-bit data
class ARROW_EXPORT Decimal256Array : public BaseDecimalArray<256> { 
  using BaseDecimalArray<256>::BaseDecimalArray;                          
};

// Backward compatibility
using DecimalArray = Decimal128Array;


}  // namespace arrow
