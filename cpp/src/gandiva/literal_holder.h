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

#include <array>
#include <string>
#include <variant>

#include <arrow/type.h>
#include "arrow/type_fwd.h"
#include "gandiva/decimal_scalar.h"
#include "gandiva/visibility.h"

namespace gandiva {

using LiteralHolder =
    std::variant<bool, float, double, int8_t, int16_t, int32_t, int64_t, uint8_t,
                 uint16_t, uint32_t, uint64_t, std::string, DecimalScalar128>;

constexpr std::array<arrow::Type::type, 13> kLiteralHolderTypes{
    arrow::Type::BOOL,      arrow::Type::FLOAT,  arrow::Type::DOUBLE, arrow::Type::INT8,
    arrow::Type::INT16,     arrow::Type::INT32,  arrow::Type::INT64,  arrow::Type::UINT8,
    arrow::Type::UINT16,    arrow::Type::UINT32, arrow::Type::UINT64, arrow::Type::STRING,
    arrow::Type::DECIMAL128};

GANDIVA_EXPORT std::string ToString(const LiteralHolder& holder);

}  // namespace gandiva
