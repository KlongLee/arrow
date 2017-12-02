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

#include <sstream>

#include "arrow/python/helpers.h"
#include "arrow/python/common.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"

#include <arrow/api.h>

namespace arrow {
namespace py {

#define GET_PRIMITIVE_TYPE(NAME, FACTORY) \
  case Type::NAME:                        \
    return FACTORY()

std::shared_ptr<DataType> GetPrimitiveType(Type::type type) {
  switch (type) {
    case Type::NA:
      return null();
      GET_PRIMITIVE_TYPE(UINT8, uint8);
      GET_PRIMITIVE_TYPE(INT8, int8);
      GET_PRIMITIVE_TYPE(UINT16, uint16);
      GET_PRIMITIVE_TYPE(INT16, int16);
      GET_PRIMITIVE_TYPE(UINT32, uint32);
      GET_PRIMITIVE_TYPE(INT32, int32);
      GET_PRIMITIVE_TYPE(UINT64, uint64);
      GET_PRIMITIVE_TYPE(INT64, int64);
      GET_PRIMITIVE_TYPE(DATE32, date32);
      GET_PRIMITIVE_TYPE(DATE64, date64);
      GET_PRIMITIVE_TYPE(BOOL, boolean);
      GET_PRIMITIVE_TYPE(HALF_FLOAT, float16);
      GET_PRIMITIVE_TYPE(FLOAT, float32);
      GET_PRIMITIVE_TYPE(DOUBLE, float64);
      GET_PRIMITIVE_TYPE(BINARY, binary);
      GET_PRIMITIVE_TYPE(STRING, utf8);
    default:
      return nullptr;
  }
}

namespace internal {

Status ImportModule(const std::string& module_name, OwnedRef* ref) {
  PyObject* module = PyImport_ImportModule(module_name.c_str());
  RETURN_IF_PYERROR();
  ref->reset(module);
  return Status::OK();
}

Status ImportFromModule(const OwnedRef& module, const std::string& name, OwnedRef* ref) {
  /// Assumes that ImportModule was called first
  DCHECK_NE(module.obj(), nullptr) << "Cannot import from nullptr Python module";

  PyObject* attr = PyObject_GetAttrString(module.obj(), name.c_str());
  RETURN_IF_PYERROR();
  ref->reset(attr);
  return Status::OK();
}

Status PythonDecimalToString(PyObject* python_decimal, std::string* out) {
  // Call Python's str(decimal_object)
  OwnedRef str_obj(PyObject_Str(python_decimal));
  RETURN_IF_PYERROR();

  PyObjectStringify str(str_obj.obj());
  RETURN_IF_PYERROR();

  const char* bytes = str.bytes;
  DCHECK_NE(bytes, nullptr);

  Py_ssize_t size = str.size;

  std::string c_string(bytes, size);
  *out = c_string;
  return Status::OK();
}

Status InferDecimalPrecisionAndScale(PyObject* decimal_value, int32_t* precision,
                                     int32_t* scale) {
  OwnedRef decimal_tuple(PyObject_CallMethod(decimal_value, "as_tuple", "()"));
  RETURN_IF_PYERROR();

  OwnedRef decimal_exponent_integer(
      PyObject_GetAttrString(decimal_tuple.obj(), "exponent"));
  RETURN_IF_PYERROR();

  if (precision != NULLPTR) {
    OwnedRef digits_tuple(PyObject_GetAttrString(decimal_tuple.obj(), "digits"));
    RETURN_IF_PYERROR();

    const auto result = static_cast<int32_t>(PyTuple_Size(digits_tuple.obj()));
    RETURN_IF_PYERROR();
    *precision = result;
  }

  if (scale != NULLPTR) {
    const auto result =
        -static_cast<int32_t>(PyLong_AsLong(decimal_exponent_integer.obj()));
    RETURN_IF_PYERROR();
    *scale = result;
  }

  return Status::OK();
}

PyObject* DecimalFromString(PyObject* decimal_constructor,
                            const std::string& decimal_string) {
  DCHECK_NE(decimal_constructor, nullptr);

  auto string_size = decimal_string.size();
  DCHECK_GT(string_size, 0);

  auto string_bytes = decimal_string.c_str();
  DCHECK_NE(string_bytes, nullptr);

  return PyObject_CallFunction(decimal_constructor, const_cast<char*>("s#"), string_bytes,
                               string_size);
}

static const Decimal128 ScaleMultipliers[] = {
    Decimal128(1),
    Decimal128(10),
    Decimal128(100),
    Decimal128(1000),
    Decimal128(10000),
    Decimal128(100000),
    Decimal128(1000000),
    Decimal128(10000000),
    Decimal128(100000000),
    Decimal128(1000000000),
    Decimal128(10000000000),
    Decimal128(100000000000),
    Decimal128(1000000000000),
    Decimal128(10000000000000),
    Decimal128(100000000000000),
    Decimal128(1000000000000000),
    Decimal128(10000000000000000),
    Decimal128(100000000000000000),
    Decimal128(1000000000000000000),
    Decimal128("10000000000000000000"),
    Decimal128("100000000000000000000"),
    Decimal128("1000000000000000000000"),
    Decimal128("10000000000000000000000"),
    Decimal128("100000000000000000000000"),
    Decimal128("1000000000000000000000000"),
    Decimal128("10000000000000000000000000"),
    Decimal128("100000000000000000000000000"),
    Decimal128("1000000000000000000000000000"),
    Decimal128("10000000000000000000000000000"),
    Decimal128("100000000000000000000000000000"),
    Decimal128("1000000000000000000000000000000"),
    Decimal128("10000000000000000000000000000000"),
    Decimal128("100000000000000000000000000000000"),
    Decimal128("1000000000000000000000000000000000"),
    Decimal128("10000000000000000000000000000000000"),
    Decimal128("100000000000000000000000000000000000"),
    Decimal128("1000000000000000000000000000000000000"),
    Decimal128("10000000000000000000000000000000000000"),
    Decimal128("100000000000000000000000000000000000000")};

/// Rescale a decimal value, doesn't check for overflow
static Status Rescale(const Decimal128& value, int32_t original_scale, int32_t new_scale,
                      Decimal128* out) {
  DCHECK_NE(out, NULLPTR);
  DCHECK_NE(original_scale, new_scale);
  const int32_t delta_scale = original_scale - new_scale;
  const int32_t abs_delta_scale = std::abs(delta_scale);
  DCHECK_GE(abs_delta_scale, 1);
  DCHECK_LE(abs_delta_scale, 38);

  const Decimal128 scale_multiplier = ScaleMultipliers[abs_delta_scale];
  const Decimal128 result = value * scale_multiplier;

  if (ARROW_PREDICT_FALSE(result < value)) {
    std::stringstream buf;
    buf << "Rescaling decimal value from original scale " << original_scale
        << " to new scale " << new_scale << " would cause overflow";
    return Status::Invalid(buf.str());
  }

  *out = result;
  return Status::OK();
}

Status DecimalFromPythonDecimal(PyObject* python_decimal, const DecimalType& arrow_type, Decimal128* out) {

  int32_t actual_precision, actual_scale, inferred_precision, inferred_scale;
  std::string string;
  RETURN_NOT_OK(PythonDecimalToString(python_decimal, &string));
  RETURN_NOT_OK(InferDecimalPrecisionAndScale(python_decimal, &actual_precision,
                                                        &actual_scale));

  Decimal128 value;
  RETURN_NOT_OK(
      Decimal128::FromString(string, &value, &inferred_precision, &inferred_scale));

  DCHECK_EQ(actual_precision, inferred_precision);
  DCHECK_EQ(actual_scale, inferred_scale);

  DCHECK_LE(actual_scale, actual_precision);

  const int32_t precision = arrow_type.precision();
  const int32_t scale = arrow_type.scale();

  if (actual_precision > precision) {
    std::stringstream buf;
    buf << "Decimal type with precision " << actual_precision
        << " does not fit into precision inferred from first array element: "
        << precision;
    return Status::Invalid(buf.str());
  }

  if (scale != actual_scale) {
    RETURN_NOT_OK(Rescale(value, actual_scale, scale, &value));
  }
  return Status::OK();
}

}  // namespace internal
}  // namespace py
}  // namespace arrow
