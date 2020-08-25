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

// helpers.h includes a NumPy header, so we include this first
#include "arrow/python/numpy_interop.h"

#include "arrow/python/helpers.h"

#include <limits>
#include <mutex>
#include <sstream>
#include <type_traits>
#include <typeinfo>

#include "arrow/python/common.h"
#include "arrow/python/decimal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

#include <arrow/api.h>

namespace arrow {

using internal::checked_cast;

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
      GET_PRIMITIVE_TYPE(LARGE_BINARY, large_binary);
      GET_PRIMITIVE_TYPE(LARGE_STRING, large_utf8);
    default:
      return nullptr;
  }
}

PyObject* PyHalf_FromHalf(npy_half value) {
  PyObject* result = PyArrayScalar_New(Half);
  if (result != NULL) {
    PyArrayScalar_ASSIGN(result, Half, value);
  }
  return result;
}

Status PyFloat_AsHalf(PyObject* obj, npy_half* out) {
  if (PyArray_IsScalar(obj, Half)) {
    *out = PyArrayScalar_VAL(obj, Half);
    return Status::OK();
  } else {
    // XXX: cannot use npy_double_to_half() without linking with Numpy
    return Status::TypeError("Expected np.float16 instance");
  }
}

namespace internal {

std::string PyBytes_AsStdString(PyObject* obj) {
  DCHECK(PyBytes_Check(obj));
  return std::string(PyBytes_AS_STRING(obj), PyBytes_GET_SIZE(obj));
}

Status PyUnicode_AsStdString(PyObject* obj, std::string* out) {
  DCHECK(PyUnicode_Check(obj));
  Py_ssize_t size;
  // The utf-8 representation is cached on the unicode object
  const char* data = PyUnicode_AsUTF8AndSize(obj, &size);
  RETURN_IF_PYERROR();
  *out = std::string(data, size);
  return Status::OK();
}

std::string PyObject_StdStringRepr(PyObject* obj) {
  OwnedRef unicode_ref(PyObject_Repr(obj));
  OwnedRef bytes_ref;

  if (unicode_ref) {
    bytes_ref.reset(
        PyUnicode_AsEncodedString(unicode_ref.obj(), "utf8", "backslashreplace"));
  }
  if (!bytes_ref) {
    PyErr_Clear();
    std::stringstream ss;
    ss << "<object of type '" << Py_TYPE(obj)->tp_name << "' repr() failed>";
    return ss.str();
  }
  return PyBytes_AsStdString(bytes_ref.obj());
}

Status PyObject_StdStringStr(PyObject* obj, std::string* out) {
  OwnedRef string_ref(PyObject_Str(obj));
  RETURN_IF_PYERROR();
  return PyUnicode_AsStdString(string_ref.obj(), out);
}

Status ImportModule(const std::string& module_name, OwnedRef* ref) {
  PyObject* module = PyImport_ImportModule(module_name.c_str());
  RETURN_IF_PYERROR();
  ref->reset(module);
  return Status::OK();
}

Status ImportFromModule(PyObject* module, const std::string& name, OwnedRef* ref) {
  PyObject* attr = PyObject_GetAttrString(module, name.c_str());
  RETURN_IF_PYERROR();
  ref->reset(attr);
  return Status::OK();
}

namespace {

Status IntegerOverflowStatus(PyObject* obj, const std::string& overflow_message) {
  if (overflow_message.empty()) {
    std::string obj_as_stdstring;
    RETURN_NOT_OK(PyObject_StdStringStr(obj, &obj_as_stdstring));
    return Status::Invalid("Value ", obj_as_stdstring,
                           " too large to fit in C integer type");
  } else {
    return Status::Invalid(overflow_message);
  }
}

// Extract C signed int from Python object
template <typename Int, enable_if_t<std::is_signed<Int>::value, Int> = 0>
Status CIntFromPythonImpl(PyObject* obj, Int* out, const std::string& overflow_message) {
  static_assert(sizeof(Int) <= sizeof(long long),  // NOLINT
                "integer type larger than long long");

  if (sizeof(Int) > sizeof(long)) {  // NOLINT
    const auto value = PyLong_AsLongLong(obj);
    if (ARROW_PREDICT_FALSE(value == -1)) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value < std::numeric_limits<Int>::min() ||
                            value > std::numeric_limits<Int>::max())) {
      return IntegerOverflowStatus(obj, overflow_message);
    }
    *out = static_cast<Int>(value);
  } else {
    const auto value = PyLong_AsLong(obj);
    if (ARROW_PREDICT_FALSE(value == -1)) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value < std::numeric_limits<Int>::min() ||
                            value > std::numeric_limits<Int>::max())) {
      return IntegerOverflowStatus(obj, overflow_message);
    }
    *out = static_cast<Int>(value);
  }
  return Status::OK();
}

// Extract C unsigned int from Python object
template <typename Int, enable_if_t<std::is_unsigned<Int>::value, Int> = 0>
Status CIntFromPythonImpl(PyObject* obj, Int* out, const std::string& overflow_message) {
  static_assert(sizeof(Int) <= sizeof(unsigned long long),  // NOLINT
                "integer type larger than unsigned long long");

  OwnedRef ref;
  // PyLong_AsUnsignedLong() and PyLong_AsUnsignedLongLong() don't handle
  // conversion from non-ints (e.g. np.uint64), so do it ourselves
  if (!PyLong_Check(obj)) {
    ref.reset(PyNumber_Long(obj));
    if (!ref) {
      RETURN_IF_PYERROR();
    }
    obj = ref.obj();
  }
  if (sizeof(Int) > sizeof(unsigned long)) {  // NOLINT
    const auto value = PyLong_AsUnsignedLongLong(obj);
    if (ARROW_PREDICT_FALSE(value == static_cast<decltype(value)>(-1))) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value > std::numeric_limits<Int>::max())) {
      return IntegerOverflowStatus(obj, overflow_message);
    }
    *out = static_cast<Int>(value);
  } else {
    const auto value = PyLong_AsUnsignedLong(obj);
    if (ARROW_PREDICT_FALSE(value == static_cast<decltype(value)>(-1))) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value > std::numeric_limits<Int>::max())) {
      return IntegerOverflowStatus(obj, overflow_message);
    }
    *out = static_cast<Int>(value);
  }
  return Status::OK();
}

}  // namespace

template <typename Int>
Status CIntFromPython(PyObject* obj, Int* out, const std::string& overflow_message) {
  if (PyBool_Check(obj)) {
    return Status::TypeError("Expected integer, got bool");
  }
  return CIntFromPythonImpl(obj, out, overflow_message);
}

template Status CIntFromPython(PyObject*, int8_t*, const std::string&);
template Status CIntFromPython(PyObject*, int16_t*, const std::string&);
template Status CIntFromPython(PyObject*, int32_t*, const std::string&);
template Status CIntFromPython(PyObject*, int64_t*, const std::string&);
template Status CIntFromPython(PyObject*, uint8_t*, const std::string&);
template Status CIntFromPython(PyObject*, uint16_t*, const std::string&);
template Status CIntFromPython(PyObject*, uint32_t*, const std::string&);
template Status CIntFromPython(PyObject*, uint64_t*, const std::string&);

inline bool MayHaveNaN(PyObject* obj) {
  // Some core types can be very quickly type-checked and do not allow NaN values
  const int64_t non_nan_tpflags = Py_TPFLAGS_LONG_SUBCLASS | Py_TPFLAGS_LIST_SUBCLASS |
                                  Py_TPFLAGS_TUPLE_SUBCLASS | Py_TPFLAGS_BYTES_SUBCLASS |
                                  Py_TPFLAGS_UNICODE_SUBCLASS | Py_TPFLAGS_DICT_SUBCLASS |
                                  Py_TPFLAGS_BASE_EXC_SUBCLASS | Py_TPFLAGS_TYPE_SUBCLASS;
  return !PyType_HasFeature(Py_TYPE(obj), non_nan_tpflags);
}

bool PyFloat_IsNaN(PyObject* obj) {
  return PyFloat_Check(obj) && std::isnan(PyFloat_AsDouble(obj));
}

namespace {

static std::once_flag pandas_static_initialized;
static PyTypeObject* pandas_NaTType = nullptr;
static PyObject* pandas_NA = nullptr;

void GetPandasStaticSymbols() {
  OwnedRef pandas;
  Status s = ImportModule("pandas", &pandas);
  if (!s.ok()) {
    return;
  }

  OwnedRef ref;
  s = ImportFromModule(pandas.obj(), "NaT", &ref);
  if (!s.ok()) {
    return;
  }
  PyObject* nat_type = PyObject_Type(ref.obj());
  pandas_NaTType = reinterpret_cast<PyTypeObject*>(nat_type);

  // PyObject_Type returns a new reference but we trust that pandas.NaT will
  // outlive our use of this PyObject*
  Py_DECREF(nat_type);

  if (ImportFromModule(pandas.obj(), "NA", &ref).ok()) {
    // If pandas.NA exists, retain a reference to it
    pandas_NA = ref.obj();
  }
}

}  // namespace

void InitPandasStaticData() {
  std::call_once(pandas_static_initialized, GetPandasStaticSymbols);
}

bool PandasObjectIsNull(PyObject* obj) {
  if (!MayHaveNaN(obj)) {
    return false;
  }
  if (obj == Py_None) {
    return true;
  }
  if (PyFloat_IsNaN(obj) || (pandas_NA && obj == pandas_NA) ||
      (pandas_NaTType && PyObject_TypeCheck(obj, pandas_NaTType)) ||
      (internal::PyDecimal_Check(obj) && internal::PyDecimal_ISNAN(obj))) {
    return true;
  }
  return false;
}

Status InvalidValue(PyObject* obj, const std::string& why) {
  std::string obj_as_str;
  RETURN_NOT_OK(internal::PyObject_StdStringStr(obj, &obj_as_str));
  return Status::Invalid("Could not convert ", obj_as_str, " with type ",
                         Py_TYPE(obj)->tp_name, ": ", why);
}

Status InvalidType(PyObject* obj, const std::string& why) {
  std::string obj_as_str;
  RETURN_NOT_OK(internal::PyObject_StdStringStr(obj, &obj_as_str));
  return Status::TypeError("Could not convert ", obj_as_str, " with type ",
                           Py_TYPE(obj)->tp_name, ": ", why);
}

Status UnboxIntegerAsInt64(PyObject* obj, int64_t* out) {
  if (PyLong_Check(obj)) {
    int overflow = 0;
    *out = PyLong_AsLongLongAndOverflow(obj, &overflow);
    if (overflow) {
      return Status::Invalid("PyLong is too large to fit int64");
    }
  } else if (PyArray_IsScalar(obj, Byte)) {
    *out = reinterpret_cast<PyByteScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, UByte)) {
    *out = reinterpret_cast<PyUByteScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, Short)) {
    *out = reinterpret_cast<PyShortScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, UShort)) {
    *out = reinterpret_cast<PyUShortScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, Int)) {
    *out = reinterpret_cast<PyIntScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, UInt)) {
    *out = reinterpret_cast<PyUIntScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, Long)) {
    *out = reinterpret_cast<PyLongScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, ULong)) {
    *out = reinterpret_cast<PyULongScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, LongLong)) {
    *out = reinterpret_cast<PyLongLongScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, Int64)) {
    *out = reinterpret_cast<PyInt64ScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, ULongLong)) {
    *out = reinterpret_cast<PyULongLongScalarObject*>(obj)->obval;
  } else if (PyArray_IsScalar(obj, UInt64)) {
    *out = reinterpret_cast<PyUInt64ScalarObject*>(obj)->obval;
  } else {
    return Status::Invalid("Integer scalar type not recognized");
  }
  return Status::OK();
}

Status IntegerScalarToDoubleSafe(PyObject* obj, double* out) {
  int64_t value = 0;
  RETURN_NOT_OK(UnboxIntegerAsInt64(obj, &value));

  constexpr int64_t kDoubleMax = 1LL << 53;
  constexpr int64_t kDoubleMin = -(1LL << 53);

  if (value < kDoubleMin || value > kDoubleMax) {
    return Status::Invalid("Integer value ", value, " is outside of the range exactly",
                           " representable by a IEEE 754 double precision value");
  }
  *out = static_cast<double>(value);
  return Status::OK();
}

Status IntegerScalarToFloat32Safe(PyObject* obj, float* out) {
  int64_t value = 0;
  RETURN_NOT_OK(UnboxIntegerAsInt64(obj, &value));

  constexpr int64_t kFloatMax = 1LL << 24;
  constexpr int64_t kFloatMin = -(1LL << 24);

  if (value < kFloatMin || value > kFloatMax) {
    return Status::Invalid("Integer value ", value, " is outside of the range exactly",
                           " representable by a IEEE 754 single precision value");
  }
  *out = static_cast<float>(value);
  return Status::OK();
}

void DebugPrint(PyObject* obj) {
  std::string repr = PyObject_StdStringRepr(obj);
  PySys_WriteStderr("%s\n", repr.c_str());
}

}  // namespace internal
}  // namespace py
}  // namespace arrow
