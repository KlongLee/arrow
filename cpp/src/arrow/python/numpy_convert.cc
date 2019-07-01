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

#include "arrow/python/numpy_interop.h"

#include "arrow/python/numpy_convert.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/sparse_tensor.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

#include "arrow/python/common.h"
#include "arrow/python/pyarrow.h"
#include "arrow/python/type_traits.h"

namespace arrow {
namespace py {

bool is_contiguous(PyObject* array) {
  if (PyArray_Check(array)) {
    return (PyArray_FLAGS(reinterpret_cast<PyArrayObject*>(array)) &
            (NPY_ARRAY_C_CONTIGUOUS | NPY_ARRAY_F_CONTIGUOUS)) != 0;
  } else {
    return false;
  }
}

NumPyBuffer::NumPyBuffer(PyObject* ao) : Buffer(nullptr, 0) {
  PyAcquireGIL lock;
  arr_ = ao;
  Py_INCREF(ao);

  if (PyArray_Check(ao)) {
    PyArrayObject* ndarray = reinterpret_cast<PyArrayObject*>(ao);
    data_ = reinterpret_cast<const uint8_t*>(PyArray_DATA(ndarray));
    size_ = PyArray_SIZE(ndarray) * PyArray_DESCR(ndarray)->elsize;
    capacity_ = size_;

    if (PyArray_FLAGS(ndarray) & NPY_ARRAY_WRITEABLE) {
      is_mutable_ = true;
    }
  }
}

NumPyBuffer::~NumPyBuffer() {
  PyAcquireGIL lock;
  Py_XDECREF(arr_);
}

#define TO_ARROW_TYPE_CASE(NPY_NAME, FACTORY) \
  case NPY_##NPY_NAME:                        \
    *out = FACTORY();                         \
    break;

Status GetTensorType(PyObject* dtype, std::shared_ptr<DataType>* out) {
  if (!PyArray_DescrCheck(dtype)) {
    return Status::TypeError("Did not pass numpy.dtype object");
  }
  PyArray_Descr* descr = reinterpret_cast<PyArray_Descr*>(dtype);
  int type_num = fix_numpy_type_num(descr->type_num);

  switch (type_num) {
    TO_ARROW_TYPE_CASE(BOOL, uint8);
    TO_ARROW_TYPE_CASE(INT8, int8);
    TO_ARROW_TYPE_CASE(INT16, int16);
    TO_ARROW_TYPE_CASE(INT32, int32);
    TO_ARROW_TYPE_CASE(INT64, int64);
    TO_ARROW_TYPE_CASE(UINT8, uint8);
    TO_ARROW_TYPE_CASE(UINT16, uint16);
    TO_ARROW_TYPE_CASE(UINT32, uint32);
    TO_ARROW_TYPE_CASE(UINT64, uint64);
    TO_ARROW_TYPE_CASE(FLOAT16, float16);
    TO_ARROW_TYPE_CASE(FLOAT32, float32);
    TO_ARROW_TYPE_CASE(FLOAT64, float64);
    default: {
      return Status::NotImplemented("Unsupported numpy type ", descr->type_num);
    }
  }
  return Status::OK();
}

Status GetNumPyType(const DataType& type, int* type_num) {
#define NUMPY_TYPE_CASE(ARROW_NAME, NPY_NAME) \
  case Type::ARROW_NAME:                      \
    *type_num = NPY_##NPY_NAME;               \
    break;

  switch (type.id()) {
    NUMPY_TYPE_CASE(UINT8, UINT8);
    NUMPY_TYPE_CASE(INT8, INT8);
    NUMPY_TYPE_CASE(UINT16, UINT16);
    NUMPY_TYPE_CASE(INT16, INT16);
    NUMPY_TYPE_CASE(UINT32, UINT32);
    NUMPY_TYPE_CASE(INT32, INT32);
    NUMPY_TYPE_CASE(UINT64, UINT64);
    NUMPY_TYPE_CASE(INT64, INT64);
    NUMPY_TYPE_CASE(HALF_FLOAT, FLOAT16);
    NUMPY_TYPE_CASE(FLOAT, FLOAT32);
    NUMPY_TYPE_CASE(DOUBLE, FLOAT64);
    default: {
      return Status::NotImplemented("Unsupported tensor type: ", type.ToString());
    }
  }
#undef NUMPY_TYPE_CASE

  return Status::OK();
}

Status NumPyDtypeToArrow(PyObject* dtype, std::shared_ptr<DataType>* out) {
  if (!PyArray_DescrCheck(dtype)) {
    return Status::TypeError("Did not pass numpy.dtype object");
  }
  PyArray_Descr* descr = reinterpret_cast<PyArray_Descr*>(dtype);
  return NumPyDtypeToArrow(descr, out);
}

Status NumPyDtypeToArrow(PyArray_Descr* descr, std::shared_ptr<DataType>* out) {
  int type_num = fix_numpy_type_num(descr->type_num);

  switch (type_num) {
    TO_ARROW_TYPE_CASE(BOOL, boolean);
    TO_ARROW_TYPE_CASE(INT8, int8);
    TO_ARROW_TYPE_CASE(INT16, int16);
    TO_ARROW_TYPE_CASE(INT32, int32);
    TO_ARROW_TYPE_CASE(INT64, int64);
    TO_ARROW_TYPE_CASE(UINT8, uint8);
    TO_ARROW_TYPE_CASE(UINT16, uint16);
    TO_ARROW_TYPE_CASE(UINT32, uint32);
    TO_ARROW_TYPE_CASE(UINT64, uint64);
    TO_ARROW_TYPE_CASE(FLOAT16, float16);
    TO_ARROW_TYPE_CASE(FLOAT32, float32);
    TO_ARROW_TYPE_CASE(FLOAT64, float64);
    TO_ARROW_TYPE_CASE(STRING, binary);
    TO_ARROW_TYPE_CASE(UNICODE, utf8);
    case NPY_DATETIME: {
      auto date_dtype =
          reinterpret_cast<PyArray_DatetimeDTypeMetaData*>(descr->c_metadata);
      switch (date_dtype->meta.base) {
        case NPY_FR_s:
          *out = timestamp(TimeUnit::SECOND);
          break;
        case NPY_FR_ms:
          *out = timestamp(TimeUnit::MILLI);
          break;
        case NPY_FR_us:
          *out = timestamp(TimeUnit::MICRO);
          break;
        case NPY_FR_ns:
          *out = timestamp(TimeUnit::NANO);
          break;
        case NPY_FR_D:
          *out = date32();
          break;
        case NPY_FR_GENERIC:
          return Status::NotImplemented("Unbound or generic datetime64 time unit");
        default:
          return Status::NotImplemented("Unsupported datetime64 time unit");
      }
    } break;
    default: {
      return Status::NotImplemented("Unsupported numpy type ", descr->type_num);
    }
  }

  return Status::OK();
}

#undef TO_ARROW_TYPE_CASE

Status NdarrayToTensor(MemoryPool* pool, PyObject* ao,
                       const std::vector<std::string>& dim_names,
                       std::shared_ptr<Tensor>* out) {
  if (!PyArray_Check(ao)) {
    return Status::TypeError("Did not pass ndarray object");
  }

  PyArrayObject* ndarray = reinterpret_cast<PyArrayObject*>(ao);

  // TODO(wesm): What do we want to do with non-contiguous memory and negative strides?

  int ndim = PyArray_NDIM(ndarray);

  std::shared_ptr<Buffer> data = std::make_shared<NumPyBuffer>(ao);
  std::vector<int64_t> shape(ndim);
  std::vector<int64_t> strides(ndim);

  npy_intp* array_strides = PyArray_STRIDES(ndarray);
  npy_intp* array_shape = PyArray_SHAPE(ndarray);
  for (int i = 0; i < ndim; ++i) {
    if (array_strides[i] < 0) {
      return Status::Invalid("Negative ndarray strides not supported");
    }
    shape[i] = array_shape[i];
    strides[i] = array_strides[i];
  }

  std::shared_ptr<DataType> type;
  RETURN_NOT_OK(
      GetTensorType(reinterpret_cast<PyObject*>(PyArray_DESCR(ndarray)), &type));
  *out = std::make_shared<Tensor>(type, data, shape, strides, dim_names);
  return Status::OK();
}

Status TensorToNdarray(const std::shared_ptr<Tensor>& tensor, PyObject* base,
                       PyObject** out) {
  int type_num;
  RETURN_NOT_OK(GetNumPyType(*tensor->type(), &type_num));
  PyArray_Descr* dtype = PyArray_DescrNewFromType(type_num);
  RETURN_IF_PYERROR();

  const int ndim = tensor->ndim();
  std::vector<npy_intp> npy_shape(ndim);
  std::vector<npy_intp> npy_strides(ndim);

  for (int i = 0; i < ndim; ++i) {
    npy_shape[i] = tensor->shape()[i];
    npy_strides[i] = tensor->strides()[i];
  }

  const void* immutable_data = nullptr;
  if (tensor->data()) {
    immutable_data = tensor->data()->data();
  }

  // Remove const =(
  void* mutable_data = const_cast<void*>(immutable_data);

  int array_flags = 0;
  if (tensor->is_row_major()) {
    array_flags |= NPY_ARRAY_C_CONTIGUOUS;
  }
  if (tensor->is_column_major()) {
    array_flags |= NPY_ARRAY_F_CONTIGUOUS;
  }
  if (tensor->is_mutable()) {
    array_flags |= NPY_ARRAY_WRITEABLE;
  }

  PyObject* result =
      PyArray_NewFromDescr(&PyArray_Type, dtype, ndim, npy_shape.data(),
                           npy_strides.data(), mutable_data, array_flags, nullptr);
  RETURN_IF_PYERROR()

  if (base == Py_None || base == nullptr) {
    base = py::wrap_tensor(tensor);
  } else {
    Py_XINCREF(base);
  }
  PyArray_SetBaseObject(reinterpret_cast<PyArrayObject*>(result), base);
  *out = result;
  return Status::OK();
}

// Wrap the dense data of a sparse tensor in a ndarray
static Status SparseTendorDataToNdarray(const SparseTensor& sparse_tensor,
                                        std::vector<npy_intp> data_shape, PyObject* base,
                                        PyObject** out_data) {
  int type_num_data;
  RETURN_NOT_OK(GetNumPyType(*sparse_tensor.type(), &type_num_data));
  PyArray_Descr* dtype_data = PyArray_DescrNewFromType(type_num_data);
  RETURN_IF_PYERROR();

  const void* immutable_data = sparse_tensor.data()->data();
  // Remove const =(
  void* mutable_data = const_cast<void*>(immutable_data);
  int array_flags = NPY_ARRAY_C_CONTIGUOUS | NPY_ARRAY_F_CONTIGUOUS;
  if (sparse_tensor.is_mutable()) {
    array_flags |= NPY_ARRAY_WRITEABLE;
  }

  *out_data = PyArray_NewFromDescr(&PyArray_Type, dtype_data,
                                   static_cast<int>(data_shape.size()), data_shape.data(),
                                   nullptr, mutable_data, array_flags, nullptr);
  RETURN_IF_PYERROR()
  Py_XINCREF(base);
  PyArray_SetBaseObject(reinterpret_cast<PyArrayObject*>(*out_data), base);
  return Status::OK();
}

Status SparseTensorCOOToNdarray(const std::shared_ptr<SparseTensorCOO>& sparse_tensor,
                                PyObject* base, PyObject** out_data,
                                PyObject** out_coords) {
  const auto& sparse_index = arrow::internal::checked_cast<const SparseCOOIndex&>(
      *sparse_tensor->sparse_index());

  // Wrap tensor data
  OwnedRef result_data;
  RETURN_NOT_OK(SparseTendorDataToNdarray(
      *sparse_tensor, {sparse_index.non_zero_length(), 1}, base, result_data.ref()));

  // Wrap indices
  PyObject* result_coords;
  RETURN_NOT_OK(TensorToNdarray(sparse_index.indices(), base, &result_coords));

  *out_data = result_data.detach();
  *out_coords = result_coords;
  return Status::OK();
}

Status SparseTensorCSRToNdarray(const std::shared_ptr<SparseTensorCSR>& sparse_tensor,
                                PyObject* base, PyObject** out_data,
                                PyObject** out_indptr, PyObject** out_indices) {
  const auto& sparse_index = arrow::internal::checked_cast<const SparseCSRIndex&>(
      *sparse_tensor->sparse_index());

  // Wrap tensor data
  OwnedRef result_data;
  RETURN_NOT_OK(SparseTendorDataToNdarray(
      *sparse_tensor, {sparse_index.non_zero_length(), 1}, base, result_data.ref()));

  // Wrap indices
  OwnedRef result_indptr;
  OwnedRef result_indices;
  RETURN_NOT_OK(TensorToNdarray(sparse_index.indptr(), base, result_indptr.ref()));
  RETURN_NOT_OK(TensorToNdarray(sparse_index.indices(), base, result_indices.ref()));

  *out_data = result_data.detach();
  *out_indptr = result_indptr.detach();
  *out_indices = result_indices.detach();
  return Status::OK();
}

Status NdarraysToSparseTensorCOO(MemoryPool* pool, PyObject* data_ao, PyObject* coords_ao,
                                 const std::vector<int64_t>& shape,
                                 const std::vector<std::string>& dim_names,
                                 std::shared_ptr<SparseTensorCOO>* out) {
  if (!PyArray_Check(data_ao) || !PyArray_Check(coords_ao)) {
    return Status::TypeError("Did not pass ndarray object");
  }

  PyArrayObject* ndarray_data = reinterpret_cast<PyArrayObject*>(data_ao);
  std::shared_ptr<Buffer> data = std::make_shared<NumPyBuffer>(data_ao);
  std::shared_ptr<DataType> type_data;
  RETURN_NOT_OK(GetTensorType(reinterpret_cast<PyObject*>(PyArray_DESCR(ndarray_data)),
                              &type_data));

  std::shared_ptr<Tensor> coords;
  RETURN_NOT_OK(NdarrayToTensor(pool, coords_ao, {}, &coords));
  ARROW_CHECK_EQ(coords->type_id(), Type::INT64);  // Should be ensured by caller

  std::shared_ptr<SparseCOOIndex> sparse_index = std::make_shared<SparseCOOIndex>(
      std::static_pointer_cast<NumericTensor<Int64Type>>(coords));
  *out = std::make_shared<SparseTensorImpl<SparseCOOIndex>>(sparse_index, type_data, data,
                                                            shape, dim_names);
  return Status::OK();
}

Status NdarraysToSparseTensorCSR(MemoryPool* pool, PyObject* data_ao, PyObject* indptr_ao,
                                 PyObject* indices_ao, const std::vector<int64_t>& shape,
                                 const std::vector<std::string>& dim_names,
                                 std::shared_ptr<SparseTensorCSR>* out) {
  if (!PyArray_Check(data_ao) || !PyArray_Check(indptr_ao) ||
      !PyArray_Check(indices_ao)) {
    return Status::TypeError("Did not pass ndarray object");
  }

  PyArrayObject* ndarray_data = reinterpret_cast<PyArrayObject*>(data_ao);
  std::shared_ptr<Buffer> data = std::make_shared<NumPyBuffer>(data_ao);
  std::shared_ptr<DataType> type_data;
  RETURN_NOT_OK(GetTensorType(reinterpret_cast<PyObject*>(PyArray_DESCR(ndarray_data)),
                              &type_data));

  std::shared_ptr<Tensor> indptr, indices;
  RETURN_NOT_OK(NdarrayToTensor(pool, indptr_ao, {}, &indptr));
  RETURN_NOT_OK(NdarrayToTensor(pool, indices_ao, {}, &indices));
  ARROW_CHECK_EQ(indptr->type_id(), Type::INT64);   // Should be ensured by caller
  ARROW_CHECK_EQ(indices->type_id(), Type::INT64);  // Should be ensured by caller

  auto sparse_index = std::make_shared<SparseCSRIndex>(
      std::static_pointer_cast<NumericTensor<Int64Type>>(indptr),
      std::static_pointer_cast<NumericTensor<Int64Type>>(indices));
  *out = std::make_shared<SparseTensorImpl<SparseCSRIndex>>(sparse_index, type_data, data,
                                                            shape, dim_names);
  return Status::OK();
}

Status TensorToSparseTensorCOO(const std::shared_ptr<Tensor>& tensor,
                               std::shared_ptr<SparseTensorCOO>* out) {
  *out = std::make_shared<SparseTensorCOO>(*tensor);
  return Status::OK();
}

Status TensorToSparseTensorCSR(const std::shared_ptr<Tensor>& tensor,
                               std::shared_ptr<SparseTensorCSR>* out) {
  *out = std::make_shared<SparseTensorCSR>(*tensor);
  return Status::OK();
}

}  // namespace py
}  // namespace arrow
