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

#ifndef ARROW_PYTHON_PYTHON_TO_ARROW_H
#define ARROW_PYTHON_PYTHON_TO_ARROW_H

#include <memory>
#include <vector>

#include "arrow/python/visibility.h"
#include "arrow/status.h"

// Forward declaring PyObject, see
// https://mail.python.org/pipermail/python-dev/2003-August/037601.html
#ifndef PyObject_HEAD
struct _object;
typedef _object PyObject;
#endif

namespace arrow {

class Buffer;
class DataType;
class MemoryPool;
class RecordBatch;
class Tensor;

namespace io {

class OutputStream;

}  // namespace io

namespace py {

struct ARROW_PYTHON_EXPORT SerializedPyObject {
  std::shared_ptr<RecordBatch> batch;
  std::vector<std::shared_ptr<Tensor>> tensors;
  std::vector<std::shared_ptr<Tensor>> ndarrays;
  std::vector<std::shared_ptr<Buffer>> buffers;

  /// \brief Write serialized Python object to OutputStream
  /// \param[in,out] dst an OutputStream
  /// \return Status
  Status WriteTo(io::OutputStream* dst);

  /// \brief Convert SerializedPyObject to a dict containing the message
  /// components as Buffer instances with minimal memory allocation
  ///
  /// {
  ///   'num_tensors': N,
  ///   'num_buffers': K,
  ///   'data': [Buffer]
  /// }
  ///
  /// Each tensor is written as two buffers, one for the metadata and one for
  /// the body. Therefore, the number of buffers in 'data' is 2 * N + K + 1,
  /// with the first buffer containing the serialized record batch containing
  /// the UnionArray that describes the whole object
  Status GetComponents(MemoryPool* pool, PyObject** out);
};

/// \brief Serialize Python sequence as a SerializedPyObject.
/// \param[in] context Serialization context which contains custom serialization
/// and deserialization callbacks. Can be any Python object with a
/// _serialize_callback method for serialization and a _deserialize_callback
/// method for deserialization. If context is None, no custom serialization
/// will be attempted.
/// \param[in] sequence A Python sequence object to serialize to Arrow data
/// structures
/// \param[out] out The serialized representation
/// \return Status
///
/// Release GIL before calling
ARROW_PYTHON_EXPORT
Status SerializeObject(PyObject* context, PyObject* sequence, SerializedPyObject* out);

/// \brief Serialize an Arrow Tensor as a SerializedPyObject.
/// \param[in] tensor Tensor to be serialized
/// \param[out] out The serialized representation
/// \return Status
ARROW_PYTHON_EXPORT
Status SerializeTensor(std::shared_ptr<Tensor> tensor, py::SerializedPyObject* out);

/// \brief Write the Tensor metadata header to an OutputStream.
/// \param[in] dtype DataType of the Tensor
/// \param[in] shape The shape of the tensor
/// \param[in] tensor_num_bytes The lengh of the Tensor data in bytes
/// \param[in] dst The OutputStream to write the Tensor header to
/// \return Status
ARROW_PYTHON_EXPORT
Status WriteNdarrayHeader(std::shared_ptr<DataType> dtype,
                          const std::vector<int64_t>& shape, int64_t tensor_num_bytes,
                          io::OutputStream* dst);

constexpr int8_t kBoolTag = 0;
constexpr int8_t kIntTag = 1;
constexpr int8_t kPy2IntTag = 2;
constexpr int8_t kBytesTag = 3;
constexpr int8_t kStringTag = 4;
constexpr int8_t kHalfFloatTag = 5;
constexpr int8_t kFloatTag = 6;
constexpr int8_t kDoubleTag = 7;
constexpr int8_t kDate64Tag = 8;
constexpr int8_t kListTag = 9;
constexpr int8_t kDictTag = 10;
constexpr int8_t kTupleTag = 11;
constexpr int8_t kSetTag = 12;
constexpr int8_t kTensorTag = 13;
constexpr int8_t kNdarrayTag = 14;
constexpr int8_t kBufferTag = 15;
constexpr int8_t kNumTags = 16;

}  // namespace py

}  // namespace arrow

#endif  // ARROW_PYTHON_PYTHON_TO_ARROW_H
