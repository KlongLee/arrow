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

#include "arrow/c/dlpack.h"

#include "arrow/array/array_base.h"
#include "arrow/c/dlpack_abi.h"
#include "arrow/device.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow::dlpack {

namespace {

Result<DLDataType> GetDLDataType(const DataType& type) {
  DLDataType dtype;
  dtype.lanes = 1;
  dtype.bits = type.bit_width();
  switch (type.id()) {
    case Type::INT8:
    case Type::INT16:
    case Type::INT32:
    case Type::INT64:
      dtype.code = DLDataTypeCode::kDLInt;
      return dtype;
    case Type::UINT8:
    case Type::UINT16:
    case Type::UINT32:
    case Type::UINT64:
      dtype.code = DLDataTypeCode::kDLUInt;
      return dtype;
    case Type::HALF_FLOAT:
    case Type::FLOAT:
    case Type::DOUBLE:
      dtype.code = DLDataTypeCode::kDLFloat;
      return dtype;
    case Type::BOOL:
      // DLPack supports byte-packed boolean values
      return Status::TypeError("Bit-packed boolean data type not supported by DLPack.");
    default:
      return Status::TypeError("DataType is not compatible with DLPack spec: ",
                               type.ToString());
  }
}

}  // namespace
struct ManagerCtx {
  std::shared_ptr<ArrayData> ref;
  DLManagedTensor tensor;
};

Result<DLManagedTensor*> ExportArray(const std::shared_ptr<Array>& arr) {
  // Define DLDevice struct nad check if array type is supported
  // by the DLPack protocol at the same time. Raise TypeError if not.
  // Supported data types: int, uint, float with no validity buffer.
  ARROW_ASSIGN_OR_RAISE(auto device, ExportDevice(arr))

  // Define the DLDataType struct
  const DataType* arrow_type = arr->type().get();
  ARROW_ASSIGN_OR_RAISE(auto dlpack_type, GetDLDataType(*arrow_type));

  // Create ManagerCtx with the reference to
  // the data of the array
  std::shared_ptr<ArrayData> array_ref = arr->data();
  std::unique_ptr<ManagerCtx> ctx(new ManagerCtx);
  ctx->ref = array_ref;

  // Define the data pointer to the DLTensor
  // If array is of length 0, data pointer should be NULL
  if (arr->length() == 0) {
    ctx->tensor.dl_tensor.data = NULL;
  } else if (arr->offset() > 0) {
    const auto byte_width = arr->type()->byte_width();
    const auto start = arr->offset() * byte_width;
    ARROW_ASSIGN_OR_RAISE(auto sliced_buffer,
                          SliceBufferSafe(array_ref->buffers[1], start));
    ctx->tensor.dl_tensor.data =
        const_cast<void*>(reinterpret_cast<const void*>(sliced_buffer->address()));
  } else {
    ctx->tensor.dl_tensor.data = const_cast<void*>(
        reinterpret_cast<const void*>(array_ref->buffers[1]->address()));
  }

  ctx->tensor.dl_tensor.device = device;
  ctx->tensor.dl_tensor.ndim = 1;
  ctx->tensor.dl_tensor.dtype = dlpack_type;
  ctx->tensor.dl_tensor.shape = const_cast<int64_t*>(&array_ref->length);
  ctx->tensor.dl_tensor.strides = NULL;
  ctx->tensor.dl_tensor.byte_offset = 0;

  ctx->tensor.manager_ctx = ctx.get();
  ctx->tensor.deleter = [](struct DLManagedTensor* self) {
    delete reinterpret_cast<ManagerCtx*>(self->manager_ctx);
  };
  return &ctx.release()->tensor;
}

Result<DLDevice> ExportDevice(const std::shared_ptr<Array>& arr) {
  // Check if array is supported by the DLPack protocol.
  if (arr->null_count() > 0) {
    return Status::TypeError("Can only use DLPack on arrays with no nulls.");
  }
  const DataType* arrow_type = arr->type().get();
  if (arrow_type->id() == Type::BOOL) {
    return Status::TypeError("Bit-packed boolean data type not supported by DLPack.");
  }
  if (!is_integer(arrow_type->id()) && !is_floating(arrow_type->id())) {
    return Status::TypeError("DataType is not compatible with DLPack spec: ",
                             arrow_type->ToString());
  }

  // Define DLDevice struct
  DLDevice device;
  if (arr->data()->buffers[1]->device_type() == DeviceAllocationType::kCPU) {
    device.device_id = 0;
    device.device_type = DLDeviceType::kDLCPU;
    return device;
  } else {
    return Status::NotImplemented(
        "DLPack support is implemented only for buffers on CPU device.");
  }
}

}  // namespace arrow::dlpack
