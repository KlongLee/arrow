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

#include "arrow/array/util.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/concatenate.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/extension_type.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/sort.h"
#include "arrow/util/span.h"
#include "arrow/visit_data_inline.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// Loading from ArrayData

namespace {

class ArrayDataWrapper {
 public:
  ArrayDataWrapper(const std::shared_ptr<ArrayData>& data, std::shared_ptr<Array>* out)
      : data_(data), out_(out) {}

  template <typename T>
  Status Visit(const T&) {
    using ArrayType = typename TypeTraits<T>::ArrayType;
    *out_ = std::make_shared<ArrayType>(data_);
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    *out_ = type.MakeArray(data_);
    return Status::OK();
  }

  const std::shared_ptr<ArrayData>& data_;
  std::shared_ptr<Array>* out_;
};

class ArrayDataEndianSwapper {
 public:
  explicit ArrayDataEndianSwapper(const std::shared_ptr<ArrayData>& data) : data_(data) {
    out_ = data->Copy();
  }

  // WARNING: this facility can be called on invalid Array data by the IPC reader.
  // Do not rely on the advertised ArrayData length, instead use the physical
  // buffer sizes to avoid accessing memory out of bounds.
  //
  // (If this guarantee turns out to be difficult to maintain, we should call
  //  Validate() instead)
  Status SwapType(const DataType& type) {
    RETURN_NOT_OK(VisitTypeInline(type, this));
    RETURN_NOT_OK(SwapChildren(type.fields()));
    if (internal::HasValidityBitmap(type.id())) {
      // Copy null bitmap
      out_->buffers[0] = data_->buffers[0];
    }
    return Status::OK();
  }

  Status SwapChildren(const FieldVector& child_fields) {
    for (size_t i = 0; i < child_fields.size(); i++) {
      ARROW_ASSIGN_OR_RAISE(out_->child_data[i],
                            internal::SwapEndianArrayData(data_->child_data[i]));
    }
    return Status::OK();
  }

  template <typename T>
  Result<std::shared_ptr<Buffer>> ByteSwapBuffer(
      const std::shared_ptr<Buffer>& in_buffer) {
    if (sizeof(T) == 1) {
      // if data size is 1, element is not swapped. We can use the original buffer
      return in_buffer;
    }
    auto in_data = reinterpret_cast<const T*>(in_buffer->data());
    ARROW_ASSIGN_OR_RAISE(auto out_buffer, AllocateBuffer(in_buffer->size()));
    auto out_data = reinterpret_cast<T*>(out_buffer->mutable_data());
    // NOTE: data_->length not trusted (see warning above)
    int64_t length = in_buffer->size() / sizeof(T);
    for (int64_t i = 0; i < length; i++) {
      out_data[i] = bit_util::ByteSwap(in_data[i]);
    }
    return std::move(out_buffer);
  }

  template <typename VALUE_TYPE>
  Status SwapOffsets(int index) {
    if (data_->buffers[index] == nullptr || data_->buffers[index]->size() == 0) {
      out_->buffers[index] = data_->buffers[index];
      return Status::OK();
    }
    // Except union, offset has one more element rather than data->length
    ARROW_ASSIGN_OR_RAISE(out_->buffers[index],
                          ByteSwapBuffer<VALUE_TYPE>(data_->buffers[index]));
    return Status::OK();
  }

  template <typename T>
  enable_if_t<std::is_base_of<FixedWidthType, T>::value &&
                  !std::is_base_of<FixedSizeBinaryType, T>::value &&
                  !std::is_base_of<DictionaryType, T>::value,
              Status>
  Visit(const T& type) {
    using value_type = typename T::c_type;
    ARROW_ASSIGN_OR_RAISE(out_->buffers[1],
                          ByteSwapBuffer<value_type>(data_->buffers[1]));
    return Status::OK();
  }

  Status Visit(const Decimal128Type& type) {
    auto data = reinterpret_cast<const uint64_t*>(data_->buffers[1]->data());
    ARROW_ASSIGN_OR_RAISE(auto new_buffer, AllocateBuffer(data_->buffers[1]->size()));
    auto new_data = reinterpret_cast<uint64_t*>(new_buffer->mutable_data());
    // NOTE: data_->length not trusted (see warning above)
    const int64_t length = data_->buffers[1]->size() / Decimal128Type::kByteWidth;
    for (int64_t i = 0; i < length; i++) {
      uint64_t tmp;
      auto idx = i * 2;
#if ARROW_LITTLE_ENDIAN
      tmp = bit_util::FromBigEndian(data[idx]);
      new_data[idx] = bit_util::FromBigEndian(data[idx + 1]);
      new_data[idx + 1] = tmp;
#else
      tmp = bit_util::FromLittleEndian(data[idx]);
      new_data[idx] = bit_util::FromLittleEndian(data[idx + 1]);
      new_data[idx + 1] = tmp;
#endif
    }
    out_->buffers[1] = std::move(new_buffer);
    return Status::OK();
  }

  Status Visit(const Decimal256Type& type) {
    auto data = reinterpret_cast<const uint64_t*>(data_->buffers[1]->data());
    ARROW_ASSIGN_OR_RAISE(auto new_buffer, AllocateBuffer(data_->buffers[1]->size()));
    auto new_data = reinterpret_cast<uint64_t*>(new_buffer->mutable_data());
    // NOTE: data_->length not trusted (see warning above)
    const int64_t length = data_->buffers[1]->size() / Decimal256Type::kByteWidth;
    for (int64_t i = 0; i < length; i++) {
      uint64_t tmp0, tmp1, tmp2;
      auto idx = i * 4;
#if ARROW_LITTLE_ENDIAN
      tmp0 = bit_util::FromBigEndian(data[idx]);
      tmp1 = bit_util::FromBigEndian(data[idx + 1]);
      tmp2 = bit_util::FromBigEndian(data[idx + 2]);
      new_data[idx] = bit_util::FromBigEndian(data[idx + 3]);
      new_data[idx + 1] = tmp2;
      new_data[idx + 2] = tmp1;
      new_data[idx + 3] = tmp0;
#else
      tmp0 = bit_util::FromLittleEndian(data[idx]);
      tmp1 = bit_util::FromLittleEndian(data[idx + 1]);
      tmp2 = bit_util::FromLittleEndian(data[idx + 2]);
      new_data[idx] = bit_util::FromLittleEndian(data[idx + 3]);
      new_data[idx + 1] = tmp2;
      new_data[idx + 2] = tmp1;
      new_data[idx + 3] = tmp0;
#endif
    }
    out_->buffers[1] = std::move(new_buffer);
    return Status::OK();
  }

  Status Visit(const DayTimeIntervalType& type) {
    ARROW_ASSIGN_OR_RAISE(out_->buffers[1], ByteSwapBuffer<uint32_t>(data_->buffers[1]));
    return Status::OK();
  }

  Status Visit(const MonthDayNanoIntervalType& type) {
    using MonthDayNanos = MonthDayNanoIntervalType::MonthDayNanos;
    auto data = reinterpret_cast<const MonthDayNanos*>(data_->buffers[1]->data());
    ARROW_ASSIGN_OR_RAISE(auto new_buffer, AllocateBuffer(data_->buffers[1]->size()));
    auto new_data = reinterpret_cast<MonthDayNanos*>(new_buffer->mutable_data());
    // NOTE: data_->length not trusted (see warning above)
    const int64_t length = data_->buffers[1]->size() / sizeof(MonthDayNanos);
    for (int64_t i = 0; i < length; i++) {
      MonthDayNanos tmp = data[i];
#if ARROW_LITTLE_ENDIAN
      tmp.months = bit_util::FromBigEndian(tmp.months);
      tmp.days = bit_util::FromBigEndian(tmp.days);
      tmp.nanoseconds = bit_util::FromBigEndian(tmp.nanoseconds);
#else
      tmp.months = bit_util::FromLittleEndian(tmp.months);
      tmp.days = bit_util::FromLittleEndian(tmp.days);
      tmp.nanoseconds = bit_util::FromLittleEndian(tmp.nanoseconds);
#endif
      new_data[i] = tmp;
    }
    out_->buffers[1] = std::move(new_buffer);
    return Status::OK();
  }

  Status Visit(const NullType& type) { return Status::OK(); }
  Status Visit(const BooleanType& type) { return Status::OK(); }
  Status Visit(const Int8Type& type) { return Status::OK(); }
  Status Visit(const UInt8Type& type) { return Status::OK(); }
  Status Visit(const FixedSizeBinaryType& type) { return Status::OK(); }
  Status Visit(const FixedSizeListType& type) { return Status::OK(); }
  Status Visit(const StructType& type) { return Status::OK(); }
  Status Visit(const RunEndEncodedType& type) {
    return Status::NotImplemented("swapping endianness of run-end encoded array");
  }
  Status Visit(const UnionType& type) {
    out_->buffers[1] = data_->buffers[1];
    if (type.mode() == UnionMode::DENSE) {
      RETURN_NOT_OK(SwapOffsets<int32_t>(2));
    }
    return Status::OK();
  }

  template <typename T>
  enable_if_t<std::is_same<BinaryType, T>::value || std::is_same<StringType, T>::value,
              Status>
  Visit(const T& type) {
    RETURN_NOT_OK(SwapOffsets<int32_t>(1));
    out_->buffers[2] = data_->buffers[2];
    return Status::OK();
  }

  template <typename T>
  enable_if_t<std::is_same<LargeBinaryType, T>::value ||
                  std::is_same<LargeStringType, T>::value,
              Status>
  Visit(const T& type) {
    RETURN_NOT_OK(SwapOffsets<int64_t>(1));
    out_->buffers[2] = data_->buffers[2];
    return Status::OK();
  }

  Status Visit(const BinaryViewType& type) {
    if (type.has_raw_pointers()) {
      return Status::NotImplemented(
          "Swapping endianness of binary / string view with raw pointers");
    }

    auto* s = data_->buffers[1]->data_as<StringHeader>();
    ARROW_ASSIGN_OR_RAISE(auto new_buffer, AllocateBuffer(data_->buffers[1]->size()));
    auto* new_s = new_buffer->mutable_data_as<StringHeader>();

    // NOTE: data_->length not trusted (see warning above)
    const int64_t length = data_->buffers[1]->size() / sizeof(StringHeader);

    for (int64_t i = 0; i < length; i++) {
      auto size = static_cast<uint32_t>(s[i].size());
#if ARROW_LITTLE_ENDIAN
      size = bit_util::FromBigEndian(size);
#else
      size = bit_util::FromLittleEndian(size);
#endif
      if (StringHeader::IsInline(size)) {
        new_s[i] = s[i];
        std::memcpy(static_cast<void*>(&new_s[i]), &size, sizeof(uint32_t));
        continue;
      }

      uint32_t buffer_index = s[i].GetBufferIndex();
      uint32_t offset = s[i].GetBufferOffset();
#if ARROW_LITTLE_ENDIAN
      buffer_index = bit_util::FromBigEndian(buffer_index);
      offset = bit_util::FromBigEndian(offset);
#else
      buffer_index = bit_util::FromLittleEndian(buffer_index);
      offset = bit_util::FromLittleEndian(offset);
#endif
      new_s[i] = StringHeader{size, s[i].GetPrefix(), buffer_index, offset};
    }
    out_->buffers[1] = std::move(new_buffer);
    return Status::OK();
  }

  Status Visit(const ListType& type) {
    RETURN_NOT_OK(SwapOffsets<int32_t>(1));
    return Status::OK();
  }
  Status Visit(const LargeListType& type) {
    RETURN_NOT_OK(SwapOffsets<int64_t>(1));
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    // dictionary was already swapped in ReadDictionary() in ipc/reader.cc
    RETURN_NOT_OK(SwapType(*type.index_type()));
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    RETURN_NOT_OK(SwapType(*type.storage_type()));
    return Status::OK();
  }

  const std::shared_ptr<ArrayData>& data_;
  std::shared_ptr<ArrayData> out_;
};

}  // namespace

namespace internal {

Result<std::shared_ptr<ArrayData>> SwapEndianArrayData(
    const std::shared_ptr<ArrayData>& data) {
  if (data->offset != 0) {
    return Status::Invalid("Unsupported data format: data.offset != 0");
  }
  ArrayDataEndianSwapper swapper(data);
  RETURN_NOT_OK(swapper.SwapType(*data->type));
  return std::move(swapper.out_);
}

}  // namespace internal

std::shared_ptr<Array> MakeArray(const std::shared_ptr<ArrayData>& data) {
  std::shared_ptr<Array> out;
  ArrayDataWrapper wrapper_visitor(data, &out);
  DCHECK_OK(VisitTypeInline(*data->type, &wrapper_visitor));
  DCHECK(out);
  return out;
}

// ----------------------------------------------------------------------
// Misc APIs

namespace {

static Result<std::shared_ptr<Scalar>> MakeScalarForRunEndValue(
    const DataType& run_end_type, int64_t run_end) {
  switch (run_end_type.id()) {
    case Type::INT16:
      if (run_end > std::numeric_limits<int16_t>::max()) {
        return Status::Invalid("Array construction with int16 run end type cannot fit ",
                               run_end);
      }
      return std::make_shared<Int16Scalar>(static_cast<int16_t>(run_end));
    case Type::INT32:
      if (run_end > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("Array construction with int32 run end type cannot fit ",
                               run_end);
      }
      return std::make_shared<Int32Scalar>(static_cast<int32_t>(run_end));
    default:
      break;
  }
  DCHECK_EQ(run_end_type.id(), Type::INT64);
  return std::make_shared<Int64Scalar>(run_end);
}

// get the maximum buffer length required, then allocate a single zeroed buffer
// to use anywhere a buffer is required
class NullArrayFactory {
 public:
  struct GetBufferLength {
    GetBufferLength(const std::shared_ptr<DataType>& type, int64_t length)
        : type_(*type), length_(length), buffer_length_(bit_util::BytesForBits(length)) {}

    Result<int64_t> Finish() && {
      RETURN_NOT_OK(VisitTypeInline(type_, this));
      return buffer_length_;
    }

    template <typename T, typename = decltype(TypeTraits<T>::bytes_required(0))>
    Status Visit(const T&) {
      return MaxOf(TypeTraits<T>::bytes_required(length_));
    }

    template <typename T>
    enable_if_var_size_list<T, Status> Visit(const T&) {
      // values array may be empty, but there must be at least one offset of 0
      return MaxOf(sizeof(typename T::offset_type) * (length_ + 1));
    }

    template <typename T>
    enable_if_base_binary<T, Status> Visit(const T&) {
      // values buffer may be empty, but there must be at least one offset of 0
      return MaxOf(sizeof(typename T::offset_type) * (length_ + 1));
    }

    Status Visit(const BinaryViewType& type) {
      return MaxOf(sizeof(StringHeader) * length_);
    }

    Status Visit(const FixedSizeListType& type) {
      return MaxOf(GetBufferLength(type.value_type(), type.list_size() * length_));
    }

    Status Visit(const FixedSizeBinaryType& type) {
      return MaxOf(type.byte_width() * length_);
    }

    Status Visit(const StructType& type) {
      for (const auto& child : type.fields()) {
        RETURN_NOT_OK(MaxOf(GetBufferLength(child->type(), length_)));
      }
      return Status::OK();
    }

    Status Visit(const SparseUnionType& type) {
      // type codes
      RETURN_NOT_OK(MaxOf(length_));
      // will create children of the same length as the union
      for (const auto& child : type.fields()) {
        RETURN_NOT_OK(MaxOf(GetBufferLength(child->type(), length_)));
      }
      return Status::OK();
    }

    Status Visit(const DenseUnionType& type) {
      // type codes
      RETURN_NOT_OK(MaxOf(length_));
      // offsets
      RETURN_NOT_OK(MaxOf(sizeof(int32_t) * length_));
      // will create children of length 1
      for (const auto& child : type.fields()) {
        RETURN_NOT_OK(MaxOf(GetBufferLength(child->type(), 1)));
      }
      return Status::OK();
    }

    Status Visit(const DictionaryType& type) {
      RETURN_NOT_OK(MaxOf(GetBufferLength(type.value_type(), length_)));
      return MaxOf(GetBufferLength(type.index_type(), length_));
    }

    Status Visit(const RunEndEncodedType& type) {
      // RunEndEncodedType has no buffers, only child arrays
      buffer_length_ = 0;
      return Status::OK();
    }

    Status Visit(const ExtensionType& type) {
      // XXX is an extension array's length always == storage length
      return MaxOf(GetBufferLength(type.storage_type(), length_));
    }

    Status Visit(const DataType& type) {
      return Status::NotImplemented("construction of all-null ", type);
    }

   private:
    Status MaxOf(GetBufferLength&& other) {
      ARROW_ASSIGN_OR_RAISE(int64_t buffer_length, std::move(other).Finish());
      return MaxOf(buffer_length);
    }

    Status MaxOf(int64_t buffer_length) {
      if (buffer_length > buffer_length_) {
        buffer_length_ = buffer_length;
      }
      return Status::OK();
    }

    const DataType& type_;
    int64_t length_, buffer_length_;
  };

  NullArrayFactory(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                   int64_t length)
      : pool_(pool), type_(type), length_(length) {}

  Status CreateBuffer() {
    if (type_->id() == Type::RUN_END_ENCODED) {
      buffer_ = NULLPTR;
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(int64_t buffer_length,
                          GetBufferLength(type_, length_).Finish());
    ARROW_ASSIGN_OR_RAISE(buffer_, AllocateBuffer(buffer_length, pool_));
    std::memset(buffer_->mutable_data(), 0, buffer_->size());
    return Status::OK();
  }

  Result<std::shared_ptr<ArrayData>> Create() {
    if (buffer_ == nullptr) {
      RETURN_NOT_OK(CreateBuffer());
    }
    std::vector<std::shared_ptr<ArrayData>> child_data(type_->num_fields());
    auto buffer_slice =
        buffer_ ? SliceBuffer(buffer_, 0, bit_util::BytesForBits(length_)) : NULLPTR;
    out_ = ArrayData::Make(type_, length_, {std::move(buffer_slice)}, child_data, length_,
                           0);
    RETURN_NOT_OK(VisitTypeInline(*type_, this));
    return out_;
  }

  Status Visit(const NullType&) {
    out_->buffers.resize(1, nullptr);
    return Status::OK();
  }

  Status Visit(const FixedWidthType&) {
    out_->buffers.resize(2, buffer_);
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    out_->buffers.resize(3, buffer_);
    return Status::OK();
  }

  Status Visit(const BinaryViewType&) {
    out_->buffers.resize(2, buffer_);
    return Status::OK();
  }

  template <typename T>
  enable_if_var_size_list<T, Status> Visit(const T& type) {
    out_->buffers.resize(2, buffer_);
    ARROW_ASSIGN_OR_RAISE(out_->child_data[0], CreateChild(type, 0, /*length=*/0));
    return Status::OK();
  }

  Status Visit(const FixedSizeListType& type) {
    ARROW_ASSIGN_OR_RAISE(out_->child_data[0],
                          CreateChild(type, 0, length_ * type.list_size()));
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    for (int i = 0; i < type_->num_fields(); ++i) {
      ARROW_ASSIGN_OR_RAISE(out_->child_data[i], CreateChild(type, i, length_));
    }
    return Status::OK();
  }

  Status Visit(const UnionType& type) {
    out_->buffers.resize(2);

    // First buffer is always null
    out_->buffers[0] = nullptr;

    out_->buffers[1] = buffer_;
    // buffer_ is zeroed, but 0 may not be a valid type code
    if (type.type_codes()[0] != 0) {
      ARROW_ASSIGN_OR_RAISE(out_->buffers[1], AllocateBuffer(length_, pool_));
      std::memset(out_->buffers[1]->mutable_data(), type.type_codes()[0], length_);
    }

    // For sparse unions, we now create children with the same length as the
    // parent
    int64_t child_length = length_;
    if (type.mode() == UnionMode::DENSE) {
      // For dense unions, we set the offsets to all zero and create children
      // with length 1
      out_->buffers.resize(3);
      out_->buffers[2] = buffer_;

      child_length = 1;
    }
    for (int i = 0; i < type_->num_fields(); ++i) {
      ARROW_ASSIGN_OR_RAISE(out_->child_data[i], CreateChild(type, i, child_length));
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    out_->buffers.resize(2, buffer_);
    ARROW_ASSIGN_OR_RAISE(auto typed_null_dict, MakeArrayOfNull(type.value_type(), 0));
    out_->dictionary = typed_null_dict->data();
    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& type) {
    ARROW_ASSIGN_OR_RAISE(auto values, MakeArrayOfNull(type.value_type(), 1, pool_));
    ARROW_ASSIGN_OR_RAISE(auto run_end_scalar,
                          MakeScalarForRunEndValue(*type.run_end_type(), length_));
    ARROW_ASSIGN_OR_RAISE(auto run_ends, MakeArrayFromScalar(*run_end_scalar, 1, pool_));
    ARROW_ASSIGN_OR_RAISE(auto ree_array,
                          RunEndEncodedArray::Make(length_, run_ends, values));
    out_ = ree_array->data();
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    out_->child_data.resize(type.storage_type()->num_fields());
    RETURN_NOT_OK(VisitTypeInline(*type.storage_type(), this));
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("construction of all-null ", type);
  }

  Result<std::shared_ptr<ArrayData>> CreateChild(const DataType& type, int i,
                                                 int64_t length) {
    NullArrayFactory child_factory(pool_, type.field(i)->type(), length);
    child_factory.buffer_ = buffer_;
    return child_factory.Create();
  }

  MemoryPool* pool_;
  const std::shared_ptr<DataType>& type_;
  int64_t length_;
  std::shared_ptr<ArrayData> out_;
  std::shared_ptr<Buffer> buffer_;
};

class RepeatedArrayFactory {
 public:
  RepeatedArrayFactory(MemoryPool* pool, const Scalar& scalar, int64_t length)
      : pool_(pool), scalar_(scalar), length_(length) {}

  Result<std::shared_ptr<Array>> Create() {
    RETURN_NOT_OK(VisitTypeInline(*scalar_.type, this));
    return out_;
  }

  Status Visit(const NullType& type) {
    DCHECK(false);  // already forwarded to MakeArrayOfNull
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBitmap(length_, pool_));
    bit_util::SetBitsTo(buffer->mutable_data(), 0, length_,
                        checked_cast<const BooleanScalar&>(scalar_).value);
    out_ = std::make_shared<BooleanArray>(length_, buffer);
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_number_type<T>::value || is_temporal_type<T>::value, Status> Visit(
      const T&) {
    auto value = checked_cast<const typename TypeTraits<T>::ScalarType&>(scalar_).value;
    return FinishFixedWidth(&value, sizeof(value));
  }

  Status Visit(const FixedSizeBinaryType& type) {
    auto value = checked_cast<const FixedSizeBinaryScalar&>(scalar_).value;
    return FinishFixedWidth(value->data(), type.byte_width());
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    using ScalarType = typename TypeTraits<T>::ScalarType;
    auto value = checked_cast<const ScalarType&>(scalar_).value.ToBytes();
    return FinishFixedWidth(value.data(), value.size());
  }

  Status Visit(const Decimal256Type&) {
    auto value = checked_cast<const Decimal256Scalar&>(scalar_).value.ToBytes();
    return FinishFixedWidth(value.data(), value.size());
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    std::shared_ptr<Buffer> value =
        checked_cast<const typename TypeTraits<T>::ScalarType&>(scalar_).value;
    std::shared_ptr<Buffer> values_buffer, offsets_buffer;
    RETURN_NOT_OK(CreateBufferOf(value->data(), value->size(), &values_buffer));
    auto size = static_cast<typename T::offset_type>(value->size());
    RETURN_NOT_OK(CreateOffsetsBuffer(size, &offsets_buffer));
    out_ = std::make_shared<typename TypeTraits<T>::ArrayType>(
        length_, std::move(offsets_buffer), std::move(values_buffer));
    return Status::OK();
  }

  template <typename T>
  enable_if_binary_view_like<T, Status> Visit(const T&) {
    const std::shared_ptr<Buffer>& value =
        checked_cast<const typename TypeTraits<T>::ScalarType&>(scalar_).value;

    StringHeader header{std::string_view{*value}};
    std::shared_ptr<Buffer> header_buffer;
    RETURN_NOT_OK(CreateBufferOf(&header, sizeof(header), &header_buffer));

    BufferVector char_buffers;
    if (!header.IsInline()) {
      char_buffers.push_back(value);
    }
    out_ = std::make_shared<typename TypeTraits<T>::ArrayType>(
        length_, std::move(header_buffer), std::move(char_buffers));
    return Status::OK();
  }

  template <typename T>
  enable_if_var_size_list<T, Status> Visit(const T& type) {
    using ScalarType = typename TypeTraits<T>::ScalarType;
    using ArrayType = typename TypeTraits<T>::ArrayType;

    auto value = checked_cast<const ScalarType&>(scalar_).value;

    ArrayVector values(length_, value);
    ARROW_ASSIGN_OR_RAISE(auto value_array, Concatenate(values, pool_));

    std::shared_ptr<Buffer> offsets_buffer;
    auto size = static_cast<typename T::offset_type>(value->length());
    RETURN_NOT_OK(CreateOffsetsBuffer(size, &offsets_buffer));

    out_ =
        std::make_shared<ArrayType>(scalar_.type, length_, offsets_buffer, value_array);
    return Status::OK();
  }

  Status Visit(const FixedSizeListType& type) {
    auto value = checked_cast<const FixedSizeListScalar&>(scalar_).value;

    ArrayVector values(length_, value);
    ARROW_ASSIGN_OR_RAISE(auto value_array, Concatenate(values, pool_));

    out_ = std::make_shared<FixedSizeListArray>(scalar_.type, length_, value_array);
    return Status::OK();
  }

  Status Visit(const MapType& type) {
    auto map_scalar = checked_cast<const MapScalar&>(scalar_);
    auto struct_array = checked_cast<const StructArray*>(map_scalar.value.get());

    ArrayVector keys(length_, struct_array->field(0));
    ArrayVector values(length_, struct_array->field(1));

    ARROW_ASSIGN_OR_RAISE(auto key_array, Concatenate(keys, pool_));
    ARROW_ASSIGN_OR_RAISE(auto value_array, Concatenate(values, pool_));

    std::shared_ptr<Buffer> offsets_buffer;
    auto size = static_cast<typename MapType::offset_type>(struct_array->length());
    RETURN_NOT_OK(CreateOffsetsBuffer(size, &offsets_buffer));

    out_ = std::make_shared<MapArray>(scalar_.type, length_, std::move(offsets_buffer),
                                      std::move(key_array), std::move(value_array));
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    const auto& value = checked_cast<const DictionaryScalar&>(scalar_).value;
    ARROW_ASSIGN_OR_RAISE(auto indices,
                          MakeArrayFromScalar(*value.index, length_, pool_));
    out_ = std::make_shared<DictionaryArray>(scalar_.type, std::move(indices),
                                             value.dictionary);
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    ArrayVector fields;
    for (const auto& value : checked_cast<const StructScalar&>(scalar_).value) {
      fields.emplace_back();
      ARROW_ASSIGN_OR_RAISE(fields.back(), MakeArrayFromScalar(*value, length_, pool_));
    }
    out_ = std::make_shared<StructArray>(scalar_.type, length_, std::move(fields));
    return Status::OK();
  }

  Status Visit(const SparseUnionType& type) {
    const auto& union_scalar = checked_cast<const SparseUnionScalar&>(scalar_);
    const auto scalar_type_code = union_scalar.type_code;

    // Create child arrays: most of them are all-null, except for the child array
    // for the given type code (if the scalar is valid).
    ArrayVector fields;
    for (int i = 0; i < type.num_fields(); ++i) {
      fields.emplace_back();
      if (i == union_scalar.child_id && scalar_.is_valid) {
        ARROW_ASSIGN_OR_RAISE(
            fields.back(), MakeArrayFromScalar(*union_scalar.value[i], length_, pool_));
      } else {
        ARROW_ASSIGN_OR_RAISE(fields.back(),
                              MakeArrayOfNull(type.field(i)->type(), length_, pool_));
      }
    }

    ARROW_ASSIGN_OR_RAISE(auto type_codes_buffer, CreateUnionTypeCodes(scalar_type_code));

    out_ = std::make_shared<SparseUnionArray>(scalar_.type, length_, std::move(fields),
                                              std::move(type_codes_buffer));
    return Status::OK();
  }

  Status Visit(const DenseUnionType& type) {
    const auto& union_scalar = checked_cast<const DenseUnionScalar&>(scalar_);
    const auto& union_type = checked_cast<const UnionType&>(*scalar_.type);
    const auto scalar_type_code = union_scalar.type_code;
    const auto scalar_child_id = union_type.child_ids()[scalar_type_code];

    // Create child arrays: all of them are empty, except for the child array
    // for the given type code (if length > 0).
    ArrayVector fields;
    for (int i = 0; i < type.num_fields(); ++i) {
      fields.emplace_back();
      if (i == scalar_child_id && length_ > 0) {
        if (scalar_.is_valid) {
          // One valid element (will be referenced by multiple offsets)
          ARROW_ASSIGN_OR_RAISE(fields.back(),
                                MakeArrayFromScalar(*union_scalar.value, 1, pool_));
        } else {
          // One null element (will be referenced by multiple offsets)
          ARROW_ASSIGN_OR_RAISE(fields.back(),
                                MakeArrayOfNull(union_type.field(i)->type(), 1, pool_));
        }
      } else {
        // Zero element (will not be referenced by any offset)
        ARROW_ASSIGN_OR_RAISE(fields.back(),
                              MakeArrayOfNull(union_type.field(i)->type(), 0, pool_));
      }
    }

    // Create an offsets buffer with all offsets equal to 0
    ARROW_ASSIGN_OR_RAISE(auto offsets_buffer,
                          AllocateBuffer(length_ * sizeof(int32_t), pool_));
    memset(offsets_buffer->mutable_data(), 0, offsets_buffer->size());

    ARROW_ASSIGN_OR_RAISE(auto type_codes_buffer, CreateUnionTypeCodes(scalar_type_code));

    out_ = std::make_shared<DenseUnionArray>(scalar_.type, length_, std::move(fields),
                                             std::move(type_codes_buffer),
                                             std::move(offsets_buffer));
    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& type) {
    const auto& ree_scalar = checked_cast<const RunEndEncodedScalar&>(scalar_);
    ARROW_ASSIGN_OR_RAISE(auto values,
                          ree_scalar.is_valid
                              ? MakeArrayFromScalar(*ree_scalar.value, 1, pool_)
                              : MakeArrayOfNull(ree_scalar.value_type(), 1, pool_));
    ARROW_ASSIGN_OR_RAISE(auto run_end_scalar,
                          MakeScalarForRunEndValue(*ree_scalar.run_end_type(), length_));
    ARROW_ASSIGN_OR_RAISE(auto run_ends, MakeArrayFromScalar(*run_end_scalar, 1, pool_));
    ARROW_ASSIGN_OR_RAISE(out_, RunEndEncodedArray::Make(length_, run_ends, values));
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    return Status::NotImplemented("construction from scalar of type ", *scalar_.type);
  }

  Result<std::shared_ptr<Buffer>> CreateUnionTypeCodes(int8_t type_code) {
    TypedBufferBuilder<int8_t> builder(pool_);
    RETURN_NOT_OK(builder.Resize(length_));
    builder.UnsafeAppend(length_, type_code);
    return builder.Finish();
  }

  template <typename OffsetType>
  Status CreateOffsetsBuffer(OffsetType value_length, std::shared_ptr<Buffer>* out) {
    TypedBufferBuilder<OffsetType> builder(pool_);
    RETURN_NOT_OK(builder.Resize(length_ + 1));
    OffsetType offset = 0;
    for (int64_t i = 0; i < length_ + 1; ++i, offset += value_length) {
      builder.UnsafeAppend(offset);
    }
    return builder.Finish(out);
  }

  Status CreateBufferOf(const void* data, size_t data_length,
                        std::shared_ptr<Buffer>* out) {
    BufferBuilder builder(pool_);
    RETURN_NOT_OK(builder.Resize(length_ * data_length));
    for (int64_t i = 0; i < length_; ++i) {
      builder.UnsafeAppend(data, data_length);
    }
    return builder.Finish(out);
  }

  Status FinishFixedWidth(const void* data, size_t data_length) {
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(CreateBufferOf(data, data_length, &buffer));
    out_ = MakeArray(
        ArrayData::Make(scalar_.type, length_, {nullptr, std::move(buffer)}, 0));
    return Status::OK();
  }

  MemoryPool* pool_;
  const Scalar& scalar_;
  int64_t length_;
  std::shared_ptr<Array> out_;
};

}  // namespace

Result<std::shared_ptr<Array>> MakeArrayOfNull(const std::shared_ptr<DataType>& type,
                                               int64_t length, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto data, NullArrayFactory(pool, type, length).Create());
  return MakeArray(data);
}

Result<std::shared_ptr<Array>> MakeArrayFromScalar(const Scalar& scalar, int64_t length,
                                                   MemoryPool* pool) {
  // Null union scalars still have a type code associated
  if (!scalar.is_valid && !is_union(scalar.type->id())) {
    return MakeArrayOfNull(scalar.type, length, pool);
  }
  return RepeatedArrayFactory(pool, scalar, length).Create();
}

Result<std::shared_ptr<Array>> MakeEmptyArray(std::shared_ptr<DataType> type,
                                              MemoryPool* memory_pool) {
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(memory_pool, type, &builder));
  RETURN_NOT_OK(builder->Resize(0));
  return builder->Finish();
}

namespace internal {

std::vector<ArrayVector> RechunkArraysConsistently(
    const std::vector<ArrayVector>& groups) {
  if (groups.size() <= 1) {
    return groups;
  }
  int64_t total_length = 0;
  for (const auto& array : groups.front()) {
    total_length += array->length();
  }
#ifndef NDEBUG
  for (const auto& group : groups) {
    int64_t group_length = 0;
    for (const auto& array : group) {
      group_length += array->length();
    }
    DCHECK_EQ(group_length, total_length)
        << "Array groups should have the same total number of elements";
  }
#endif
  if (total_length == 0) {
    return groups;
  }

  // Set up result vectors
  std::vector<ArrayVector> rechunked_groups(groups.size());

  // Set up progress counters
  std::vector<ArrayVector::const_iterator> current_arrays;
  std::vector<int64_t> array_offsets;
  for (const auto& group : groups) {
    current_arrays.emplace_back(group.cbegin());
    array_offsets.emplace_back(0);
  }

  // Scan all array vectors at once, rechunking along the way
  int64_t start = 0;
  while (start < total_length) {
    // First compute max possible length for next chunk
    int64_t chunk_length = std::numeric_limits<int64_t>::max();
    for (size_t i = 0; i < groups.size(); i++) {
      auto& arr_it = current_arrays[i];
      auto& offset = array_offsets[i];
      // Skip any done arrays (including 0-length arrays)
      while (offset == (*arr_it)->length()) {
        ++arr_it;
        offset = 0;
      }
      const auto& array = *arr_it;
      DCHECK_GT(array->length(), offset);
      chunk_length = std::min(chunk_length, array->length() - offset);
    }
    DCHECK_GT(chunk_length, 0);

    // Then slice all arrays along this chunk size
    for (size_t i = 0; i < groups.size(); i++) {
      const auto& array = *current_arrays[i];
      auto& offset = array_offsets[i];
      if (offset == 0 && array->length() == chunk_length) {
        // Slice spans entire array
        rechunked_groups[i].emplace_back(array);
      } else {
        DCHECK_LT(chunk_length - offset, array->length());
        rechunked_groups[i].emplace_back(array->Slice(offset, chunk_length));
      }
      offset += chunk_length;
    }
    start += chunk_length;
  }

  return rechunked_groups;
}

namespace {
Status FromRawPointerStringHeaders(const ArraySpan& raw,
                                   util::span<const std::shared_ptr<Buffer>> char_buffers,
                                   StringHeader* io) {
  DCHECK_NE(char_buffers.size(), 0);

  auto IsInBuffer = [](const Buffer& buffer, StringHeader s) {
    return buffer.data_as<char>() <= s.data() &&
           buffer.data_as<char>() + buffer.size() >= s.data() + s.size();
  };

  auto Write = [&](auto find_containing_buffer) {
    // Given `find_containing_buffer` which looks up the index of a buffer containing
    // a StringHeader, write an equivalent buffer of index/offset string views.
    static const Buffer kEmptyBuffer{""};
    const Buffer* buffer_containing_previous_view = &kEmptyBuffer;
    uint32_t buffer_index;

    auto* raw_ptr = raw.GetValues<StringHeader>(1);

    bool all_valid = true;
    VisitNullBitmapInline(
        raw.buffers[0].data, raw.offset, raw.length, raw.null_count,
        [&] {
          // Copied to a local variable, so even if io == raw_ptr
          // we can modify safely.
          auto s = *raw_ptr++;

          if (!s.IsInline()) {
            // Fast path: for most string view arrays, we'll have runs
            // of views into the same buffer.
            if (ARROW_PREDICT_FALSE(!IsInBuffer(*buffer_containing_previous_view, s))) {
              auto found = find_containing_buffer(s);
              if (ARROW_PREDICT_FALSE(!found)) {
                all_valid = false;
                return;
              }
              // Assume that we're at the start of a run of views into
              // char_buffers[buffer_index]; adjust the fast path's pointer accordingly
              buffer_index = *found;
              buffer_containing_previous_view = char_buffers[buffer_index].get();
            }

            s.SetIndexOffset(
                buffer_index,
                static_cast<uint32_t>(
                    s.data() - char_buffers[buffer_index]->template data_as<char>()));
          }
          *io++ = s;
        },
        [&] {
          ++raw_ptr;
          *io++ = {};
        });

    if (!all_valid) {
      return Status::IndexError(
          "A header pointed outside the provided character buffers");
    }
    return Status::OK();
  };

  auto LinearSearch = [&](StringHeader s) -> std::optional<uint32_t> {
    uint32_t buffer_index = 0;
    for (const auto& char_buffer : char_buffers) {
      if (IsInBuffer(*char_buffer, s)) return buffer_index;
      ++buffer_index;
    }
    return {};
  };

  if (char_buffers.size() <= 32) {
    // If there are few buffers to search through, sorting/binary search is not
    // worthwhile. TODO(bkietz) benchmark this and get a less magic number here.
    return Write(LinearSearch);
  }

  auto sort_indices = ArgSort<uint32_t>(
      char_buffers, [](const auto& l, const auto& r) { return l->data() < r->data(); });

  auto first_overlapping = std::adjacent_find(
      sort_indices.begin(), sort_indices.end(), [&](uint32_t before, uint32_t after) {
        return char_buffers[before]->data() + char_buffers[before]->size() <=
               char_buffers[after]->data();
      });
  if (ARROW_PREDICT_FALSE(first_overlapping != sort_indices.end())) {
    // Using a binary search with overlapping buffers would not *uniquely* identify
    // a potentially-containing buffer. Moreover this should be a fairly rare case
    // so optimizing for it seems premature.
    return Write(LinearSearch);
  }

  auto BinarySearch = [&](StringHeader s) -> std::optional<uint32_t> {
    // Find the first buffer whose data starts after the data in view-
    // only buffers *before* this could contain view. Since we've additionally
    // checked that the buffers do not overlap, only the buffer *immediately before*
    // this could contain view.
    auto one_past_potential_super =
        std::upper_bound(sort_indices.begin(), sort_indices.end(), s,
                         [&](const StringHeader& s, int64_t i) {
                           return IsInBuffer(*char_buffers[i], s);
                         });

    if (ARROW_PREDICT_FALSE(one_past_potential_super == sort_indices.begin())) {
      return {};
    }

    uint32_t buffer_index = *(one_past_potential_super - 1);
    const Buffer& char_buffer = *char_buffers[buffer_index];
    if (ARROW_PREDICT_TRUE(IsInBuffer(char_buffer, s))) return buffer_index;

    return {};
  };

  return Write(BinarySearch);
}

Status ToRawPointerStringHeaders(const ArraySpan& io,
                                 util::span<const std::shared_ptr<Buffer>> char_buffers,
                                 StringHeader* raw) {
  DCHECK_NE(char_buffers.size(), 0);

  uint32_t buffer_index = 0;
  const char* buffer_data = char_buffers[0]->data_as<char>();
  auto* io_ptr = io.GetValues<StringHeader>(1);

  bool all_valid = true;
  VisitNullBitmapInline(
      io.buffers[0].data, io.offset, io.length, io.null_count,
      [&] {
        // Copied to a local variable, so even if raw == io_ptr
        // we can modify safely.
        auto s = *io_ptr++;

        if (!s.IsInline()) {
          // Fast path: for most string view arrays, we'll have runs
          // of views into the same buffer.
          if (ARROW_PREDICT_FALSE(s.GetBufferIndex() != buffer_index)) {
            if (ARROW_PREDICT_FALSE(s.GetBufferIndex() >= char_buffers.size())) {
              all_valid = false;
              return;
            }
            // Assume that we're at the start of a run of views into
            // char_buffers[buffer_index]; adjust the fast path's pointer accordingly
            buffer_index = s.GetBufferIndex();
            buffer_data = char_buffers[buffer_index]->data_as<char>();
          }
          s.SetRawPointer(buffer_data + s.GetBufferOffset());
        }
        *raw++ = s;
      },
      [&] {
        ++io_ptr;
        *raw++ = {};
      });

  if (!all_valid) {
    return Status::IndexError("A header pointed outside the provided character buffers");
  }
  return Status::OK();
}
}  // namespace

Status SwapStringHeaderPointers(const ArraySpan& in, StringHeader* out) {
  util::span char_buffers{
      reinterpret_cast<const std::shared_ptr<Buffer>*>(in.buffers[2].data),
      static_cast<size_t>(in.buffers[2].size / sizeof(std::shared_ptr<Buffer>))};

  if (char_buffers.size() == 0) {
    // If there are no character buffers, then all string views must be inline.
    // In this case the buffer does not require swizzling between pointers and
    // index/offsets.
    auto* in_ptr = in.GetValues<StringHeader>(1);

    bool all_inline = true;
    VisitNullBitmapInline(
        in.buffers[0].data, in.offset, in.length, in.null_count,
        [&] {
          all_inline = all_inline && in_ptr->IsInline();
          auto s = *in_ptr++;
          *out++ = s;
        },
        [&] {
          ++in_ptr;
          *out++ = {};
        });
    if (ARROW_PREDICT_FALSE(!all_inline)) {
      return Status::IndexError(
          "A header was not inline when no character buffers were provided");
    }
    return Status::OK();
  }

  return checked_cast<const BinaryViewType*>(in.type)->has_raw_pointers()
             ? FromRawPointerStringHeaders(in, char_buffers, out)
             : ToRawPointerStringHeaders(in, char_buffers, out);
}

void StringHeadersFromStrings(const ArraySpan& strings, StringHeader* s) {
  auto* buffer_data = reinterpret_cast<const char*>(strings.buffers[2].data);
  VisitArraySpanInline<StringType>(
      strings,
      [&](std::string_view v) {
        *s++ = StringHeader{v.data(), static_cast<uint32_t>(v.size()), 0, buffer_data};
      },
      [&] { *s++ = StringHeader{}; });
}

void RawPointerStringHeadersFromStrings(const ArraySpan& strings, StringHeader* s) {
  VisitArraySpanInline<StringType>(
      strings,
      [&](std::string_view v) {
        *s++ = StringHeader{v.data(), static_cast<uint32_t>(v.size())};
      },
      [&] { *s++ = StringHeader{}; });
}

}  // namespace internal
}  // namespace arrow
