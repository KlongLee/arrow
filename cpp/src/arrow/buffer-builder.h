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

#ifndef ARROW_BUFFER_BUILDER_H
#define ARROW_BUFFER_BUILDER_H

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

// ----------------------------------------------------------------------
// Buffer builder classes

/// \class BufferBuilder
/// \brief A class for incrementally building a contiguous chunk of in-memory
/// data
class ARROW_EXPORT BufferBuilder {
 public:
  explicit BufferBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : pool_(pool), data_(NULLPTR), capacity_(0), size_(0) {}

  /// \brief Resize the buffer to the nearest multiple of 64 bytes
  ///
  /// \param new_capacity the new capacity of the of the builder. Will be
  /// rounded up to a multiple of 64 bytes for padding \param shrink_to_fit if
  /// new capacity is smaller than the existing size, reallocate internal
  /// buffer. Set to false to avoid reallocations when shrinking the builder.
  /// \return Status
  Status Resize(const int64_t new_capacity, bool shrink_to_fit = true) {
    // Resize(0) is a no-op
    if (new_capacity == 0) {
      return Status::OK();
    }
    int64_t old_capacity = capacity_;

    if (buffer_ == NULLPTR) {
      ARROW_RETURN_NOT_OK(AllocateResizableBuffer(pool_, new_capacity, &buffer_));
    } else {
      ARROW_RETURN_NOT_OK(buffer_->Resize(new_capacity, shrink_to_fit));
    }
    capacity_ = buffer_->capacity();
    data_ = buffer_->mutable_data();
    if (capacity_ > old_capacity) {
      memset(data_ + old_capacity, 0, capacity_ - old_capacity);
    }
    return Status::OK();
  }

  /// \brief Ensure that builder can accommodate the additional number of bytes
  /// without the need to perform allocations
  ///
  /// \param[in] additional_bytes number of additional bytes to make space for
  /// \param[in] grow_by_factor if true, round up allocations using the
  /// strategy in BufferBuilder::GrowByFactor
  /// \return Status
  Status Reserve(const int64_t additional_bytes, bool grow_by_factor = false) {
    auto min_capacity = size_ + additional_bytes;
    if (min_capacity <= capacity_) return Status::OK();
    if (grow_by_factor) {
      min_capacity = GrowByFactor(min_capacity);
    }
    return Resize(min_capacity, false);
  }

  /// \brief Return a capacity expanded by a growth factor of 2
  static int64_t GrowByFactor(const int64_t min_capacity) {
    // If the capacity was not already a multiple of 2, do so here
    // TODO(emkornfield) doubling isn't great default allocation practice
    // see https://github.com/facebook/folly/blob/master/folly/docs/FBVector.md
    // for discussion
    return BitUtil::NextPower2(min_capacity);
  }

  /// \brief Append the given data to the buffer
  ///
  /// The buffer is automatically expanded if necessary.
  Status Append(const void* data, const int64_t length) {
    if (ARROW_PREDICT_FALSE(size_ + length > capacity_)) {
      ARROW_RETURN_NOT_OK(Resize(GrowByFactor(size_ + length), false));
    }
    UnsafeAppend(data, length);
    return Status::OK();
  }

  /// \brief Append copies of a value to the buffer
  ///
  /// The buffer is automatically expanded if necessary.
  Status Append(const int64_t num_copies, uint8_t value) {
    ARROW_RETURN_NOT_OK(Reserve(num_copies, true));
    UnsafeAppend(num_copies, value);
    return Status::OK();
  }

  /// \brief Append the given data to the buffer
  ///
  /// The buffer is automatically expanded if necessary.
  template <size_t NBYTES>
  Status Append(const std::array<uint8_t, NBYTES>& data) {
    constexpr auto nbytes = static_cast<int64_t>(NBYTES);
    ARROW_RETURN_NOT_OK(Reserve(NBYTES, true));
    std::copy(data.cbegin(), data.cend(), data_ + size_);
    size_ += nbytes;
    return Status::OK();
  }

  // Advance pointer and zero out memory
  Status Advance(const int64_t length) { return Append(length, 0); }

  // Unsafe methods don't check existing size
  void UnsafeAppend(const void* data, const int64_t length) {
    memcpy(data_ + size_, data, static_cast<size_t>(length));
    size_ += length;
  }

  void UnsafeAppend(const int64_t num_copies, uint8_t value) {
    memset(data_ + size_, value, static_cast<size_t>(num_copies));
    size_ += num_copies;
  }

  /// \brief Return result of builder as a Buffer object.
  ///
  /// The builder is reset and can be reused afterwards.
  ///
  /// \param[out] out the finalized Buffer object
  /// \param shrink_to_fit if the buffer size is smaller than its capacity,
  /// reallocate to fit more tightly in memory. Set to false to avoid
  /// a reallocation, at the expense of potentially more memory consumption.
  /// \return Status
  Status Finish(std::shared_ptr<Buffer>* out, bool shrink_to_fit = true) {
    ARROW_RETURN_NOT_OK(Resize(size_, shrink_to_fit));
    *out = buffer_;
    Reset();
    return Status::OK();
  }

  void Reset() {
    buffer_ = NULLPTR;
    capacity_ = size_ = 0;
  }

  int64_t capacity() const { return capacity_; }
  int64_t length() const { return size_; }
  const uint8_t* data() const { return data_; }
  uint8_t* mutable_data() { return data_; }

 private:
  std::shared_ptr<ResizableBuffer> buffer_;
  MemoryPool* pool_;
  uint8_t* data_;
  int64_t capacity_;
  int64_t size_;
};

template <typename T, typename Enable = void>
class TypedBufferBuilder;

/// \brief A BufferBuilder for building a buffer of arithmetic elements
template <typename T>
class TypedBufferBuilder<T, typename std::enable_if<std::is_arithmetic<T>::value>::type> {
 public:
  explicit TypedBufferBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : bytes_builder_(pool) {}

  Status Append(T value) {
    return bytes_builder_.Append(reinterpret_cast<uint8_t*>(&value), sizeof(T));
  }

  Status Append(const T* values, int64_t num_elements) {
    return bytes_builder_.Append(reinterpret_cast<const uint8_t*>(values),
                                 num_elements * sizeof(T));
  }

  Status Append(const int64_t num_copies, T value) {
    ARROW_RETURN_NOT_OK(
        Resize(BufferBuilder::GrowByFactor(num_copies + length()), false));
    UnsafeAppend(num_copies, value);
    return Status::OK();
  }

  void UnsafeAppend(T value) {
    bytes_builder_.UnsafeAppend(reinterpret_cast<uint8_t*>(&value), sizeof(T));
  }

  void UnsafeAppend(const T* values, int64_t num_elements) {
    bytes_builder_.UnsafeAppend(reinterpret_cast<const uint8_t*>(values),
                                num_elements * sizeof(T));
  }

  void UnsafeAppend(const int64_t num_copies, T value) {
    auto data = mutable_data() + length();
    bytes_builder_.UnsafeAppend(num_copies * sizeof(T), 0);
    for (const auto end = data + num_copies; data != end; ++data) {
      *data = value;
    }
  }

  Status Resize(const int64_t new_capacity, bool shrink_to_fit = true) {
    return bytes_builder_.Resize(new_capacity * sizeof(T), shrink_to_fit);
  }

  Status Reserve(const int64_t additional_elements) {
    return bytes_builder_.Reserve(additional_elements * sizeof(T));
  }

  Status Advance(const int64_t length) {
    return bytes_builder_.Advance(length * sizeof(T));
  }

  Status Finish(std::shared_ptr<Buffer>* out, bool shrink_to_fit = true) {
    return bytes_builder_.Finish(out, shrink_to_fit);
  }

  void Reset() { bytes_builder_.Reset(); }

  int64_t length() const { return bytes_builder_.length() / sizeof(T); }
  int64_t capacity() const { return bytes_builder_.capacity() / sizeof(T); }
  const T* data() const { return reinterpret_cast<const T*>(bytes_builder_.data()); }
  T* mutable_data() { return reinterpret_cast<T*>(bytes_builder_.mutable_data()); }

 private:
  BufferBuilder bytes_builder_;
};

/// \brief A BufferBuilder for building a buffer containing a bitmap
template <>
class TypedBufferBuilder<bool> {
 public:
  explicit TypedBufferBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : bytes_builder_(pool) {}

  Status Append(bool value) {
    ARROW_RETURN_NOT_OK(ResizeWithGrowthFactor(bit_length_ + 1));
    UnsafeAppend(value);
    return Status::OK();
  }

  Status Append(const uint8_t* valid_bytes, int64_t num_elements) {
    ARROW_RETURN_NOT_OK(ResizeWithGrowthFactor(bit_length_ + num_elements));
    UnsafeAppend(valid_bytes, num_elements);
    return Status::OK();
  }

  Status Append(const int64_t num_copies, bool value) {
    ARROW_RETURN_NOT_OK(ResizeWithGrowthFactor(bit_length_ + num_copies));
    UnsafeAppend(num_copies, value);
    return Status::OK();
  }

  void UnsafeAppend(bool value) {
    BitUtil::SetBitTo(mutable_data(), bit_length_, value);
    if (!value) {
      ++false_count_;
    }
    ++bit_length_;
  }

  void UnsafeAppend(const uint8_t* bytes, int64_t num_elements) {
    if (num_elements == 0) return;
    int64_t i = 0;
    internal::GenerateBitsUnrolled(mutable_data(), bit_length_, num_elements, [&] {
      bool value = bytes[i++];
      if (!value) ++false_count_;
      return value;
    });
    bit_length_ += num_elements;
  }

  void UnsafeAppend(const int64_t num_copies, bool value) {
    BitUtil::SetBitsTo(mutable_data(), bit_length_, num_copies, value);
    if (!value) {
      false_count_ += num_copies;
    }
    bit_length_ += num_copies;
  }

  Status Resize(const int64_t new_capacity, bool shrink_to_fit = true) {
    const int64_t old_byte_capacity = bytes_builder_.capacity();
    const int64_t new_byte_capacity = BitUtil::BytesForBits(new_capacity);
    ARROW_RETURN_NOT_OK(bytes_builder_.Resize(new_byte_capacity, shrink_to_fit));
    if (new_byte_capacity > old_byte_capacity) {
      memset(mutable_data() + old_byte_capacity, 0,
             static_cast<size_t>(new_byte_capacity - old_byte_capacity));
    }
    return Status::OK();
  }

  Status Reserve(const int64_t additional_elements) {
    return Resize(bit_length_ + additional_elements, false);
  }

  Status Advance(const int64_t length) {
    bit_length_ += length;
    false_count_ += length;
    return ResizeWithGrowthFactor(bit_length_);
  }

  Status Finish(std::shared_ptr<Buffer>* out, bool shrink_to_fit = true) {
    bit_length_ = false_count_ = 0;
    return bytes_builder_.Finish(out, shrink_to_fit);
  }

  void Reset() {
    bytes_builder_.Reset();
    bit_length_ = false_count_ = 0;
  }

  int64_t length() const { return bit_length_; }
  int64_t capacity() const { return bytes_builder_.capacity() * 8; }
  const uint8_t* data() const { return bytes_builder_.data(); }
  uint8_t* mutable_data() { return bytes_builder_.mutable_data(); }
  int64_t false_count() const { return false_count_; }

 private:
  Status ResizeWithGrowthFactor(const int64_t min_capacity) {
    return Resize(BufferBuilder::GrowByFactor(min_capacity), false);
  }
  BufferBuilder bytes_builder_;
  int64_t bit_length_ = 0;
  int64_t false_count_ = 0;
};

}  // namespace arrow

#endif  // ARROW_BUFFER_BUILDER_H
