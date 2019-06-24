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

#include <algorithm>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace compute {

using internal::checked_cast;
using internal::checked_pointer_cast;

template <typename Builder, typename Scalar>
static Status UnsafeAppend(Builder* builder, Scalar&& value) {
  builder->UnsafeAppend(std::forward<Scalar>(value));
  return Status::OK();
}

static Status UnsafeAppend(BinaryBuilder* builder, util::string_view value) {
  RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
  builder->UnsafeAppend(value);
  return Status::OK();
}

static Status UnsafeAppend(StringBuilder* builder, util::string_view value) {
  RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
  builder->UnsafeAppend(value);
  return Status::OK();
}

template <typename IndexSequence>
class Taker {
 public:
  explicit Taker(const std::shared_ptr<DataType>& type) : type_(type) {}

  virtual ~Taker() = default;

  virtual Status MakeChildren() { return Status::OK(); }

  virtual Status Init(MemoryPool* pool) = 0;

  virtual Status Take(const Array& values, IndexSequence indices) = 0;

  virtual Status Finish(std::shared_ptr<Array>*) = 0;

  static Status Make(const std::shared_ptr<DataType>& type, std::unique_ptr<Taker>* out);

  static_assert(std::is_copy_constructible<IndexSequence>::value,
                "Index sequences must be copy constructible");

  static_assert(
      IndexSequence::take_null_index == std::numeric_limits<int64_t>::min(),
      "Index sequences must declare a taken element as null with index == LONG_MIN");

  static_assert(
      std::is_same<decltype(IndexSequence::never_out_of_bounds), const bool>::value,
      "Index sequences must declare whether bounds checking is necessary");

  static_assert(
      std::is_same<decltype(std::declval<IndexSequence>().Next()), int64_t>::value,
      "An index sequence must yield indices of type int64_t.");

  static_assert(std::is_same<decltype(std::declval<const IndexSequence>().length()),
                             int64_t>::value,
                "An index sequence must provide its length.");

  static_assert(std::is_same<decltype(std::declval<const IndexSequence>().null_count()),
                             int64_t>::value,
                "An index sequence must provide the number of nulls it will take.");

 protected:
  Status BoundsCheck(const Array& values, int64_t index) {
    if (IndexSequence::never_out_of_bounds) {
      return Status::OK();
    }
    if (index < 0 || index >= values.length()) {
      return Status::IndexError("take index out of bounds");
    }
    return Status::OK();
  }

  template <typename Builder>
  Status MakeBuilder(MemoryPool* pool, std::unique_ptr<Builder>* out) {
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(arrow::MakeBuilder(pool, type_, &builder));
    out->reset(checked_cast<Builder*>(builder.release()));
    return Status::OK();
  }

  std::shared_ptr<DataType> type_;
};

class RangeIndexSequence {
 public:
  static constexpr int64_t take_null_index = std::numeric_limits<int64_t>::min();
  static constexpr bool never_out_of_bounds = true;

  RangeIndexSequence(int64_t offset, int64_t length) : index_(offset), length_(length) {}

  int64_t Next() { return index_++; }

  int64_t length() const { return length_; }

  int64_t null_count() const { return 0; }

 private:
  int64_t index_ = 0, length_ = -1;
};

class RangeOrNullIndexSequence {
 public:
  static constexpr int64_t take_null_index = std::numeric_limits<int64_t>::min();
  static constexpr bool never_out_of_bounds = true;

  RangeOrNullIndexSequence(int64_t offset_or_null, int64_t length)
      : index_(offset_or_null), length_(length) {}

  int64_t Next() {
    if (index_ == take_null_index) {
      return take_null_index;
    }
    return index_++;
  }

  int64_t length() const { return length_; }

  int64_t null_count() const { return index_ == take_null_index ? length_ : 0; }

 private:
  int64_t index_ = 0, length_ = -1;
};

template <typename IndexSequence, typename T>
class TakerImpl;

template <typename IndexSequence>
class TakerImpl<IndexSequence, NullType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status Init(MemoryPool*) override { return Status::OK(); }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));

    if (!IndexSequence::never_out_of_bounds) {
      for (int64_t i = 0; i < indices.length(); ++i) {
        int64_t index = indices.Next();
        if (index == IndexSequence::take_null_index) {
          continue;
        }
        RETURN_NOT_OK(this->BoundsCheck(values, index));
      }
    }

    length_ += indices.length();
    return Status::OK();
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    out->reset(new NullArray(length_));
    return Status::OK();
  }

 private:
  int64_t length_ = 0;
};

template <typename IndexSequence, typename T>
class TakerImpl : public Taker<IndexSequence> {
 public:
  using ArrayType = typename TypeTraits<T>::ArrayType;
  using BuilderType = typename TypeTraits<T>::BuilderType;

  using Taker<IndexSequence>::Taker;

  Status Init(MemoryPool* pool) override { return this->MakeBuilder(pool, &builder_); }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));
    RETURN_NOT_OK(builder_->Reserve(indices.length()));

    if (indices.null_count() == 0) {
      if (values.null_count() == 0) {
        return Take<false, false>(values, indices);
      } else {
        return Take<false, true>(values, indices);
      }
    } else {
      if (values.null_count() == 0) {
        return Take<true, false>(values, indices);
      } else {
        return Take<true, true>(values, indices);
      }
    }
  }

  Status Finish(std::shared_ptr<Array>* out) override { return builder_->Finish(out); }

 private:
  template <bool SomeIndicesNull, bool SomeValuesNull>
  Status Take(const Array& values, IndexSequence indices) {
    for (int64_t i = 0; i < indices.length(); ++i) {
      int64_t index = indices.Next();

      if (SomeIndicesNull && index == IndexSequence::take_null_index) {
        builder_->UnsafeAppendNull();
        continue;
      }

      if (SomeValuesNull && values.IsNull(index)) {
        builder_->UnsafeAppendNull();
        continue;
      }

      RETURN_NOT_OK(this->BoundsCheck(values, index));
      auto value = checked_cast<const ArrayType&>(values).GetView(index);
      RETURN_NOT_OK(UnsafeAppend(builder_.get(), value));
    }
    return Status::OK();
  }

  std::unique_ptr<BuilderType> builder_;
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, ListType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status MakeChildren() override {
    const auto& list_type = checked_cast<const ListType&>(*this->type_);
    return Taker<RangeIndexSequence>::Make(list_type.value_type(), &value_taker_);
  }

  Status Init(MemoryPool* pool) override {
    null_bitmap_builder_.reset(new TypedBufferBuilder<bool>(pool));
    offset_builder_.reset(new TypedBufferBuilder<int32_t>(pool));
    return value_taker_->Init(pool);
  }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));

    const auto& list_array = checked_cast<const ListArray&>(values);

    RETURN_NOT_OK(null_bitmap_builder_->Reserve(indices.length()));
    RETURN_NOT_OK(offset_builder_->Reserve(indices.length() + 1));
    int32_t offset = 0;
    offset_builder_->UnsafeAppend(offset);

    for (int64_t i = 0; i < indices.length(); ++i) {
      int64_t index = indices.Next();

      bool is_valid = index != IndexSequence::take_null_index && values.IsValid(index);
      null_bitmap_builder_->UnsafeAppend(is_valid);

      if (is_valid) {
        RETURN_NOT_OK(this->BoundsCheck(values, index));
        offset += list_array.value_length(index);
        RangeIndexSequence value_indices(list_array.value_offset(index),
                                         list_array.value_length(index));
        RETURN_NOT_OK(value_taker_->Take(*list_array.values(), value_indices));
      }
      offset_builder_->UnsafeAppend(offset);
    }
    return Status::OK();
  }

  Status Finish(std::shared_ptr<Array>* out) override { return FinishAs<ListArray>(out); }

 protected:
  template <typename T>
  Status FinishAs(std::shared_ptr<Array>* out) {
    auto null_count = null_bitmap_builder_->false_count();
    auto length = null_bitmap_builder_->length();

    std::shared_ptr<Buffer> offsets, null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder_->Finish(&null_bitmap));
    RETURN_NOT_OK(offset_builder_->Finish(&offsets));

    std::shared_ptr<Array> taken_values;
    RETURN_NOT_OK(value_taker_->Finish(&taken_values));

    out->reset(
        new T(this->type_, length, offsets, taken_values, null_bitmap, null_count));
    return Status::OK();
  }

  std::unique_ptr<TypedBufferBuilder<bool>> null_bitmap_builder_;
  std::unique_ptr<TypedBufferBuilder<int32_t>> offset_builder_;
  std::unique_ptr<Taker<RangeIndexSequence>> value_taker_;
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, MapType> : public TakerImpl<IndexSequence, ListType> {
 public:
  using TakerImpl<IndexSequence, ListType>::TakerImpl;

  Status Finish(std::shared_ptr<Array>* out) override {
    return this->template FinishAs<MapArray>(out);
  }
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, FixedSizeListType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status MakeChildren() override {
    const auto& list_type = checked_cast<const FixedSizeListType&>(*this->type_);
    return Taker<RangeOrNullIndexSequence>::Make(list_type.value_type(), &value_taker_);
  }

  Status Init(MemoryPool* pool) override {
    null_bitmap_builder_.reset(new TypedBufferBuilder<bool>(pool));
    return value_taker_->Init(pool);
  }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));

    const auto& list_array = checked_cast<const FixedSizeListArray&>(values);
    auto list_size = list_array.list_type()->list_size();

    RETURN_NOT_OK(null_bitmap_builder_->Reserve(indices.length()));

    for (int64_t i = 0; i < indices.length(); ++i) {
      int64_t index = indices.Next();

      bool is_valid = index != IndexSequence::take_null_index && values.IsValid(index);
      null_bitmap_builder_->UnsafeAppend(is_valid);

      if (is_valid) {
        RETURN_NOT_OK(this->BoundsCheck(values, index));
      }
      RangeOrNullIndexSequence value_indices(
          is_valid ? list_array.value_offset(index) : IndexSequence::take_null_index,
          list_size);
      RETURN_NOT_OK(value_taker_->Take(*list_array.values(), value_indices));
    }
    return Status::OK();
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    auto null_count = null_bitmap_builder_->false_count();
    auto length = null_bitmap_builder_->length();

    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder_->Finish(&null_bitmap));

    std::shared_ptr<Array> taken_values;
    RETURN_NOT_OK(value_taker_->Finish(&taken_values));

    out->reset(new FixedSizeListArray(this->type_, length, taken_values, null_bitmap,
                                      null_count));
    return Status::OK();
  }

 protected:
  std::unique_ptr<TypedBufferBuilder<bool>> null_bitmap_builder_;
  std::unique_ptr<Taker<RangeOrNullIndexSequence>> value_taker_;
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, StructType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status MakeChildren() override {
    children_.resize(this->type_->num_children());
    for (int i = 0; i < this->type_->num_children(); ++i) {
      RETURN_NOT_OK(
          Taker<IndexSequence>::Make(this->type_->child(i)->type(), &children_[i]));
    }
    return Status::OK();
  }

  Status Init(MemoryPool* pool) override {
    null_bitmap_builder_.reset(new TypedBufferBuilder<bool>(pool));
    for (int i = 0; i < this->type_->num_children(); ++i) {
      RETURN_NOT_OK(children_[i]->Init(pool));
    }
    return Status::OK();
  }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));

    const auto& struct_array = checked_cast<const StructArray&>(values);
    for (int i = 0; i < this->type_->num_children(); ++i) {
      RETURN_NOT_OK(children_[i]->Take(*struct_array.field(i), indices));
    }
    // TODO(bkietz) each child is doing bounds checking; this only needs to happen once

    RETURN_NOT_OK(null_bitmap_builder_->Reserve(indices.length()));
    for (int64_t i = 0; i < indices.length(); ++i) {
      int64_t index = indices.Next();
      null_bitmap_builder_->UnsafeAppend(index != IndexSequence::take_null_index &&
                                         values.IsValid(index));
    }
    return Status::OK();
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    auto null_count = null_bitmap_builder_->false_count();
    auto length = null_bitmap_builder_->length();
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder_->Finish(&null_bitmap));

    ArrayVector fields(this->type_->num_children());
    for (int i = 0; i < this->type_->num_children(); ++i) {
      RETURN_NOT_OK(children_[i]->Finish(&fields[i]));
    }

    out->reset(
        new StructArray(this->type_, length, std::move(fields), null_bitmap, null_count));
    return Status::OK();
  }

 protected:
  std::unique_ptr<TypedBufferBuilder<bool>> null_bitmap_builder_;
  std::vector<std::unique_ptr<Taker<IndexSequence>>> children_;
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, DictionaryType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status MakeChildren() override {
    const auto& dict_type = checked_cast<const DictionaryType&>(*this->type_);
    return Taker<IndexSequence>::Make(dict_type.index_type(), &index_taker_);
  }

  Status Init(MemoryPool* pool) override {
    dictionary_ = nullptr;
    return index_taker_->Init(pool);
  }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));
    const auto& dict_array = checked_cast<const DictionaryArray&>(values);

    if (dictionary_ != nullptr && dictionary_ != dict_array.dictionary()) {
      return Status::NotImplemented(
          "taking from DictionaryArrays with different dictionaries");
    } else {
      dictionary_ = dict_array.dictionary();
    }
    return index_taker_->Take(*dict_array.indices(), indices);
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    std::shared_ptr<Array> taken_indices;
    RETURN_NOT_OK(index_taker_->Finish(&taken_indices));
    out->reset(new DictionaryArray(this->type_, taken_indices, dictionary_));
    return Status::OK();
  }

 protected:
  std::shared_ptr<Array> dictionary_;
  std::unique_ptr<Taker<IndexSequence>> index_taker_;
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, ExtensionType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status MakeChildren() override {
    const auto& ext_type = checked_cast<const ExtensionType&>(*this->type_);
    return Taker<IndexSequence>::Make(ext_type.storage_type(), &storage_taker_);
  }

  Status Init(MemoryPool* pool) override { return storage_taker_->Init(pool); }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));
    const auto& ext_array = checked_cast<const ExtensionArray&>(values);
    return storage_taker_->Take(*ext_array.storage(), indices);
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    std::shared_ptr<Array> taken_storage;
    RETURN_NOT_OK(storage_taker_->Finish(&taken_storage));
    out->reset(new ExtensionArray(this->type_, taken_storage));
    return Status::OK();
  }

 protected:
  std::unique_ptr<Taker<IndexSequence>> storage_taker_;
};

template <typename IndexSequence>
struct TakerMakeImpl {
  Status Visit(const NullType&) { return Make<NullType>(); }

  template <typename Fixed>
  typename std::enable_if<std::is_base_of<FixedWidthType, Fixed>::value, Status>::type
  Visit(const Fixed&) {
    return Make<Fixed>();
  }

  Status Visit(const BinaryType&) { return Make<BinaryType>(); }

  Status Visit(const StringType&) { return Make<StringType>(); }

  Status Visit(const ListType&) { return Make<ListType>(); }

  Status Visit(const MapType&) { return Make<MapType>(); }

  Status Visit(const FixedSizeListType&) { return Make<FixedSizeListType>(); }

  Status Visit(const StructType& t) { return Make<StructType>(); }

  Status Visit(const DictionaryType& t) { return Make<DictionaryType>(); }

  Status Visit(const ExtensionType& t) { return Make<ExtensionType>(); }

  Status Visit(const DataType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  template <typename T>
  Status Make() {
    out_->reset(new TakerImpl<IndexSequence, T>(type_));
    return (*out_)->MakeChildren();
  }

  std::shared_ptr<DataType> type_;
  std::unique_ptr<Taker<IndexSequence>>* out_;
};

template <typename IndexSequence>
Status Taker<IndexSequence>::Make(const std::shared_ptr<DataType>& type,
                                  std::unique_ptr<Taker>* out) {
  TakerMakeImpl<IndexSequence> visitor{type, out};
  return VisitTypeInline(*type, &visitor);
}

}  // namespace compute
}  // namespace arrow
