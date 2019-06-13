// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// returnGegarding copyright ownership.  The ASF licenses this file
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

#include <algorithm>
#include <memory>
#include <utility>

#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernels/filter.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/concatenate.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace compute {

using internal::checked_cast;
using internal::checked_pointer_cast;

/// \brief BinaryKernel implementing Filter operation
class ARROW_EXPORT FilterKernel : public BinaryKernel {
 public:
  explicit FilterKernel(const std::shared_ptr<DataType>& type) : type_(type) {}

  Status Call(FunctionContext* ctx, const Datum& values, const Datum& filter,
              Datum* out) override;

  std::shared_ptr<DataType> out_type() const override { return type_; }

  virtual Status Filter(FunctionContext* ctx, const Array& values,
                        const BooleanArray& filter, std::shared_ptr<Array>* out) = 0;

  static Status Make(const std::shared_ptr<DataType>& value_type,
                     std::unique_ptr<FilterKernel>* out);

 protected:
  std::shared_ptr<DataType> type_;
};

template <typename Builder>
Status MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                   std::unique_ptr<Builder>* out) {
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(pool, type, &builder));
  out->reset(checked_cast<Builder*>(builder.release()));
  return Status::OK();
}

struct FilterParameters {
  std::shared_ptr<DataType> value_type;
  std::unique_ptr<FilterKernel>* out;
};

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

// TODO(bkietz) this can be optimized
static int64_t OutputSize(const BooleanArray& filter) {
  auto offset = filter.offset();
  auto length = filter.length();
  internal::BitmapReader filter_data(filter.data()->buffers[1]->data(), offset, length);
  int64_t size = 0;
  for (auto i = offset; i < offset + length; ++i) {
    if (filter.IsNull(i) || filter_data.IsSet()) {
      ++size;
    }
    filter_data.Next();
  }
  return size;
}

template <typename ValueType>
class FilterImpl;

template <>
class FilterImpl<NullType> : public FilterKernel {
 public:
  using FilterKernel::FilterKernel;

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                std::shared_ptr<Array>* out) override {
    out->reset(new NullArray(OutputSize(checked_cast<const BooleanArray&>(filter))));
    return Status::OK();
  }
};

template <typename ValueType>
class FilterImpl : public FilterKernel {
 public:
  using ValueArray = typename TypeTraits<ValueType>::ArrayType;
  using OutBuilder = typename TypeTraits<ValueType>::BuilderType;

  using FilterKernel::FilterKernel;

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                std::shared_ptr<Array>* out) override {
    std::unique_ptr<OutBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), type_, &builder));
    RETURN_NOT_OK(builder->Resize(OutputSize(filter)));
    RETURN_NOT_OK(UnpackValuesNullCount(checked_cast<const ValueArray&>(values), filter,
                                        builder.get()));
    return builder->Finish(out);
  }

 private:
  Status UnpackValuesNullCount(const ValueArray& values, const BooleanArray& filter,
                               OutBuilder* builder) {
    if (values.null_count() == 0) {
      return UnpackIndicesNullCount<true>(values, filter, builder);
    }
    return UnpackIndicesNullCount<false>(values, filter, builder);
  }

  template <bool AllValuesValid>
  Status UnpackIndicesNullCount(const ValueArray& values, const BooleanArray& filter,
                                OutBuilder* builder) {
    if (filter.null_count() == 0) {
      return Filter<AllValuesValid, true>(values, filter, builder);
    }
    return Filter<AllValuesValid, false>(values, filter, builder);
  }

  template <bool AllValuesValid, bool AllIndicesValid>
  Status Filter(const ValueArray& values, const BooleanArray& filter,
                OutBuilder* builder) {
    for (int64_t i = 0; i < filter.length(); ++i) {
      if (!AllIndicesValid && filter.IsNull(i)) {
        builder->UnsafeAppendNull();
        continue;
      }
      if (!filter.Value(i)) {
        continue;
      }
      if (!AllValuesValid && values.IsNull(i)) {
        builder->UnsafeAppendNull();
        continue;
      }
      RETURN_NOT_OK(UnsafeAppend(builder, values.GetView(i)));
    }
    return Status::OK();
  }
};

template <>
class FilterImpl<StructType> : public FilterKernel {
 public:
  using FilterKernel::FilterKernel;

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                std::shared_ptr<Array>* out) override {
    const auto& struct_array = checked_cast<const StructArray&>(values);

    auto length = OutputSize(filter);
    TypedBufferBuilder<bool> null_bitmap_builder(ctx->memory_pool());
    RETURN_NOT_OK(null_bitmap_builder.Resize(length));

    ArrayVector fields(type_->num_children());
    for (int i = 0; i < type_->num_children(); ++i) {
      RETURN_NOT_OK(
          arrow::compute::Filter(ctx, *struct_array.field(i), filter, &fields[i]));
    }

    for (int64_t i = 0; i < filter.length(); ++i) {
      if (filter.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        continue;
      }
      if (!filter.Value(i)) {
        continue;
      }
      if (struct_array.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        continue;
      }
      null_bitmap_builder.UnsafeAppend(true);
    }

    auto null_count = null_bitmap_builder.false_count();
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder.Finish(&null_bitmap));

    out->reset(new StructArray(type_, length, fields, null_bitmap, null_count));
    return Status::OK();
  }
};

template <>
class FilterImpl<FixedSizeListType> : public FilterKernel {
 public:
  using FilterKernel::FilterKernel;

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                std::shared_ptr<Array>* out) override {
    const auto& list_array = checked_cast<const FixedSizeListArray&>(values);

    TypedBufferBuilder<bool> null_bitmap_builder(ctx->memory_pool());
    auto length = OutputSize(filter);
    RETURN_NOT_OK(null_bitmap_builder.Resize(length));

    std::shared_ptr<Array> null_slice;
    RETURN_NOT_OK(MakeArrayOfNull(list_array.value_type(),
                                  list_array.list_type()->list_size(), &null_slice));
    ArrayVector value_slices(length, null_slice);

    for (int64_t filtered_i = 0, i = 0; i < filter.length(); ++i) {
      if (filter.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        ++filtered_i;
        continue;
      }
      if (!filter.Value(i)) {
        continue;
      }
      if (values.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        ++filtered_i;
        continue;
      }
      null_bitmap_builder.UnsafeAppend(true);
      value_slices[filtered_i] = list_array.value_slice(i);
      ++filtered_i;
    }

    std::shared_ptr<Array> out_values;
    if (length != 0) {
      RETURN_NOT_OK(Concatenate(value_slices, ctx->memory_pool(), &out_values));
    } else {
      out_values = list_array.values()->Slice(0, 0);
    }

    auto null_count = null_bitmap_builder.false_count();
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder.Finish(&null_bitmap));

    out->reset(
        new FixedSizeListArray(type_, length, out_values, null_bitmap, null_count));
    return Status::OK();
  }
};

class ListFilterImpl : public FilterKernel {
 public:
  using FilterKernel::FilterKernel;

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                std::shared_ptr<Array>* out) override {
    const auto& list_array = checked_cast<const ListArray&>(values);

    TypedBufferBuilder<bool> null_bitmap_builder(ctx->memory_pool());
    auto length = OutputSize(filter);
    RETURN_NOT_OK(null_bitmap_builder.Resize(length));

    TypedBufferBuilder<int32_t> offset_builder(ctx->memory_pool());
    RETURN_NOT_OK(offset_builder.Resize(length + 1));
    int32_t offset = 0;
    offset_builder.UnsafeAppend(offset);

    ArrayVector value_slices(length, list_array.values()->Slice(0, 0));
    for (int64_t filtered_i = 0, i = 0; i < filter.length(); ++i) {
      if (filter.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        offset_builder.UnsafeAppend(offset);
        ++filtered_i;
        continue;
      }
      if (!filter.Value(i)) {
        continue;
      }
      if (values.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        offset_builder.UnsafeAppend(offset);
        ++filtered_i;
        continue;
      }
      null_bitmap_builder.UnsafeAppend(true);
      value_slices[filtered_i] = list_array.value_slice(i);
      ++filtered_i;
      offset += list_array.value_length(i);
      offset_builder.UnsafeAppend(offset);
    }

    std::shared_ptr<Array> out_values;
    if (length != 0) {
      RETURN_NOT_OK(Concatenate(value_slices, ctx->memory_pool(), &out_values));
    } else {
      out_values = list_array.values()->Slice(0, 0);
    }

    auto null_count = null_bitmap_builder.false_count();
    std::shared_ptr<Buffer> offsets, null_bitmap;
    RETURN_NOT_OK(offset_builder.Finish(&offsets));
    RETURN_NOT_OK(null_bitmap_builder.Finish(&null_bitmap));

    *out = MakeArray(ArrayData::Make(type_, length, {null_bitmap, offsets},
                                     {out_values->data()}, null_count));
    return Status::OK();
  }
};

class DictionaryFilterImpl : public FilterKernel {
 public:
  DictionaryFilterImpl(const std::shared_ptr<DataType>& type,
                       std::unique_ptr<FilterKernel> impl)
      : FilterKernel(type), impl_(std::move(impl)) {}

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                std::shared_ptr<Array>* out) override {
    auto dict_array = checked_cast<const DictionaryArray*>(&values);
    // To filter a dictionary, apply the current kernel to the dictionary's indices.
    std::shared_ptr<Array> taken_indices;
    RETURN_NOT_OK(impl_->Filter(ctx, *dict_array->indices(), filter, &taken_indices));
    return DictionaryArray::FromArrays(values.type(), taken_indices,
                                       dict_array->dictionary(), out);
  }

 private:
  std::unique_ptr<FilterKernel> impl_;
};

class ExtensionFilterImpl : public FilterKernel {
 public:
  ExtensionFilterImpl(const std::shared_ptr<DataType>& type,
                      std::unique_ptr<FilterKernel> impl)
      : FilterKernel(type), impl_(std::move(impl)) {}

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                std::shared_ptr<Array>* out) override {
    auto ext_array = checked_cast<const ExtensionArray*>(&values);
    // To take from an extension array, apply the current kernel to storage.
    std::shared_ptr<Array> taken_storage;
    RETURN_NOT_OK(impl_->Filter(ctx, *ext_array->storage(), filter, &taken_storage));
    *out = ext_array->extension_type()->MakeArray(taken_storage->data());
    return Status::OK();
  }

 private:
  std::unique_ptr<FilterKernel> impl_;
};

template <typename T>
using ArrayType = typename TypeTraits<T>::ArrayType;

struct UnpackValues {
  template <typename ValueType>
  Status Visit(const ValueType&) {
    return Make<FilterImpl<ValueType>>();
  }

  Status Visit(const NullType&) { return Make<FilterImpl<NullType>>(); }

  Status Visit(const DictionaryType& t) {
    std::unique_ptr<FilterKernel> indices_filter_impl;
    FilterParameters params = params_;
    params.value_type = t.index_type();
    params.out = &indices_filter_impl;
    UnpackValues unpack = {params};
    RETURN_NOT_OK(VisitTypeInline(*t.index_type(), &unpack));
    return Make<DictionaryFilterImpl>(std::move(indices_filter_impl));
  }

  Status Visit(const ExtensionType& t) {
    std::unique_ptr<FilterKernel> storage_filter_impl;
    FilterParameters params = params_;
    params.value_type = t.storage_type();
    params.out = &storage_filter_impl;
    UnpackValues unpack = {params};
    RETURN_NOT_OK(VisitTypeInline(*t.storage_type(), &unpack));
    return Make<ExtensionFilterImpl>(std::move(storage_filter_impl));
  }

  Status Visit(const UnionType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const ListType& t) { return Make<ListFilterImpl>(); }

  Status Visit(const FixedSizeListType& t) {
    return Make<FilterImpl<FixedSizeListType>>();
  }

  Status Visit(const StructType& t) { return Make<FilterImpl<StructType>>(); }

  template <typename Impl, typename... Extra>
  Status Make(Extra&&... extra) {
    *params_.out =
        internal::make_unique<Impl>(params_.value_type, std::forward<Extra>(extra)...);
    return Status::OK();
  }

  const FilterParameters& params_;
};

Status FilterKernel::Call(FunctionContext* ctx, const Datum& values, const Datum& filter,
                          Datum* out) {
  if (!values.is_array() || !filter.is_array()) {
    return Status::Invalid("FilterKernel expects array values and filter");
  }
  std::shared_ptr<Array> out_array;
  auto filter_array = checked_pointer_cast<BooleanArray>(filter.make_array());
  auto values_array = values.make_array();
  RETURN_NOT_OK(this->Filter(ctx, *values_array, *filter_array, &out_array));
  *out = out_array;
  return Status::OK();
}

Status FilterKernel::Make(const std::shared_ptr<DataType>& value_type,
                          std::unique_ptr<FilterKernel>* out) {
  FilterParameters params;
  params.value_type = value_type;
  params.out = out;
  UnpackValues unpack = {params};
  RETURN_NOT_OK(VisitTypeInline(*value_type, &unpack));
  return Status::OK();
}

Status Filter(FunctionContext* context, const Array& values, const Array& filter,
              std::shared_ptr<Array>* out) {
  Datum out_datum;
  RETURN_NOT_OK(Filter(context, Datum(values.data()), Datum(filter.data()), &out_datum));
  *out = out_datum.make_array();
  return Status::OK();
}

Status Filter(FunctionContext* context, const Datum& values, const Datum& filter,
              Datum* out) {
  std::unique_ptr<FilterKernel> kernel;
  RETURN_NOT_OK(FilterKernel::Make(values.type(), &kernel));
  RETURN_NOT_OK(kernel->Call(context, values, filter, out));
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
