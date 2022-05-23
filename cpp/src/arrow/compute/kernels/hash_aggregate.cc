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

#include <cmath>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer_builder.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec/key_hash.h"
#include "arrow/compute/exec/key_map.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/aggregate_var_std_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/row_encoder.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/record_batch.h"
#include "arrow/stl_allocator.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/int128_internal.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/task_group.h"
#include "arrow/util/tdigest.h"
#include "arrow/util/thread_pool.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::FirstTimeBitmapWriter;

namespace compute {
namespace internal {
namespace {

/// C++ abstract base class for the HashAggregateKernel interface.
/// Implementations should be default constructible and perform initialization in
/// Init().
struct GroupedAggregator : KernelState {
  virtual Status Init(ExecContext*, const std::vector<ValueDescr>& inputs,
                      const FunctionOptions*) = 0;

  virtual Status Resize(int64_t new_num_groups) = 0;

  virtual Status Consume(const ExecBatch& batch) = 0;

  virtual Status Merge(GroupedAggregator&& other, const ArrayData& group_id_mapping) = 0;

  virtual Result<Datum> Finalize() = 0;

  virtual std::shared_ptr<DataType> out_type() const = 0;
};

template <typename Impl>
Result<std::unique_ptr<KernelState>> HashAggregateInit(KernelContext* ctx,
                                                       const KernelInitArgs& args) {
  auto impl = ::arrow::internal::make_unique<Impl>();
  RETURN_NOT_OK(impl->Init(ctx->exec_context(), args.inputs, args.options));
  return std::move(impl);
}

Status HashAggregateResize(KernelContext* ctx, int64_t num_groups) {
  return checked_cast<GroupedAggregator*>(ctx->state())->Resize(num_groups);
}
Status HashAggregateConsume(KernelContext* ctx, const ExecBatch& batch) {
  return checked_cast<GroupedAggregator*>(ctx->state())->Consume(batch);
}
Status HashAggregateMerge(KernelContext* ctx, KernelState&& other,
                          const ArrayData& group_id_mapping) {
  return checked_cast<GroupedAggregator*>(ctx->state())
      ->Merge(checked_cast<GroupedAggregator&&>(other), group_id_mapping);
}
Status HashAggregateFinalize(KernelContext* ctx, Datum* out) {
  return checked_cast<GroupedAggregator*>(ctx->state())->Finalize().Value(out);
}

HashAggregateKernel MakeKernel(InputType argument_type, KernelInit init) {
  HashAggregateKernel kernel;
  kernel.init = std::move(init);
  kernel.signature = KernelSignature::Make(
      {std::move(argument_type), InputType::Array(Type::UINT32)},
      OutputType(
          [](KernelContext* ctx, const std::vector<ValueDescr>&) -> Result<ValueDescr> {
            return checked_cast<GroupedAggregator*>(ctx->state())->out_type();
          }));
  kernel.resize = HashAggregateResize;
  kernel.consume = HashAggregateConsume;
  kernel.merge = HashAggregateMerge;
  kernel.finalize = HashAggregateFinalize;
  return kernel;
}

Status AddHashAggKernels(
    const std::vector<std::shared_ptr<DataType>>& types,
    Result<HashAggregateKernel> make_kernel(const std::shared_ptr<DataType>&),
    HashAggregateFunction* function) {
  for (const auto& ty : types) {
    ARROW_ASSIGN_OR_RAISE(auto kernel, make_kernel(ty));
    RETURN_NOT_OK(function->AddKernel(std::move(kernel)));
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Helpers for more easily implementing hash aggregates

template <typename T>
struct GroupedValueTraits {
  using CType = typename TypeTraits<T>::CType;

  static CType Get(const CType* values, uint32_t g) { return values[g]; }
  static void Set(CType* values, uint32_t g, CType v) { values[g] = v; }
  static Status AppendBuffers(TypedBufferBuilder<CType>* destination,
                              const uint8_t* values, int64_t offset, int64_t num_values) {
    RETURN_NOT_OK(
        destination->Append(reinterpret_cast<const CType*>(values) + offset, num_values));
    return Status::OK();
  }
};
template <>
struct GroupedValueTraits<BooleanType> {
  static bool Get(const uint8_t* values, uint32_t g) {
    return bit_util::GetBit(values, g);
  }
  static void Set(uint8_t* values, uint32_t g, bool v) {
    bit_util::SetBitTo(values, g, v);
  }
  static Status AppendBuffers(TypedBufferBuilder<bool>* destination,
                              const uint8_t* values, int64_t offset, int64_t num_values) {
    RETURN_NOT_OK(destination->Reserve(num_values));
    destination->UnsafeAppend(values, offset, num_values);
    return Status::OK();
  }
};

template <typename Type, typename ConsumeValue, typename ConsumeNull>
typename arrow::internal::call_traits::enable_if_return<ConsumeValue, void>::type
VisitGroupedValues(const ExecBatch& batch, ConsumeValue&& valid_func,
                   ConsumeNull&& null_func) {
  auto g = batch[1].array()->GetValues<uint32_t>(1);
  if (batch[0].is_array()) {
    VisitArrayValuesInline<Type>(
        *batch[0].array(),
        [&](typename TypeTraits<Type>::CType val) { valid_func(*g++, val); },
        [&]() { null_func(*g++); });
    return;
  }
  const auto& input = *batch[0].scalar();
  if (input.is_valid) {
    const auto val = UnboxScalar<Type>::Unbox(input);
    for (int64_t i = 0; i < batch.length; i++) {
      valid_func(*g++, val);
    }
  } else {
    for (int64_t i = 0; i < batch.length; i++) {
      null_func(*g++);
    }
  }
}

template <typename Type, typename ConsumeValue, typename ConsumeNull>
typename arrow::internal::call_traits::enable_if_return<ConsumeValue, Status>::type
VisitGroupedValues(const ExecBatch& batch, ConsumeValue&& valid_func,
                   ConsumeNull&& null_func) {
  auto g = batch[1].array()->GetValues<uint32_t>(1);
  if (batch[0].is_array()) {
    return VisitArrayValuesInline<Type>(
        *batch[0].array(),
        [&](typename GetViewType<Type>::T val) { return valid_func(*g++, val); },
        [&]() { return null_func(*g++); });
  }
  const auto& input = *batch[0].scalar();
  if (input.is_valid) {
    const auto val = UnboxScalar<Type>::Unbox(input);
    for (int64_t i = 0; i < batch.length; i++) {
      RETURN_NOT_OK(valid_func(*g++, val));
    }
  } else {
    for (int64_t i = 0; i < batch.length; i++) {
      RETURN_NOT_OK(null_func(*g++));
    }
  }
  return Status::OK();
}

template <typename Type, typename ConsumeValue>
void VisitGroupedValuesNonNull(const ExecBatch& batch, ConsumeValue&& valid_func) {
  VisitGroupedValues<Type>(batch, std::forward<ConsumeValue>(valid_func),
                           [](uint32_t) {});
}

// ----------------------------------------------------------------------
// Count implementation

struct GroupedCountImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    options_ = checked_cast<const CountOptions&>(*options);
    counts_ = BufferBuilder(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    return counts_.Append(added_groups * sizeof(int64_t), 0);
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedCountImpl*>(&raw_other);

    auto counts = reinterpret_cast<int64_t*>(counts_.mutable_data());
    auto other_counts = reinterpret_cast<const int64_t*>(other->counts_.mutable_data());

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      counts[*g] += other_counts[other_g];
    }
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    auto counts = reinterpret_cast<int64_t*>(counts_.mutable_data());
    auto g_begin = batch[1].array()->GetValues<uint32_t>(1);

    if (options_.mode == CountOptions::ALL) {
      for (int64_t i = 0; i < batch.length; ++i, ++g_begin) {
        counts[*g_begin] += 1;
      }
    } else if (batch[0].is_array()) {
      const auto& input = batch[0].array();
      if (options_.mode == CountOptions::ONLY_VALID) {
        if (input->type->id() != arrow::Type::NA) {
          arrow::internal::VisitSetBitRunsVoid(
              input->buffers[0], input->offset, input->length,
              [&](int64_t offset, int64_t length) {
                auto g = g_begin + offset;
                for (int64_t i = 0; i < length; ++i, ++g) {
                  counts[*g] += 1;
                }
              });
        }
      } else {  // ONLY_NULL
        if (input->type->id() == arrow::Type::NA) {
          for (int64_t i = 0; i < batch.length; ++i, ++g_begin) {
            counts[*g_begin] += 1;
          }
        } else if (input->MayHaveNulls()) {
          auto end = input->offset + input->length;
          for (int64_t i = input->offset; i < end; ++i, ++g_begin) {
            counts[*g_begin] += !bit_util::GetBit(input->buffers[0]->data(), i);
          }
        }
      }
    } else {
      const auto& input = *batch[0].scalar();
      if (options_.mode == CountOptions::ONLY_VALID) {
        for (int64_t i = 0; i < batch.length; ++i, ++g_begin) {
          counts[*g_begin] += input.is_valid;
        }
      } else {  // ONLY_NULL
        for (int64_t i = 0; i < batch.length; ++i, ++g_begin) {
          counts[*g_begin] += !input.is_valid;
        }
      }
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto counts, counts_.Finish());
    return std::make_shared<Int64Array>(num_groups_, std::move(counts));
  }

  std::shared_ptr<DataType> out_type() const override { return int64(); }

  int64_t num_groups_ = 0;
  CountOptions options_;
  BufferBuilder counts_;
};

// ----------------------------------------------------------------------
// Sum/Mean/Product implementation

template <typename Type, typename Impl>
struct GroupedReducingAggregator : public GroupedAggregator {
  using AccType = typename FindAccumulatorType<Type>::Type;
  using CType = typename TypeTraits<AccType>::CType;
  using InputCType = typename TypeTraits<Type>::CType;

  Status Init(ExecContext* ctx, const std::vector<ValueDescr>& inputs,
              const FunctionOptions* options) override {
    pool_ = ctx->memory_pool();
    options_ = checked_cast<const ScalarAggregateOptions&>(*options);
    reduced_ = TypedBufferBuilder<CType>(pool_);
    counts_ = TypedBufferBuilder<int64_t>(pool_);
    no_nulls_ = TypedBufferBuilder<bool>(pool_);
    out_type_ = GetOutType(inputs[0].type);
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    RETURN_NOT_OK(reduced_.Append(added_groups, Impl::NullValue(*out_type_)));
    RETURN_NOT_OK(counts_.Append(added_groups, 0));
    RETURN_NOT_OK(no_nulls_.Append(added_groups, true));
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    CType* reduced = reduced_.mutable_data();
    int64_t* counts = counts_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();

    VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, InputCType value) {
          reduced[g] = Impl::Reduce(*out_type_, reduced[g], value);
          counts[g]++;
        },
        [&](uint32_t g) { bit_util::SetBitTo(no_nulls, g, false); });
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedReducingAggregator<Type, Impl>*>(&raw_other);

    CType* reduced = reduced_.mutable_data();
    int64_t* counts = counts_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();

    const CType* other_reduced = other->reduced_.data();
    const int64_t* other_counts = other->counts_.data();
    const uint8_t* other_no_nulls = no_nulls_.mutable_data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      counts[*g] += other_counts[other_g];
      reduced[*g] = Impl::Reduce(*out_type_, reduced[*g], other_reduced[other_g]);
      bit_util::SetBitTo(
          no_nulls, *g,
          bit_util::GetBit(no_nulls, *g) && bit_util::GetBit(other_no_nulls, other_g));
    }
    return Status::OK();
  }

  // Generate the values/nulls buffers
  static Result<std::shared_ptr<Buffer>> Finish(MemoryPool* pool,
                                                const ScalarAggregateOptions& options,
                                                const int64_t* counts,
                                                TypedBufferBuilder<CType>* reduced,
                                                int64_t num_groups, int64_t* null_count,
                                                std::shared_ptr<Buffer>* null_bitmap) {
    for (int64_t i = 0; i < num_groups; ++i) {
      if (counts[i] >= options.min_count) continue;

      if ((*null_bitmap) == nullptr) {
        ARROW_ASSIGN_OR_RAISE(*null_bitmap, AllocateBitmap(num_groups, pool));
        bit_util::SetBitsTo((*null_bitmap)->mutable_data(), 0, num_groups, true);
      }

      (*null_count)++;
      bit_util::SetBitTo((*null_bitmap)->mutable_data(), i, false);
    }
    return reduced->Finish();
  }

  Result<Datum> Finalize() override {
    std::shared_ptr<Buffer> null_bitmap = nullptr;
    const int64_t* counts = counts_.data();
    int64_t null_count = 0;

    ARROW_ASSIGN_OR_RAISE(auto values,
                          Impl::Finish(pool_, options_, counts, &reduced_, num_groups_,
                                       &null_count, &null_bitmap));

    if (!options_.skip_nulls) {
      null_count = kUnknownNullCount;
      if (null_bitmap) {
        arrow::internal::BitmapAnd(null_bitmap->data(), /*left_offset=*/0,
                                   no_nulls_.data(), /*right_offset=*/0, num_groups_,
                                   /*out_offset=*/0, null_bitmap->mutable_data());
      } else {
        ARROW_ASSIGN_OR_RAISE(null_bitmap, no_nulls_.Finish());
      }
    }

    return ArrayData::Make(out_type(), num_groups_,
                           {std::move(null_bitmap), std::move(values)}, null_count);
  }

  std::shared_ptr<DataType> out_type() const override { return out_type_; }

  template <typename T = Type>
  static enable_if_t<!is_decimal_type<T>::value, std::shared_ptr<DataType>> GetOutType(
      const std::shared_ptr<DataType>& in_type) {
    return TypeTraits<AccType>::type_singleton();
  }

  template <typename T = Type>
  static enable_if_decimal<T, std::shared_ptr<DataType>> GetOutType(
      const std::shared_ptr<DataType>& in_type) {
    return in_type;
  }

  int64_t num_groups_ = 0;
  ScalarAggregateOptions options_;
  TypedBufferBuilder<CType> reduced_;
  TypedBufferBuilder<int64_t> counts_;
  TypedBufferBuilder<bool> no_nulls_;
  std::shared_ptr<DataType> out_type_;
  MemoryPool* pool_;
};

struct GroupedNullImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    pool_ = ctx->memory_pool();
    options_ = checked_cast<const ScalarAggregateOptions&>(*options);
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override { return Status::OK(); }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    if (options_.skip_nulls && options_.min_count == 0) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> data,
                            AllocateBuffer(num_groups_ * sizeof(int64_t), pool_));
      output_empty(data);
      return ArrayData::Make(out_type(), num_groups_, {nullptr, std::move(data)});
    } else {
      return MakeArrayOfNull(out_type(), num_groups_, pool_);
    }
  }

  virtual void output_empty(const std::shared_ptr<Buffer>& data) = 0;

  int64_t num_groups_;
  ScalarAggregateOptions options_;
  MemoryPool* pool_;
};

template <template <typename> class Impl, const char* kFriendlyName, class NullImpl>
struct GroupedReducingFactory {
  template <typename T, typename AccType = typename FindAccumulatorType<T>::Type>
  Status Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), HashAggregateInit<Impl<T>>);
    return Status::OK();
  }

  Status Visit(const Decimal128Type&) {
    kernel =
        MakeKernel(std::move(argument_type), HashAggregateInit<Impl<Decimal128Type>>);
    return Status::OK();
  }

  Status Visit(const Decimal256Type&) {
    kernel =
        MakeKernel(std::move(argument_type), HashAggregateInit<Impl<Decimal256Type>>);
    return Status::OK();
  }

  Status Visit(const NullType&) {
    kernel = MakeKernel(std::move(argument_type), HashAggregateInit<NullImpl>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Computing ", kFriendlyName, " of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Computing ", kFriendlyName, " of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedReducingFactory<Impl, kFriendlyName, NullImpl> factory;
    factory.argument_type = InputType::Array(type->id());
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return std::move(factory.kernel);
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};

// ----------------------------------------------------------------------
// Sum implementation

template <typename Type>
struct GroupedSumImpl : public GroupedReducingAggregator<Type, GroupedSumImpl<Type>> {
  using Base = GroupedReducingAggregator<Type, GroupedSumImpl<Type>>;
  using CType = typename Base::CType;
  using InputCType = typename Base::InputCType;

  // Default value for a group
  static CType NullValue(const DataType&) { return CType(0); }

  template <typename T = Type>
  static enable_if_number<T, CType> Reduce(const DataType&, const CType u,
                                           const InputCType v) {
    return static_cast<CType>(to_unsigned(u) + to_unsigned(static_cast<CType>(v)));
  }

  static CType Reduce(const DataType&, const CType u, const CType v) {
    return static_cast<CType>(to_unsigned(u) + to_unsigned(v));
  }

  using Base::Finish;
};

struct GroupedSumNullImpl final : public GroupedNullImpl {
  std::shared_ptr<DataType> out_type() const override { return int64(); }

  void output_empty(const std::shared_ptr<Buffer>& data) override {
    std::fill_n(reinterpret_cast<int64_t*>(data->mutable_data()), num_groups_, 0);
  }
};

static constexpr const char kSumName[] = "sum";
using GroupedSumFactory =
    GroupedReducingFactory<GroupedSumImpl, kSumName, GroupedSumNullImpl>;

// ----------------------------------------------------------------------
// Product implementation

template <typename Type>
struct GroupedProductImpl final
    : public GroupedReducingAggregator<Type, GroupedProductImpl<Type>> {
  using Base = GroupedReducingAggregator<Type, GroupedProductImpl<Type>>;
  using AccType = typename Base::AccType;
  using CType = typename Base::CType;
  using InputCType = typename Base::InputCType;

  static CType NullValue(const DataType& out_type) {
    return MultiplyTraits<AccType>::one(out_type);
  }

  template <typename T = Type>
  static enable_if_number<T, CType> Reduce(const DataType& out_type, const CType u,
                                           const InputCType v) {
    return MultiplyTraits<AccType>::Multiply(out_type, u, static_cast<CType>(v));
  }

  static CType Reduce(const DataType& out_type, const CType u, const CType v) {
    return MultiplyTraits<AccType>::Multiply(out_type, u, v);
  }

  using Base::Finish;
};

struct GroupedProductNullImpl final : public GroupedNullImpl {
  std::shared_ptr<DataType> out_type() const override { return int64(); }

  void output_empty(const std::shared_ptr<Buffer>& data) override {
    std::fill_n(reinterpret_cast<int64_t*>(data->mutable_data()), num_groups_, 1);
  }
};

static constexpr const char kProductName[] = "product";
using GroupedProductFactory =
    GroupedReducingFactory<GroupedProductImpl, kProductName, GroupedProductNullImpl>;

// ----------------------------------------------------------------------
// Mean implementation

template <typename Type>
struct GroupedMeanImpl : public GroupedReducingAggregator<Type, GroupedMeanImpl<Type>> {
  using Base = GroupedReducingAggregator<Type, GroupedMeanImpl<Type>>;
  using CType = typename Base::CType;
  using InputCType = typename Base::InputCType;
  using MeanType =
      typename std::conditional<is_decimal_type<Type>::value, CType, double>::type;

  static CType NullValue(const DataType&) { return CType(0); }

  template <typename T = Type>
  static enable_if_number<T, CType> Reduce(const DataType&, const CType u,
                                           const InputCType v) {
    return static_cast<CType>(to_unsigned(u) + to_unsigned(static_cast<CType>(v)));
  }

  static CType Reduce(const DataType&, const CType u, const CType v) {
    return static_cast<CType>(to_unsigned(u) + to_unsigned(v));
  }

  template <typename T = Type>
  static enable_if_decimal<T, Result<MeanType>> DoMean(CType reduced, int64_t count) {
    static_assert(std::is_same<MeanType, CType>::value, "");
    CType quotient, remainder;
    ARROW_ASSIGN_OR_RAISE(std::tie(quotient, remainder), reduced.Divide(count));
    // Round the decimal result based on the remainder
    remainder.Abs();
    if (remainder * 2 >= count) {
      if (reduced >= 0) {
        quotient += 1;
      } else {
        quotient -= 1;
      }
    }
    return quotient;
  }

  template <typename T = Type>
  static enable_if_t<!is_decimal_type<T>::value, Result<MeanType>> DoMean(CType reduced,
                                                                          int64_t count) {
    return static_cast<MeanType>(reduced) / count;
  }

  static Result<std::shared_ptr<Buffer>> Finish(MemoryPool* pool,
                                                const ScalarAggregateOptions& options,
                                                const int64_t* counts,
                                                TypedBufferBuilder<CType>* reduced_,
                                                int64_t num_groups, int64_t* null_count,
                                                std::shared_ptr<Buffer>* null_bitmap) {
    const CType* reduced = reduced_->data();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> values,
                          AllocateBuffer(num_groups * sizeof(MeanType), pool));
    MeanType* means = reinterpret_cast<MeanType*>(values->mutable_data());
    for (int64_t i = 0; i < num_groups; ++i) {
      if (counts[i] >= options.min_count) {
        ARROW_ASSIGN_OR_RAISE(means[i], DoMean(reduced[i], counts[i]));
        continue;
      }
      means[i] = MeanType(0);

      if ((*null_bitmap) == nullptr) {
        ARROW_ASSIGN_OR_RAISE(*null_bitmap, AllocateBitmap(num_groups, pool));
        bit_util::SetBitsTo((*null_bitmap)->mutable_data(), 0, num_groups, true);
      }

      (*null_count)++;
      bit_util::SetBitTo((*null_bitmap)->mutable_data(), i, false);
    }
    return std::move(values);
  }

  std::shared_ptr<DataType> out_type() const override {
    if (is_decimal_type<Type>::value) return this->out_type_;
    return float64();
  }
};

struct GroupedMeanNullImpl final : public GroupedNullImpl {
  std::shared_ptr<DataType> out_type() const override { return float64(); }

  void output_empty(const std::shared_ptr<Buffer>& data) override {
    std::fill_n(reinterpret_cast<double*>(data->mutable_data()), num_groups_, 0);
  }
};

static constexpr const char kMeanName[] = "mean";
using GroupedMeanFactory =
    GroupedReducingFactory<GroupedMeanImpl, kMeanName, GroupedMeanNullImpl>;

// Variance/Stdev implementation

using arrow::internal::int128_t;

template <typename Type>
struct GroupedVarStdImpl : public GroupedAggregator {
  using CType = typename TypeTraits<Type>::CType;

  Status Init(ExecContext* ctx, const std::vector<ValueDescr>& inputs,
              const FunctionOptions* options) override {
    options_ = *checked_cast<const VarianceOptions*>(options);
    if (is_decimal_type<Type>::value) {
      const int32_t scale = checked_cast<const DecimalType&>(*inputs[0].type).scale();
      return InitInternal(ctx, scale, options);
    }
    return InitInternal(ctx, 0, options);
  }

  Status InitInternal(ExecContext* ctx, int32_t decimal_scale,
                      const FunctionOptions* options) {
    options_ = *checked_cast<const VarianceOptions*>(options);
    decimal_scale_ = decimal_scale;
    ctx_ = ctx;
    pool_ = ctx->memory_pool();
    counts_ = TypedBufferBuilder<int64_t>(pool_);
    means_ = TypedBufferBuilder<double>(pool_);
    m2s_ = TypedBufferBuilder<double>(pool_);
    no_nulls_ = TypedBufferBuilder<bool>(pool_);
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    RETURN_NOT_OK(counts_.Append(added_groups, 0));
    RETURN_NOT_OK(means_.Append(added_groups, 0));
    RETURN_NOT_OK(m2s_.Append(added_groups, 0));
    RETURN_NOT_OK(no_nulls_.Append(added_groups, true));
    return Status::OK();
  }

  template <typename T>
  double ToDouble(T value) const {
    return static_cast<double>(value);
  }
  double ToDouble(const Decimal128& value) const {
    return value.ToDouble(decimal_scale_);
  }
  double ToDouble(const Decimal256& value) const {
    return value.ToDouble(decimal_scale_);
  }

  Status Consume(const ExecBatch& batch) override { return ConsumeImpl(batch); }

  // float/double/int64/decimal: calculate `m2` (sum((X-mean)^2)) with
  // `two pass algorithm` (see aggregate_var_std.cc)
  template <typename T = Type>
  enable_if_t<is_floating_type<T>::value || (sizeof(CType) > 4), Status> ConsumeImpl(
      const ExecBatch& batch) {
    using SumType = typename internal::GetSumType<T>::SumType;

    GroupedVarStdImpl<Type> state;
    RETURN_NOT_OK(state.InitInternal(ctx_, decimal_scale_, &options_));
    RETURN_NOT_OK(state.Resize(num_groups_));
    int64_t* counts = state.counts_.mutable_data();
    double* means = state.means_.mutable_data();
    double* m2s = state.m2s_.mutable_data();
    uint8_t* no_nulls = state.no_nulls_.mutable_data();

    // XXX this uses naive summation; we should switch to pairwise summation as was
    // done for the scalar aggregate kernel in ARROW-11567
    std::vector<SumType> sums(num_groups_);
    VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, typename TypeTraits<Type>::CType value) {
          sums[g] += value;
          counts[g]++;
        },
        [&](uint32_t g) { bit_util::ClearBit(no_nulls, g); });

    for (int64_t i = 0; i < num_groups_; i++) {
      means[i] = ToDouble(sums[i]) / counts[i];
    }

    VisitGroupedValuesNonNull<Type>(
        batch, [&](uint32_t g, typename TypeTraits<Type>::CType value) {
          const double v = ToDouble(value);
          m2s[g] += (v - means[g]) * (v - means[g]);
        });

    ARROW_ASSIGN_OR_RAISE(auto mapping,
                          AllocateBuffer(num_groups_ * sizeof(uint32_t), pool_));
    for (uint32_t i = 0; static_cast<int64_t>(i) < num_groups_; i++) {
      reinterpret_cast<uint32_t*>(mapping->mutable_data())[i] = i;
    }
    ArrayData group_id_mapping(uint32(), num_groups_, {nullptr, std::move(mapping)},
                               /*null_count=*/0);
    return this->Merge(std::move(state), group_id_mapping);
  }

  // int32/16/8: textbook one pass algorithm with integer arithmetic (see
  // aggregate_var_std.cc)
  template <typename T = Type>
  enable_if_t<is_integer_type<T>::value && (sizeof(CType) <= 4), Status> ConsumeImpl(
      const ExecBatch& batch) {
    // max number of elements that sum will not overflow int64 (2Gi int32 elements)
    // for uint32:    0 <= sum < 2^63 (int64 >= 0)
    // for int32: -2^62 <= sum < 2^62
    constexpr int64_t max_length = 1ULL << (63 - sizeof(CType) * 8);

    const auto g = batch[1].array()->GetValues<uint32_t>(1);
    if (batch[0].is_scalar() && !batch[0].scalar()->is_valid) {
      uint8_t* no_nulls = no_nulls_.mutable_data();
      for (int64_t i = 0; i < batch.length; i++) {
        bit_util::ClearBit(no_nulls, g[i]);
      }
      return Status::OK();
    }

    std::vector<IntegerVarStd<Type>> var_std(num_groups_);

    ARROW_ASSIGN_OR_RAISE(auto mapping,
                          AllocateBuffer(num_groups_ * sizeof(uint32_t), pool_));
    for (uint32_t i = 0; static_cast<int64_t>(i) < num_groups_; i++) {
      reinterpret_cast<uint32_t*>(mapping->mutable_data())[i] = i;
    }
    ArrayData group_id_mapping(uint32(), num_groups_, {nullptr, std::move(mapping)},
                               /*null_count=*/0);

    for (int64_t start_index = 0; start_index < batch.length; start_index += max_length) {
      // process in chunks that overflow will never happen

      // reset state
      var_std.clear();
      var_std.resize(num_groups_);
      GroupedVarStdImpl<Type> state;
      RETURN_NOT_OK(state.InitInternal(ctx_, decimal_scale_, &options_));
      RETURN_NOT_OK(state.Resize(num_groups_));
      int64_t* other_counts = state.counts_.mutable_data();
      double* other_means = state.means_.mutable_data();
      double* other_m2s = state.m2s_.mutable_data();
      uint8_t* other_no_nulls = state.no_nulls_.mutable_data();

      if (batch[0].is_array()) {
        const auto& array = *batch[0].array();
        const CType* values = array.GetValues<CType>(1);
        auto visit_values = [&](int64_t pos, int64_t len) {
          for (int64_t i = 0; i < len; ++i) {
            const int64_t index = start_index + pos + i;
            const auto value = values[index];
            var_std[g[index]].ConsumeOne(value);
          }
        };

        if (array.MayHaveNulls()) {
          arrow::internal::BitRunReader reader(
              array.buffers[0]->data(), array.offset + start_index,
              std::min(max_length, batch.length - start_index));
          int64_t position = 0;
          while (true) {
            auto run = reader.NextRun();
            if (run.length == 0) break;
            if (run.set) {
              visit_values(position, run.length);
            } else {
              for (int64_t i = 0; i < run.length; ++i) {
                bit_util::ClearBit(other_no_nulls, g[start_index + position + i]);
              }
            }
            position += run.length;
          }
        } else {
          visit_values(0, array.length);
        }
      } else {
        const auto value = UnboxScalar<Type>::Unbox(*batch[0].scalar());
        for (int64_t i = 0; i < std::min(max_length, batch.length - start_index); ++i) {
          const int64_t index = start_index + i;
          var_std[g[index]].ConsumeOne(value);
        }
      }

      for (int64_t i = 0; i < num_groups_; i++) {
        if (var_std[i].count == 0) continue;

        other_counts[i] = var_std[i].count;
        other_means[i] = var_std[i].mean();
        other_m2s[i] = var_std[i].m2();
      }
      RETURN_NOT_OK(this->Merge(std::move(state), group_id_mapping));
    }
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    // Combine m2 from two chunks (see aggregate_var_std.cc)
    auto other = checked_cast<GroupedVarStdImpl*>(&raw_other);

    int64_t* counts = counts_.mutable_data();
    double* means = means_.mutable_data();
    double* m2s = m2s_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();

    const int64_t* other_counts = other->counts_.data();
    const double* other_means = other->means_.data();
    const double* other_m2s = other->m2s_.data();
    const uint8_t* other_no_nulls = other->no_nulls_.data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      if (!bit_util::GetBit(other_no_nulls, other_g)) {
        bit_util::ClearBit(no_nulls, *g);
      }
      if (other_counts[other_g] == 0) continue;
      MergeVarStd(counts[*g], means[*g], other_counts[other_g], other_means[other_g],
                  other_m2s[other_g], &counts[*g], &means[*g], &m2s[*g]);
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    std::shared_ptr<Buffer> null_bitmap;
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> values,
                          AllocateBuffer(num_groups_ * sizeof(double), pool_));
    int64_t null_count = 0;

    double* results = reinterpret_cast<double*>(values->mutable_data());
    const int64_t* counts = counts_.data();
    const double* m2s = m2s_.data();
    for (int64_t i = 0; i < num_groups_; ++i) {
      if (counts[i] > options_.ddof && counts[i] >= options_.min_count) {
        const double variance = m2s[i] / (counts[i] - options_.ddof);
        results[i] = result_type_ == VarOrStd::Var ? variance : std::sqrt(variance);
        continue;
      }

      results[i] = 0;
      if (null_bitmap == nullptr) {
        ARROW_ASSIGN_OR_RAISE(null_bitmap, AllocateBitmap(num_groups_, pool_));
        bit_util::SetBitsTo(null_bitmap->mutable_data(), 0, num_groups_, true);
      }

      null_count += 1;
      bit_util::SetBitTo(null_bitmap->mutable_data(), i, false);
    }
    if (!options_.skip_nulls) {
      if (null_bitmap) {
        arrow::internal::BitmapAnd(null_bitmap->data(), 0, no_nulls_.data(), 0,
                                   num_groups_, 0, null_bitmap->mutable_data());
      } else {
        ARROW_ASSIGN_OR_RAISE(null_bitmap, no_nulls_.Finish());
      }
      null_count = kUnknownNullCount;
    }

    return ArrayData::Make(float64(), num_groups_,
                           {std::move(null_bitmap), std::move(values)}, null_count);
  }

  std::shared_ptr<DataType> out_type() const override { return float64(); }

  VarOrStd result_type_;
  int32_t decimal_scale_;
  VarianceOptions options_;
  int64_t num_groups_ = 0;
  // m2 = count * s2 = sum((X-mean)^2)
  TypedBufferBuilder<int64_t> counts_;
  TypedBufferBuilder<double> means_, m2s_;
  TypedBufferBuilder<bool> no_nulls_;
  ExecContext* ctx_;
  MemoryPool* pool_;
};

template <typename T, VarOrStd result_type>
Result<std::unique_ptr<KernelState>> VarStdInit(KernelContext* ctx,
                                                const KernelInitArgs& args) {
  auto impl = ::arrow::internal::make_unique<GroupedVarStdImpl<T>>();
  impl->result_type_ = result_type;
  RETURN_NOT_OK(impl->Init(ctx->exec_context(), args.inputs, args.options));
  return std::move(impl);
}

template <VarOrStd result_type>
struct GroupedVarStdFactory {
  template <typename T, typename Enable = enable_if_t<is_integer_type<T>::value ||
                                                      is_floating_type<T>::value ||
                                                      is_decimal_type<T>::value>>
  Status Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), VarStdInit<T, result_type>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Computing variance/stddev of data of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Computing variance/stddev of data of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedVarStdFactory factory;
    factory.argument_type = InputType::Array(type->id());
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return std::move(factory.kernel);
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};

// ----------------------------------------------------------------------
// TDigest implementation

using arrow::internal::TDigest;

template <typename Type>
struct GroupedTDigestImpl : public GroupedAggregator {
  using CType = typename TypeTraits<Type>::CType;

  Status Init(ExecContext* ctx, const std::vector<ValueDescr>& inputs,
              const FunctionOptions* options) override {
    options_ = *checked_cast<const TDigestOptions*>(options);
    if (is_decimal_type<Type>::value) {
      decimal_scale_ = checked_cast<const DecimalType&>(*inputs[0].type).scale();
    } else {
      decimal_scale_ = 0;
    }
    ctx_ = ctx;
    pool_ = ctx->memory_pool();
    counts_ = TypedBufferBuilder<int64_t>(pool_);
    no_nulls_ = TypedBufferBuilder<bool>(pool_);
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    const int64_t added_groups = new_num_groups - tdigests_.size();
    tdigests_.reserve(new_num_groups);
    for (int64_t i = 0; i < added_groups; i++) {
      tdigests_.emplace_back(options_.delta, options_.buffer_size);
    }
    RETURN_NOT_OK(counts_.Append(new_num_groups, 0));
    RETURN_NOT_OK(no_nulls_.Append(new_num_groups, true));
    return Status::OK();
  }

  template <typename T>
  double ToDouble(T value) const {
    return static_cast<double>(value);
  }
  double ToDouble(const Decimal128& value) const {
    return value.ToDouble(decimal_scale_);
  }
  double ToDouble(const Decimal256& value) const {
    return value.ToDouble(decimal_scale_);
  }

  Status Consume(const ExecBatch& batch) override {
    int64_t* counts = counts_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();
    VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, CType value) {
          tdigests_[g].NanAdd(ToDouble(value));
          counts[g]++;
        },
        [&](uint32_t g) { bit_util::SetBitTo(no_nulls, g, false); });
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedTDigestImpl*>(&raw_other);

    int64_t* counts = counts_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();

    const int64_t* other_counts = other->counts_.data();
    const uint8_t* other_no_nulls = no_nulls_.mutable_data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      tdigests_[*g].Merge(other->tdigests_[other_g]);
      counts[*g] += other_counts[other_g];
      bit_util::SetBitTo(
          no_nulls, *g,
          bit_util::GetBit(no_nulls, *g) && bit_util::GetBit(other_no_nulls, other_g));
    }

    return Status::OK();
  }

  Result<Datum> Finalize() override {
    const int64_t slot_length = options_.q.size();
    const int64_t num_values = tdigests_.size() * slot_length;
    const int64_t* counts = counts_.data();
    std::shared_ptr<Buffer> null_bitmap;
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> values,
                          AllocateBuffer(num_values * sizeof(double), pool_));
    int64_t null_count = 0;

    double* results = reinterpret_cast<double*>(values->mutable_data());
    for (int64_t i = 0; static_cast<size_t>(i) < tdigests_.size(); ++i) {
      if (!tdigests_[i].is_empty() && counts[i] >= options_.min_count &&
          (options_.skip_nulls || bit_util::GetBit(no_nulls_.data(), i))) {
        for (int64_t j = 0; j < slot_length; j++) {
          results[i * slot_length + j] = tdigests_[i].Quantile(options_.q[j]);
        }
        continue;
      }

      if (!null_bitmap) {
        ARROW_ASSIGN_OR_RAISE(null_bitmap, AllocateBitmap(num_values, pool_));
        bit_util::SetBitsTo(null_bitmap->mutable_data(), 0, num_values, true);
      }
      null_count += slot_length;
      bit_util::SetBitsTo(null_bitmap->mutable_data(), i * slot_length, slot_length,
                          false);
      std::fill(&results[i * slot_length], &results[(i + 1) * slot_length], 0.0);
    }

    auto child = ArrayData::Make(float64(), num_values,
                                 {std::move(null_bitmap), std::move(values)}, null_count);
    return ArrayData::Make(out_type(), tdigests_.size(), {nullptr}, {std::move(child)},
                           /*null_count=*/0);
  }

  std::shared_ptr<DataType> out_type() const override {
    return fixed_size_list(float64(), static_cast<int32_t>(options_.q.size()));
  }

  TDigestOptions options_;
  int32_t decimal_scale_;
  std::vector<TDigest> tdigests_;
  TypedBufferBuilder<int64_t> counts_;
  TypedBufferBuilder<bool> no_nulls_;
  ExecContext* ctx_;
  MemoryPool* pool_;
};

struct GroupedTDigestFactory {
  template <typename T>
  enable_if_number<T, Status> Visit(const T&) {
    kernel =
        MakeKernel(std::move(argument_type), HashAggregateInit<GroupedTDigestImpl<T>>);
    return Status::OK();
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    kernel =
        MakeKernel(std::move(argument_type), HashAggregateInit<GroupedTDigestImpl<T>>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Computing t-digest of data of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Computing t-digest of data of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedTDigestFactory factory;
    factory.argument_type = InputType::Array(type->id());
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return std::move(factory.kernel);
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};

HashAggregateKernel MakeApproximateMedianKernel(HashAggregateFunction* tdigest_func) {
  HashAggregateKernel kernel;
  kernel.init = [tdigest_func](
                    KernelContext* ctx,
                    const KernelInitArgs& args) -> Result<std::unique_ptr<KernelState>> {
    ARROW_ASSIGN_OR_RAISE(auto kernel, tdigest_func->DispatchExact(args.inputs));
    const auto& scalar_options =
        checked_cast<const ScalarAggregateOptions&>(*args.options);
    TDigestOptions options;
    // Default q = 0.5
    options.min_count = scalar_options.min_count;
    options.skip_nulls = scalar_options.skip_nulls;
    KernelInitArgs new_args{kernel, args.inputs, &options};
    return kernel->init(ctx, new_args);
  };
  kernel.signature =
      KernelSignature::Make({InputType(ValueDescr::ANY), InputType::Array(Type::UINT32)},
                            ValueDescr::Array(float64()));
  kernel.resize = HashAggregateResize;
  kernel.consume = HashAggregateConsume;
  kernel.merge = HashAggregateMerge;
  kernel.finalize = [](KernelContext* ctx, Datum* out) {
    ARROW_ASSIGN_OR_RAISE(Datum temp,
                          checked_cast<GroupedAggregator*>(ctx->state())->Finalize());
    *out = temp.array_as<FixedSizeListArray>()->values();
    return Status::OK();
  };
  return kernel;
}

// ----------------------------------------------------------------------
// MinMax implementation

template <typename CType>
struct AntiExtrema {
  static constexpr CType anti_min() { return std::numeric_limits<CType>::max(); }
  static constexpr CType anti_max() { return std::numeric_limits<CType>::min(); }
};

template <>
struct AntiExtrema<bool> {
  static constexpr bool anti_min() { return true; }
  static constexpr bool anti_max() { return false; }
};

template <>
struct AntiExtrema<float> {
  static constexpr float anti_min() { return std::numeric_limits<float>::infinity(); }
  static constexpr float anti_max() { return -std::numeric_limits<float>::infinity(); }
};

template <>
struct AntiExtrema<double> {
  static constexpr double anti_min() { return std::numeric_limits<double>::infinity(); }
  static constexpr double anti_max() { return -std::numeric_limits<double>::infinity(); }
};

template <>
struct AntiExtrema<Decimal128> {
  static constexpr Decimal128 anti_min() { return BasicDecimal128::GetMaxSentinel(); }
  static constexpr Decimal128 anti_max() { return BasicDecimal128::GetMinSentinel(); }
};

template <>
struct AntiExtrema<Decimal256> {
  static constexpr Decimal256 anti_min() { return BasicDecimal256::GetMaxSentinel(); }
  static constexpr Decimal256 anti_max() { return BasicDecimal256::GetMinSentinel(); }
};

template <typename Type, typename Enable = void>
struct GroupedMinMaxImpl final : public GroupedAggregator {
  using CType = typename TypeTraits<Type>::CType;
  using GetSet = GroupedValueTraits<Type>;
  using ArrType =
      typename std::conditional<is_boolean_type<Type>::value, uint8_t, CType>::type;

  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    options_ = *checked_cast<const ScalarAggregateOptions*>(options);
    // type_ initialized by MinMaxInit
    mins_ = TypedBufferBuilder<CType>(ctx->memory_pool());
    maxes_ = TypedBufferBuilder<CType>(ctx->memory_pool());
    has_values_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    has_nulls_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    RETURN_NOT_OK(mins_.Append(added_groups, AntiExtrema<CType>::anti_min()));
    RETURN_NOT_OK(maxes_.Append(added_groups, AntiExtrema<CType>::anti_max()));
    RETURN_NOT_OK(has_values_.Append(added_groups, false));
    RETURN_NOT_OK(has_nulls_.Append(added_groups, false));
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    auto raw_mins = mins_.mutable_data();
    auto raw_maxes = maxes_.mutable_data();

    VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, CType val) {
          GetSet::Set(raw_mins, g, std::min(GetSet::Get(raw_mins, g), val));
          GetSet::Set(raw_maxes, g, std::max(GetSet::Get(raw_maxes, g), val));
          bit_util::SetBit(has_values_.mutable_data(), g);
        },
        [&](uint32_t g) { bit_util::SetBit(has_nulls_.mutable_data(), g); });
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedMinMaxImpl*>(&raw_other);

    auto raw_mins = mins_.mutable_data();
    auto raw_maxes = maxes_.mutable_data();

    auto other_raw_mins = other->mins_.mutable_data();
    auto other_raw_maxes = other->maxes_.mutable_data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length;
         ++other_g, ++g) {
      GetSet::Set(
          raw_mins, *g,
          std::min(GetSet::Get(raw_mins, *g), GetSet::Get(other_raw_mins, other_g)));
      GetSet::Set(
          raw_maxes, *g,
          std::max(GetSet::Get(raw_maxes, *g), GetSet::Get(other_raw_maxes, other_g)));

      if (bit_util::GetBit(other->has_values_.data(), other_g)) {
        bit_util::SetBit(has_values_.mutable_data(), *g);
      }
      if (bit_util::GetBit(other->has_nulls_.data(), other_g)) {
        bit_util::SetBit(has_nulls_.mutable_data(), *g);
      }
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    // aggregation for group is valid if there was at least one value in that group
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap, has_values_.Finish());

    if (!options_.skip_nulls) {
      // ... and there were no nulls in that group
      ARROW_ASSIGN_OR_RAISE(auto has_nulls, has_nulls_.Finish());
      arrow::internal::BitmapAndNot(null_bitmap->data(), 0, has_nulls->data(), 0,
                                    num_groups_, 0, null_bitmap->mutable_data());
    }

    auto mins = ArrayData::Make(type_, num_groups_, {null_bitmap, nullptr});
    auto maxes = ArrayData::Make(type_, num_groups_, {std::move(null_bitmap), nullptr});
    ARROW_ASSIGN_OR_RAISE(mins->buffers[1], mins_.Finish());
    ARROW_ASSIGN_OR_RAISE(maxes->buffers[1], maxes_.Finish());

    return ArrayData::Make(out_type(), num_groups_, {nullptr},
                           {std::move(mins), std::move(maxes)});
  }

  std::shared_ptr<DataType> out_type() const override {
    return struct_({field("min", type_), field("max", type_)});
  }

  int64_t num_groups_;
  TypedBufferBuilder<CType> mins_, maxes_;
  TypedBufferBuilder<bool> has_values_, has_nulls_;
  std::shared_ptr<DataType> type_;
  ScalarAggregateOptions options_;
};

// For binary-like types
// In principle, FixedSizeBinary could use base implementation
template <typename Type>
struct GroupedMinMaxImpl<Type,
                         enable_if_t<is_base_binary_type<Type>::value ||
                                     std::is_same<Type, FixedSizeBinaryType>::value>>
    final : public GroupedAggregator {
  using Allocator = arrow::stl::allocator<char>;
  using StringType = std::basic_string<char, std::char_traits<char>, Allocator>;

  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    ctx_ = ctx;
    allocator_ = Allocator(ctx->memory_pool());
    options_ = *checked_cast<const ScalarAggregateOptions*>(options);
    // type_ initialized by MinMaxInit
    has_values_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    has_nulls_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    DCHECK_GE(added_groups, 0);
    num_groups_ = new_num_groups;
    mins_.resize(new_num_groups);
    maxes_.resize(new_num_groups);
    RETURN_NOT_OK(has_values_.Append(added_groups, false));
    RETURN_NOT_OK(has_nulls_.Append(added_groups, false));
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    return VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, util::string_view val) {
          if (!mins_[g] || val < *mins_[g]) {
            mins_[g].emplace(val.data(), val.size(), allocator_);
          }
          if (!maxes_[g] || val > *maxes_[g]) {
            maxes_[g].emplace(val.data(), val.size(), allocator_);
          }
          bit_util::SetBit(has_values_.mutable_data(), g);
          return Status::OK();
        },
        [&](uint32_t g) {
          bit_util::SetBit(has_nulls_.mutable_data(), g);
          return Status::OK();
        });
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedMinMaxImpl*>(&raw_other);
    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length;
         ++other_g, ++g) {
      if (!mins_[*g] ||
          (mins_[*g] && other->mins_[other_g] && *mins_[*g] > *other->mins_[other_g])) {
        mins_[*g] = std::move(other->mins_[other_g]);
      }
      if (!maxes_[*g] || (maxes_[*g] && other->maxes_[other_g] &&
                          *maxes_[*g] < *other->maxes_[other_g])) {
        maxes_[*g] = std::move(other->maxes_[other_g]);
      }

      if (bit_util::GetBit(other->has_values_.data(), other_g)) {
        bit_util::SetBit(has_values_.mutable_data(), *g);
      }
      if (bit_util::GetBit(other->has_nulls_.data(), other_g)) {
        bit_util::SetBit(has_nulls_.mutable_data(), *g);
      }
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    // aggregation for group is valid if there was at least one value in that group
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap, has_values_.Finish());

    if (!options_.skip_nulls) {
      // ... and there were no nulls in that group
      ARROW_ASSIGN_OR_RAISE(auto has_nulls, has_nulls_.Finish());
      arrow::internal::BitmapAndNot(null_bitmap->data(), 0, has_nulls->data(), 0,
                                    num_groups_, 0, null_bitmap->mutable_data());
    }

    auto mins = ArrayData::Make(type_, num_groups_, {null_bitmap, nullptr});
    auto maxes = ArrayData::Make(type_, num_groups_, {std::move(null_bitmap), nullptr});
    RETURN_NOT_OK(MakeOffsetsValues(mins.get(), mins_));
    RETURN_NOT_OK(MakeOffsetsValues(maxes.get(), maxes_));
    return ArrayData::Make(out_type(), num_groups_, {nullptr},
                           {std::move(mins), std::move(maxes)});
  }

  template <typename T = Type>
  enable_if_base_binary<T, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<util::optional<StringType>>& values) {
    using offset_type = typename T::offset_type;
    ARROW_ASSIGN_OR_RAISE(
        auto raw_offsets,
        AllocateBuffer((1 + values.size()) * sizeof(offset_type), ctx_->memory_pool()));
    offset_type* offsets = reinterpret_cast<offset_type*>(raw_offsets->mutable_data());
    offsets[0] = 0;
    offsets++;
    const uint8_t* null_bitmap = array->buffers[0]->data();
    offset_type total_length = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const util::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        if (value->size() >
                static_cast<size_t>(std::numeric_limits<offset_type>::max()) ||
            arrow::internal::AddWithOverflow(
                total_length, static_cast<offset_type>(value->size()), &total_length)) {
          return Status::Invalid("Result is too large to fit in ", *array->type,
                                 " cast to large_ variant of type");
        }
      }
      offsets[i] = total_length;
    }
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const util::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), value->size());
        offset += value->size();
      }
    }
    array->buffers[1] = std::move(raw_offsets);
    array->buffers.push_back(std::move(data));
    return Status::OK();
  }

  template <typename T = Type>
  enable_if_same<T, FixedSizeBinaryType, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<util::optional<StringType>>& values) {
    const uint8_t* null_bitmap = array->buffers[0]->data();
    const int32_t slot_width =
        checked_cast<const FixedSizeBinaryType&>(*array->type).byte_width();
    int64_t total_length = values.size() * slot_width;
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const util::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), slot_width);
      } else {
        std::memset(data->mutable_data() + offset, 0x00, slot_width);
      }
      offset += slot_width;
    }
    array->buffers[1] = std::move(data);
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override {
    return struct_({field("min", type_), field("max", type_)});
  }

  ExecContext* ctx_;
  Allocator allocator_;
  int64_t num_groups_;
  std::vector<util::optional<StringType>> mins_, maxes_;
  TypedBufferBuilder<bool> has_values_, has_nulls_;
  std::shared_ptr<DataType> type_;
  ScalarAggregateOptions options_;
};

struct GroupedNullMinMaxImpl final : public GroupedAggregator {
  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions*) override {
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override { return Status::OK(); }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    return ArrayData::Make(
        out_type(), num_groups_, {nullptr},
        {
            ArrayData::Make(null(), num_groups_, {nullptr}, num_groups_),
            ArrayData::Make(null(), num_groups_, {nullptr}, num_groups_),
        });
  }

  std::shared_ptr<DataType> out_type() const override {
    return struct_({field("min", null()), field("max", null())});
  }

  int64_t num_groups_;
};

template <typename T>
Result<std::unique_ptr<KernelState>> MinMaxInit(KernelContext* ctx,
                                                const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto impl, HashAggregateInit<GroupedMinMaxImpl<T>>(ctx, args));
  static_cast<GroupedMinMaxImpl<T>*>(impl.get())->type_ = args.inputs[0].type;
  return std::move(impl);
}

template <MinOrMax min_or_max>
HashAggregateKernel MakeMinOrMaxKernel(HashAggregateFunction* min_max_func) {
  HashAggregateKernel kernel;
  kernel.init = [min_max_func](
                    KernelContext* ctx,
                    const KernelInitArgs& args) -> Result<std::unique_ptr<KernelState>> {
    std::vector<ValueDescr> inputs = args.inputs;
    ARROW_ASSIGN_OR_RAISE(auto kernel, min_max_func->DispatchExact(args.inputs));
    KernelInitArgs new_args{kernel, inputs, args.options};
    return kernel->init(ctx, new_args);
  };
  kernel.signature = KernelSignature::Make(
      {InputType(ValueDescr::ANY), InputType::Array(Type::UINT32)},
      OutputType([](KernelContext* ctx,
                    const std::vector<ValueDescr>& descrs) -> Result<ValueDescr> {
        return ValueDescr::Array(descrs[0].type);
      }));
  kernel.resize = HashAggregateResize;
  kernel.consume = HashAggregateConsume;
  kernel.merge = HashAggregateMerge;
  kernel.finalize = [](KernelContext* ctx, Datum* out) {
    ARROW_ASSIGN_OR_RAISE(Datum temp,
                          checked_cast<GroupedAggregator*>(ctx->state())->Finalize());
    *out = temp.array_as<StructArray>()->field(static_cast<uint8_t>(min_or_max));
    return Status::OK();
  };
  return kernel;
}

struct GroupedMinMaxFactory {
  template <typename T>
  enable_if_physical_integer<T, Status> Visit(const T&) {
    using PhysicalType = typename T::PhysicalType;
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<PhysicalType>);
    return Status::OK();
  }

  // MSVC2015 apparently doesn't compile this properly if we use
  // enable_if_floating_point
  Status Visit(const FloatType&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<FloatType>);
    return Status::OK();
  }

  Status Visit(const DoubleType&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<DoubleType>);
    return Status::OK();
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<T>);
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<T>);
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<FixedSizeBinaryType>);
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<BooleanType>);
    return Status::OK();
  }

  Status Visit(const NullType&) {
    kernel =
        MakeKernel(std::move(argument_type), HashAggregateInit<GroupedNullMinMaxImpl>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Computing min/max of data of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Computing min/max of data of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedMinMaxFactory factory;
    factory.argument_type = InputType::Array(type->id());
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return std::move(factory.kernel);
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};

// ----------------------------------------------------------------------
// Any/All implementation

template <typename Impl>
struct GroupedBooleanAggregator : public GroupedAggregator {
  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    options_ = checked_cast<const ScalarAggregateOptions&>(*options);
    pool_ = ctx->memory_pool();
    reduced_ = TypedBufferBuilder<bool>(pool_);
    no_nulls_ = TypedBufferBuilder<bool>(pool_);
    counts_ = TypedBufferBuilder<int64_t>(pool_);
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    RETURN_NOT_OK(reduced_.Append(added_groups, Impl::NullValue()));
    RETURN_NOT_OK(no_nulls_.Append(added_groups, true));
    return counts_.Append(added_groups, 0);
  }

  Status Consume(const ExecBatch& batch) override {
    uint8_t* reduced = reduced_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();
    int64_t* counts = counts_.mutable_data();
    auto g = batch[1].array()->GetValues<uint32_t>(1);

    if (batch[0].is_array()) {
      const auto& input = *batch[0].array();
      if (input.MayHaveNulls()) {
        const uint8_t* bitmap = input.buffers[1]->data();
        arrow::internal::VisitBitBlocksVoid(
            input.buffers[0], input.offset, input.length,
            [&](int64_t position) {
              counts[*g]++;
              Impl::UpdateGroupWith(reduced, *g, bit_util::GetBit(bitmap, position));
              g++;
            },
            [&] { bit_util::SetBitTo(no_nulls, *g++, false); });
      } else {
        arrow::internal::VisitBitBlocksVoid(
            input.buffers[1], input.offset, input.length,
            [&](int64_t) {
              Impl::UpdateGroupWith(reduced, *g, true);
              counts[*g++]++;
            },
            [&]() {
              Impl::UpdateGroupWith(reduced, *g, false);
              counts[*g++]++;
            });
      }
    } else {
      const auto& input = *batch[0].scalar();
      if (input.is_valid) {
        const bool value = UnboxScalar<BooleanType>::Unbox(input);
        for (int64_t i = 0; i < batch.length; i++) {
          Impl::UpdateGroupWith(reduced, *g, value);
          counts[*g++]++;
        }
      } else {
        for (int64_t i = 0; i < batch.length; i++) {
          bit_util::SetBitTo(no_nulls, *g++, false);
        }
      }
    }
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedBooleanAggregator<Impl>*>(&raw_other);

    uint8_t* reduced = reduced_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();
    int64_t* counts = counts_.mutable_data();

    const uint8_t* other_reduced = other->reduced_.mutable_data();
    const uint8_t* other_no_nulls = other->no_nulls_.mutable_data();
    const int64_t* other_counts = other->counts_.mutable_data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      counts[*g] += other_counts[other_g];
      Impl::UpdateGroupWith(reduced, *g, bit_util::GetBit(other_reduced, other_g));
      bit_util::SetBitTo(
          no_nulls, *g,
          bit_util::GetBit(no_nulls, *g) && bit_util::GetBit(other_no_nulls, other_g));
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    std::shared_ptr<Buffer> null_bitmap;
    const int64_t* counts = counts_.data();
    int64_t null_count = 0;

    for (int64_t i = 0; i < num_groups_; ++i) {
      if (counts[i] >= options_.min_count) continue;

      if (null_bitmap == nullptr) {
        ARROW_ASSIGN_OR_RAISE(null_bitmap, AllocateBitmap(num_groups_, pool_));
        bit_util::SetBitsTo(null_bitmap->mutable_data(), 0, num_groups_, true);
      }

      null_count += 1;
      bit_util::SetBitTo(null_bitmap->mutable_data(), i, false);
    }

    ARROW_ASSIGN_OR_RAISE(auto reduced, reduced_.Finish());
    if (!options_.skip_nulls) {
      null_count = kUnknownNullCount;
      ARROW_ASSIGN_OR_RAISE(auto no_nulls, no_nulls_.Finish());
      Impl::AdjustForMinCount(no_nulls->mutable_data(), reduced->data(), num_groups_);
      if (null_bitmap) {
        arrow::internal::BitmapAnd(null_bitmap->data(), /*left_offset=*/0,
                                   no_nulls->data(), /*right_offset=*/0, num_groups_,
                                   /*out_offset=*/0, null_bitmap->mutable_data());
      } else {
        null_bitmap = std::move(no_nulls);
      }
    }

    return ArrayData::Make(out_type(), num_groups_,
                           {std::move(null_bitmap), std::move(reduced)}, null_count);
  }

  std::shared_ptr<DataType> out_type() const override { return boolean(); }

  int64_t num_groups_ = 0;
  ScalarAggregateOptions options_;
  TypedBufferBuilder<bool> reduced_, no_nulls_;
  TypedBufferBuilder<int64_t> counts_;
  MemoryPool* pool_;
};

struct GroupedAnyImpl : public GroupedBooleanAggregator<GroupedAnyImpl> {
  // The default value for a group.
  static bool NullValue() { return false; }

  // Update the value for a group given an observation.
  static void UpdateGroupWith(uint8_t* seen, uint32_t g, bool value) {
    if (!bit_util::GetBit(seen, g) && value) {
      bit_util::SetBit(seen, g);
    }
  }

  // Combine the array of observed nulls with the array of group values.
  static void AdjustForMinCount(uint8_t* no_nulls, const uint8_t* seen,
                                int64_t num_groups) {
    arrow::internal::BitmapOr(no_nulls, /*left_offset=*/0, seen, /*right_offset=*/0,
                              num_groups, /*out_offset=*/0, no_nulls);
  }
};

struct GroupedAllImpl : public GroupedBooleanAggregator<GroupedAllImpl> {
  static bool NullValue() { return true; }

  static void UpdateGroupWith(uint8_t* seen, uint32_t g, bool value) {
    if (!value) {
      bit_util::ClearBit(seen, g);
    }
  }

  static void AdjustForMinCount(uint8_t* no_nulls, const uint8_t* seen,
                                int64_t num_groups) {
    arrow::internal::BitmapOrNot(no_nulls, /*left_offset=*/0, seen, /*right_offset=*/0,
                                 num_groups, /*out_offset=*/0, no_nulls);
  }
};

// ----------------------------------------------------------------------
// CountDistinct/Distinct implementation

struct GroupedCountDistinctImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    ctx_ = ctx;
    pool_ = ctx->memory_pool();
    options_ = checked_cast<const CountOptions&>(*options);
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    ARROW_ASSIGN_OR_RAISE(std::ignore, grouper_->Consume(batch));
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedCountDistinctImpl*>(&raw_other);

    // Get (value, group_id) pairs, then translate the group IDs and consume them
    // ourselves
    ARROW_ASSIGN_OR_RAISE(auto uniques, other->grouper_->GetUniques());
    ARROW_ASSIGN_OR_RAISE(auto remapped_g,
                          AllocateBuffer(uniques.length * sizeof(uint32_t), pool_));

    const auto* g_mapping = group_id_mapping.GetValues<uint32_t>(1);
    const auto* other_g = uniques[1].array()->GetValues<uint32_t>(1);
    auto* g = reinterpret_cast<uint32_t*>(remapped_g->mutable_data());

    for (int64_t i = 0; i < uniques.length; i++) {
      g[i] = g_mapping[other_g[i]];
    }
    uniques.values[1] =
        ArrayData::Make(uint32(), uniques.length, {nullptr, std::move(remapped_g)});

    return Consume(std::move(uniques));
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> values,
                          AllocateBuffer(num_groups_ * sizeof(int64_t), pool_));
    int64_t* counts = reinterpret_cast<int64_t*>(values->mutable_data());
    std::fill(counts, counts + num_groups_, 0);

    ARROW_ASSIGN_OR_RAISE(auto uniques, grouper_->GetUniques());
    auto* g = uniques[1].array()->GetValues<uint32_t>(1);
    const auto& items = *uniques[0].array();
    const auto* valid = items.GetValues<uint8_t>(0, 0);
    if (options_.mode == CountOptions::ALL ||
        (options_.mode == CountOptions::ONLY_VALID && !valid)) {
      for (int64_t i = 0; i < uniques.length; i++) {
        counts[g[i]]++;
      }
    } else if (options_.mode == CountOptions::ONLY_VALID) {
      for (int64_t i = 0; i < uniques.length; i++) {
        counts[g[i]] += bit_util::GetBit(valid, items.offset + i);
      }
    } else if (valid) {  // ONLY_NULL
      for (int64_t i = 0; i < uniques.length; i++) {
        counts[g[i]] += !bit_util::GetBit(valid, items.offset + i);
      }
    }

    return ArrayData::Make(int64(), num_groups_, {nullptr, std::move(values)},
                           /*null_count=*/0);
  }

  std::shared_ptr<DataType> out_type() const override { return int64(); }

  ExecContext* ctx_;
  MemoryPool* pool_;
  int64_t num_groups_;
  CountOptions options_;
  std::unique_ptr<Grouper> grouper_;
  std::shared_ptr<DataType> out_type_;
};

struct GroupedDistinctImpl : public GroupedCountDistinctImpl {
  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto uniques, grouper_->GetUniques());
    ARROW_ASSIGN_OR_RAISE(auto groupings, grouper_->MakeGroupings(
                                              *uniques[1].array_as<UInt32Array>(),
                                              static_cast<uint32_t>(num_groups_), ctx_));
    ARROW_ASSIGN_OR_RAISE(
        auto list, grouper_->ApplyGroupings(*groupings, *uniques[0].make_array(), ctx_));
    auto values = list->values();
    DCHECK_EQ(values->offset(), 0);
    int32_t* offsets = reinterpret_cast<int32_t*>(list->value_offsets()->mutable_data());
    if (options_.mode == CountOptions::ALL ||
        (options_.mode == CountOptions::ONLY_VALID && values->null_count() == 0)) {
      return list;
    } else if (options_.mode == CountOptions::ONLY_VALID) {
      int32_t prev_offset = offsets[0];
      for (int64_t i = 0; i < list->length(); i++) {
        const int32_t slot_length = offsets[i + 1] - prev_offset;
        const int64_t null_count =
            slot_length - arrow::internal::CountSetBits(values->null_bitmap()->data(),
                                                        prev_offset, slot_length);
        DCHECK_LE(null_count, 1);
        const int32_t offset = null_count > 0 ? slot_length - 1 : slot_length;
        prev_offset = offsets[i + 1];
        offsets[i + 1] = offsets[i] + offset;
      }
      auto filter =
          std::make_shared<BooleanArray>(values->length(), values->null_bitmap());
      ARROW_ASSIGN_OR_RAISE(
          auto new_values,
          Filter(std::move(values), filter, FilterOptions(FilterOptions::DROP), ctx_));
      return std::make_shared<ListArray>(list->type(), list->length(),
                                         list->value_offsets(), new_values.make_array());
    }
    // ONLY_NULL
    if (values->null_count() == 0) {
      std::fill(offsets + 1, offsets + list->length() + 1, offsets[0]);
    } else {
      int32_t prev_offset = offsets[0];
      for (int64_t i = 0; i < list->length(); i++) {
        const int32_t slot_length = offsets[i + 1] - prev_offset;
        const int64_t null_count =
            slot_length - arrow::internal::CountSetBits(values->null_bitmap()->data(),
                                                        prev_offset, slot_length);
        const int32_t offset = null_count > 0 ? 1 : 0;
        prev_offset = offsets[i + 1];
        offsets[i + 1] = offsets[i] + offset;
      }
    }
    ARROW_ASSIGN_OR_RAISE(
        auto new_values,
        MakeArrayOfNull(out_type_,
                        list->length() > 0 ? offsets[list->length()] - offsets[0] : 0,
                        pool_));
    return std::make_shared<ListArray>(list->type(), list->length(),
                                       list->value_offsets(), std::move(new_values));
  }

  std::shared_ptr<DataType> out_type() const override { return list(out_type_); }
};

template <typename Impl>
Result<std::unique_ptr<KernelState>> GroupedDistinctInit(KernelContext* ctx,
                                                         const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto impl, HashAggregateInit<Impl>(ctx, args));
  auto instance = static_cast<Impl*>(impl.get());
  instance->out_type_ = args.inputs[0].type;
  ARROW_ASSIGN_OR_RAISE(instance->grouper_,
                        Grouper::Make(args.inputs, ctx->exec_context()));
  return std::move(impl);
}

// ----------------------------------------------------------------------
// One implementation

template <typename Type, typename Enable = void>
struct GroupedOneImpl final : public GroupedAggregator {
  using CType = typename TypeTraits<Type>::CType;
  using GetSet = GroupedValueTraits<Type>;

  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    // out_type_ initialized by GroupedOneInit
    ones_ = TypedBufferBuilder<CType>(ctx->memory_pool());
    has_one_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    RETURN_NOT_OK(ones_.Append(added_groups, static_cast<CType>(0)));
    RETURN_NOT_OK(has_one_.Append(added_groups, false));
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    auto raw_ones_ = ones_.mutable_data();

    return VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, CType val) -> Status {
          if (!bit_util::GetBit(has_one_.data(), g)) {
            GetSet::Set(raw_ones_, g, val);
            bit_util::SetBit(has_one_.mutable_data(), g);
          }
          return Status::OK();
        },
        [&](uint32_t g) -> Status { return Status::OK(); });
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedOneImpl*>(&raw_other);

    auto raw_ones = ones_.mutable_data();
    auto other_raw_ones = other->ones_.mutable_data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length;
         ++other_g, ++g) {
      if (!bit_util::GetBit(has_one_.data(), *g)) {
        if (bit_util::GetBit(other->has_one_.data(), other_g)) {
          GetSet::Set(raw_ones, *g, GetSet::Get(other_raw_ones, other_g));
          bit_util::SetBit(has_one_.mutable_data(), *g);
        }
      }
    }

    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap, has_one_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto data, ones_.Finish());
    return ArrayData::Make(out_type_, num_groups_,
                           {std::move(null_bitmap), std::move(data)});
  }

  std::shared_ptr<DataType> out_type() const override { return out_type_; }

  int64_t num_groups_;
  TypedBufferBuilder<CType> ones_;
  TypedBufferBuilder<bool> has_one_;
  std::shared_ptr<DataType> out_type_;
};

struct GroupedNullOneImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override { return Status::OK(); }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    return ArrayData::Make(null(), num_groups_, {nullptr}, num_groups_);
  }

  std::shared_ptr<DataType> out_type() const override { return null(); }

  int64_t num_groups_;
};

template <typename Type>
struct GroupedOneImpl<Type, enable_if_t<is_base_binary_type<Type>::value ||
                                        std::is_same<Type, FixedSizeBinaryType>::value>>
    final : public GroupedAggregator {
  using Allocator = arrow::stl::allocator<char>;
  using StringType = std::basic_string<char, std::char_traits<char>, Allocator>;

  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    ctx_ = ctx;
    allocator_ = Allocator(ctx->memory_pool());
    // out_type_ initialized by GroupedOneInit
    has_one_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    DCHECK_GE(added_groups, 0);
    num_groups_ = new_num_groups;
    ones_.resize(new_num_groups);
    RETURN_NOT_OK(has_one_.Append(added_groups, false));
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    return VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, util::string_view val) -> Status {
          if (!bit_util::GetBit(has_one_.data(), g)) {
            ones_[g].emplace(val.data(), val.size(), allocator_);
            bit_util::SetBit(has_one_.mutable_data(), g);
          }
          return Status::OK();
        },
        [&](uint32_t g) -> Status { return Status::OK(); });
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedOneImpl*>(&raw_other);
    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length;
         ++other_g, ++g) {
      if (!bit_util::GetBit(has_one_.data(), *g)) {
        if (bit_util::GetBit(other->has_one_.data(), other_g)) {
          ones_[*g] = std::move(other->ones_[other_g]);
          bit_util::SetBit(has_one_.mutable_data(), *g);
        }
      }
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap, has_one_.Finish());
    auto ones =
        ArrayData::Make(out_type(), num_groups_, {std::move(null_bitmap), nullptr});
    RETURN_NOT_OK(MakeOffsetsValues(ones.get(), ones_));
    return ones;
  }

  template <typename T = Type>
  enable_if_base_binary<T, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<util::optional<StringType>>& values) {
    using offset_type = typename T::offset_type;
    ARROW_ASSIGN_OR_RAISE(
        auto raw_offsets,
        AllocateBuffer((1 + values.size()) * sizeof(offset_type), ctx_->memory_pool()));
    auto* offsets = reinterpret_cast<offset_type*>(raw_offsets->mutable_data());
    offsets[0] = 0;
    offsets++;
    const uint8_t* null_bitmap = array->buffers[0]->data();
    offset_type total_length = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const util::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        if (value->size() >
                static_cast<size_t>(std::numeric_limits<offset_type>::max()) ||
            arrow::internal::AddWithOverflow(
                total_length, static_cast<offset_type>(value->size()), &total_length)) {
          return Status::Invalid("Result is too large to fit in ", *array->type,
                                 " cast to large_ variant of type");
        }
      }
      offsets[i] = total_length;
    }
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const util::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), value->size());
        offset += value->size();
      }
    }
    array->buffers[1] = std::move(raw_offsets);
    array->buffers.push_back(std::move(data));
    return Status::OK();
  }

  template <typename T = Type>
  enable_if_same<T, FixedSizeBinaryType, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<util::optional<StringType>>& values) {
    const uint8_t* null_bitmap = array->buffers[0]->data();
    const int32_t slot_width =
        checked_cast<const FixedSizeBinaryType&>(*array->type).byte_width();
    int64_t total_length = values.size() * slot_width;
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const util::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), slot_width);
      } else {
        std::memset(data->mutable_data() + offset, 0x00, slot_width);
      }
      offset += slot_width;
    }
    array->buffers[1] = std::move(data);
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return out_type_; }

  ExecContext* ctx_;
  Allocator allocator_;
  int64_t num_groups_;
  std::vector<util::optional<StringType>> ones_;
  TypedBufferBuilder<bool> has_one_;
  std::shared_ptr<DataType> out_type_;
};

template <typename T>
Result<std::unique_ptr<KernelState>> GroupedOneInit(KernelContext* ctx,
                                                    const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto impl, HashAggregateInit<GroupedOneImpl<T>>(ctx, args));
  auto instance = static_cast<GroupedOneImpl<T>*>(impl.get());
  instance->out_type_ = args.inputs[0].type;
  return std::move(impl);
}

struct GroupedOneFactory {
  template <typename T>
  enable_if_physical_integer<T, Status> Visit(const T&) {
    using PhysicalType = typename T::PhysicalType;
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<PhysicalType>);
    return Status::OK();
  }

  template <typename T>
  enable_if_floating_point<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<T>);
    return Status::OK();
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<T>);
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<T>);
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType&) {
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<FixedSizeBinaryType>);
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<BooleanType>);
    return Status::OK();
  }

  Status Visit(const NullType&) {
    kernel = MakeKernel(std::move(argument_type), HashAggregateInit<GroupedNullOneImpl>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Outputting one of data of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Outputting one of data of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedOneFactory factory;
    factory.argument_type = InputType::Array(type->id());
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return std::move(factory.kernel);
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};

// ----------------------------------------------------------------------
// List implementation

template <typename Type, typename Enable = void>
struct GroupedListImpl final : public GroupedAggregator {
  using CType = typename TypeTraits<Type>::CType;
  using GetSet = GroupedValueTraits<Type>;

  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    ctx_ = ctx;
    has_nulls_ = false;
    // out_type_ initialized by GroupedListInit
    values_ = TypedBufferBuilder<CType>(ctx_->memory_pool());
    groups_ = TypedBufferBuilder<uint32_t>(ctx_->memory_pool());
    values_bitmap_ = TypedBufferBuilder<bool>(ctx_->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    const auto& values_array_data = batch[0].array();
    int64_t num_values = values_array_data->length;

    const auto& groups_array_data = batch[1].array();
    const auto* groups = groups_array_data->GetValues<uint32_t>(1, 0);
    DCHECK_EQ(groups_array_data->offset, 0);
    RETURN_NOT_OK(groups_.Append(groups, num_values));

    int64_t offset = values_array_data->offset;
    const uint8_t* values = values_array_data->buffers[1]->data();
    RETURN_NOT_OK(GetSet::AppendBuffers(&values_, values, offset, num_values));

    if (batch[0].null_count() > 0) {
      if (!has_nulls_) {
        has_nulls_ = true;
        RETURN_NOT_OK(values_bitmap_.Append(num_args_, true));
      }
      const uint8_t* values_bitmap = values_array_data->buffers[0]->data();
      RETURN_NOT_OK(GroupedValueTraits<BooleanType>::AppendBuffers(
          &values_bitmap_, values_bitmap, offset, num_values));
    } else if (has_nulls_) {
      RETURN_NOT_OK(values_bitmap_.Append(num_values, true));
    }
    num_args_ += num_values;
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedListImpl*>(&raw_other);
    const auto* other_raw_groups = other->groups_.data();
    const auto* g = group_id_mapping.GetValues<uint32_t>(1);

    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < other->num_args_;
         ++other_g) {
      RETURN_NOT_OK(groups_.Append(g[other_raw_groups[other_g]]));
    }

    const uint8_t* values = reinterpret_cast<const uint8_t*>(other->values_.data());
    RETURN_NOT_OK(GetSet::AppendBuffers(&values_, values, 0, other->num_args_));

    if (other->has_nulls_) {
      if (!has_nulls_) {
        has_nulls_ = true;
        RETURN_NOT_OK(values_bitmap_.Append(num_args_, true));
      }
      const uint8_t* values_bitmap = other->values_bitmap_.data();
      RETURN_NOT_OK(GroupedValueTraits<BooleanType>::AppendBuffers(
          &values_bitmap_, values_bitmap, 0, other->num_args_));
    } else if (has_nulls_) {
      RETURN_NOT_OK(values_bitmap_.Append(other->num_args_, true));
    }
    num_args_ += other->num_args_;
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto values_buffer, values_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto groups_buffer, groups_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap_buffer, values_bitmap_.Finish());

    auto groups = UInt32Array(num_args_, groups_buffer);
    ARROW_ASSIGN_OR_RAISE(
        auto groupings,
        Grouper::MakeGroupings(groups, static_cast<uint32_t>(num_groups_), ctx_));

    auto values_array_data = ArrayData::Make(
        out_type_, num_args_,
        {has_nulls_ ? std::move(null_bitmap_buffer) : nullptr, std::move(values_buffer)});
    auto values = MakeArray(values_array_data);
    return Grouper::ApplyGroupings(*groupings, *values);
  }

  std::shared_ptr<DataType> out_type() const override { return list(out_type_); }

  ExecContext* ctx_;
  int64_t num_groups_, num_args_ = 0;
  bool has_nulls_ = false;
  TypedBufferBuilder<CType> values_;
  TypedBufferBuilder<uint32_t> groups_;
  TypedBufferBuilder<bool> values_bitmap_;
  std::shared_ptr<DataType> out_type_;
};

template <typename Type>
struct GroupedListImpl<Type, enable_if_t<is_base_binary_type<Type>::value ||
                                         std::is_same<Type, FixedSizeBinaryType>::value>>
    final : public GroupedAggregator {
  using Allocator = arrow::stl::allocator<char>;
  using StringType = std::basic_string<char, std::char_traits<char>, Allocator>;
  using GetSet = GroupedValueTraits<Type>;

  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    ctx_ = ctx;
    allocator_ = Allocator(ctx_->memory_pool());
    // out_type_ initialized by GroupedListInit
    groups_ = TypedBufferBuilder<uint32_t>(ctx_->memory_pool());
    values_bitmap_ = TypedBufferBuilder<bool>(ctx_->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    const auto& values_array_data = batch[0].array();
    int64_t num_values = values_array_data->length;
    int64_t offset = values_array_data->offset;

    const auto& groups_array_data = batch[1].array();
    const auto* groups = groups_array_data->GetValues<uint32_t>(1, 0);
    DCHECK_EQ(groups_array_data->offset, 0);
    RETURN_NOT_OK(groups_.Append(groups, num_values));

    if (batch[0].null_count() == 0) {
      RETURN_NOT_OK(values_bitmap_.Append(num_values, true));
    } else {
      const uint8_t* values_bitmap = values_array_data->buffers[0]->data();
      RETURN_NOT_OK(GroupedValueTraits<BooleanType>::AppendBuffers(
          &values_bitmap_, values_bitmap, offset, num_values));
    }
    num_args_ += num_values;
    return VisitGroupedValues<Type>(
        batch,
        [&](uint32_t group, util::string_view val) -> Status {
          values_.emplace_back(StringType(val.data(), val.size(), allocator_));
          return Status::OK();
        },
        [&](uint32_t group) -> Status {
          values_.emplace_back("");
          return Status::OK();
        });
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedListImpl*>(&raw_other);
    const auto* other_raw_groups = other->groups_.data();
    const auto* g = group_id_mapping.GetValues<uint32_t>(1);

    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < other->num_args_;
         ++other_g) {
      RETURN_NOT_OK(groups_.Append(g[other_raw_groups[other_g]]));
    }

    values_.insert(values_.end(), other->values_.begin(), other->values_.end());

    const uint8_t* values_bitmap = other->values_bitmap_.data();
    RETURN_NOT_OK(GroupedValueTraits<BooleanType>::AppendBuffers(
        &values_bitmap_, values_bitmap, 0, other->num_args_));
    num_args_ += other->num_args_;
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto groups_buffer, groups_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap_buffer, values_bitmap_.Finish());

    auto groups = UInt32Array(num_args_, groups_buffer);
    ARROW_ASSIGN_OR_RAISE(
        auto groupings,
        Grouper::MakeGroupings(groups, static_cast<uint32_t>(num_groups_), ctx_));

    auto values_array_data =
        ArrayData::Make(out_type_, num_args_, {std::move(null_bitmap_buffer), nullptr});
    RETURN_NOT_OK(MakeOffsetsValues(values_array_data.get(), values_));
    auto values = MakeArray(values_array_data);
    return Grouper::ApplyGroupings(*groupings, *values);
  }

  template <typename T = Type>
  enable_if_base_binary<T, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<util::optional<StringType>>& values) {
    using offset_type = typename T::offset_type;
    ARROW_ASSIGN_OR_RAISE(
        auto raw_offsets,
        AllocateBuffer((1 + values.size()) * sizeof(offset_type), ctx_->memory_pool()));
    auto* offsets = reinterpret_cast<offset_type*>(raw_offsets->mutable_data());
    offsets[0] = 0;
    offsets++;
    const uint8_t* null_bitmap = array->buffers[0]->data();
    offset_type total_length = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const util::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        if (value->size() >
                static_cast<size_t>(std::numeric_limits<offset_type>::max()) ||
            arrow::internal::AddWithOverflow(
                total_length, static_cast<offset_type>(value->size()), &total_length)) {
          return Status::Invalid("Result is too large to fit in ", *array->type,
                                 " cast to large_ variant of type");
        }
      }
      offsets[i] = total_length;
    }
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const util::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), value->size());
        offset += value->size();
      }
    }
    array->buffers[1] = std::move(raw_offsets);
    array->buffers.push_back(std::move(data));
    return Status::OK();
  }

  template <typename T = Type>
  enable_if_same<T, FixedSizeBinaryType, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<util::optional<StringType>>& values) {
    const uint8_t* null_bitmap = array->buffers[0]->data();
    const int32_t slot_width =
        checked_cast<const FixedSizeBinaryType&>(*array->type).byte_width();
    int64_t total_length = values.size() * slot_width;
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const util::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), slot_width);
      } else {
        std::memset(data->mutable_data() + offset, 0x00, slot_width);
      }
      offset += slot_width;
    }
    array->buffers[1] = std::move(data);
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return list(out_type_); }

  ExecContext* ctx_;
  Allocator allocator_;
  int64_t num_groups_, num_args_ = 0;
  std::vector<util::optional<StringType>> values_;
  TypedBufferBuilder<uint32_t> groups_;
  TypedBufferBuilder<bool> values_bitmap_;
  std::shared_ptr<DataType> out_type_;
};

struct GroupedNullListImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const std::vector<ValueDescr>&,
              const FunctionOptions* options) override {
    ctx_ = ctx;
    counts_ = TypedBufferBuilder<int64_t>(ctx_->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    return counts_.Append(added_groups, 0);
  }

  Status Consume(const ExecBatch& batch) override {
    int64_t* counts = counts_.mutable_data();
    const auto* g_begin = batch[1].array()->GetValues<uint32_t>(1);
    for (int64_t i = 0; i < batch.length; ++i, ++g_begin) {
      counts[*g_begin] += 1;
    }
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedNullListImpl*>(&raw_other);

    int64_t* counts = counts_.mutable_data();
    const int64_t* other_counts = other->counts_.data();

    const auto* g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      counts[*g] += other_counts[other_g];
    }

    return Status::OK();
  }

  Result<Datum> Finalize() override {
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(ctx_->memory_pool(), list(null()), &builder));
    auto list_builder = checked_cast<ListBuilder*>(builder.get());
    auto value_builder = checked_cast<NullBuilder*>(list_builder->value_builder());
    const int64_t* counts = counts_.data();

    for (int64_t group = 0; group < num_groups_; ++group) {
      RETURN_NOT_OK(list_builder->Append(true));
      RETURN_NOT_OK(value_builder->AppendNulls(counts[group]));
    }
    return list_builder->Finish();
  }

  std::shared_ptr<DataType> out_type() const override { return list(null()); }

  ExecContext* ctx_;
  int64_t num_groups_ = 0;
  TypedBufferBuilder<int64_t> counts_;
};

template <typename T>
Result<std::unique_ptr<KernelState>> GroupedListInit(KernelContext* ctx,
                                                     const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto impl, HashAggregateInit<GroupedListImpl<T>>(ctx, args));
  auto instance = static_cast<GroupedListImpl<T>*>(impl.get());
  instance->out_type_ = args.inputs[0].type;
  return std::move(impl);
}

struct GroupedListFactory {
  template <typename T>
  enable_if_physical_integer<T, Status> Visit(const T&) {
    using PhysicalType = typename T::PhysicalType;
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<PhysicalType>);
    return Status::OK();
  }

  template <typename T>
  enable_if_floating_point<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<T>);
    return Status::OK();
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<T>);
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<T>);
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType&) {
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<FixedSizeBinaryType>);
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<BooleanType>);
    return Status::OK();
  }

  Status Visit(const NullType&) {
    kernel = MakeKernel(std::move(argument_type), HashAggregateInit<GroupedNullListImpl>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Outputting list of data of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Outputting list of data of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedListFactory factory;
    factory.argument_type = InputType::Array(type->id());
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return std::move(factory.kernel);
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};
}  // namespace

Result<std::vector<const HashAggregateKernel*>> GetKernels(
    ExecContext* ctx, const std::vector<Aggregate>& aggregates,
    const std::vector<ValueDescr>& in_descrs) {
  if (aggregates.size() != in_descrs.size()) {
    return Status::Invalid(aggregates.size(), " aggregate functions were specified but ",
                           in_descrs.size(), " arguments were provided.");
  }

  std::vector<const HashAggregateKernel*> kernels(in_descrs.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto function,
                          ctx->func_registry()->GetFunction(aggregates[i].function));
    ARROW_ASSIGN_OR_RAISE(
        const Kernel* kernel,
        function->DispatchExact({in_descrs[i], ValueDescr::Array(uint32())}));
    kernels[i] = static_cast<const HashAggregateKernel*>(kernel);
  }
  return kernels;
}

Result<std::vector<std::unique_ptr<KernelState>>> InitKernels(
    const std::vector<const HashAggregateKernel*>& kernels, ExecContext* ctx,
    const std::vector<Aggregate>& aggregates, const std::vector<ValueDescr>& in_descrs) {
  std::vector<std::unique_ptr<KernelState>> states(kernels.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    auto options = aggregates[i].options;

    if (options == nullptr) {
      // use known default options for the named function if possible
      auto maybe_function = ctx->func_registry()->GetFunction(aggregates[i].function);
      if (maybe_function.ok()) {
        options = maybe_function.ValueOrDie()->default_options();
      }
    }

    KernelContext kernel_ctx{ctx};
    ARROW_ASSIGN_OR_RAISE(
        states[i],
        kernels[i]->init(&kernel_ctx, KernelInitArgs{kernels[i],
                                                     {
                                                         in_descrs[i],
                                                         ValueDescr::Array(uint32()),
                                                     },
                                                     options}));
  }

  return std::move(states);
}

Result<FieldVector> ResolveKernels(
    const std::vector<Aggregate>& aggregates,
    const std::vector<const HashAggregateKernel*>& kernels,
    const std::vector<std::unique_ptr<KernelState>>& states, ExecContext* ctx,
    const std::vector<ValueDescr>& descrs) {
  FieldVector fields(descrs.size());

  for (size_t i = 0; i < kernels.size(); ++i) {
    KernelContext kernel_ctx{ctx};
    kernel_ctx.SetState(states[i].get());

    ARROW_ASSIGN_OR_RAISE(auto descr, kernels[i]->signature->out_type().Resolve(
                                          &kernel_ctx, {
                                                           descrs[i],
                                                           ValueDescr::Array(uint32()),
                                                       }));
    fields[i] = field(aggregates[i].function, std::move(descr.type));
  }
  return fields;
}

Result<Datum> GroupBy(const std::vector<Datum>& arguments, const std::vector<Datum>& keys,
                      const std::vector<Aggregate>& aggregates, bool use_threads,
                      ExecContext* ctx) {
  auto task_group =
      use_threads
          ? arrow::internal::TaskGroup::MakeThreaded(arrow::internal::GetCpuThreadPool())
          : arrow::internal::TaskGroup::MakeSerial();

  std::vector<const HashAggregateKernel*> kernels;
  std::vector<std::vector<std::unique_ptr<KernelState>>> states;
  FieldVector out_fields;

  using arrow::compute::detail::ExecBatchIterator;
  std::unique_ptr<ExecBatchIterator> argument_batch_iterator;

  if (!arguments.empty()) {
    // Construct and initialize HashAggregateKernels
    ARROW_ASSIGN_OR_RAISE(auto argument_descrs,
                          ExecBatch::Make(arguments).Map(
                              [](ExecBatch batch) { return batch.GetDescriptors(); }));

    ARROW_ASSIGN_OR_RAISE(kernels, GetKernels(ctx, aggregates, argument_descrs));

    states.resize(task_group->parallelism());
    for (auto& state : states) {
      ARROW_ASSIGN_OR_RAISE(state,
                            InitKernels(kernels, ctx, aggregates, argument_descrs));
    }

    ARROW_ASSIGN_OR_RAISE(
        out_fields, ResolveKernels(aggregates, kernels, states[0], ctx, argument_descrs));

    ARROW_ASSIGN_OR_RAISE(argument_batch_iterator,
                          ExecBatchIterator::Make(arguments, ctx->exec_chunksize()));
  }

  // Construct Groupers
  ARROW_ASSIGN_OR_RAISE(auto key_descrs, ExecBatch::Make(keys).Map([](ExecBatch batch) {
    return batch.GetDescriptors();
  }));

  std::vector<std::unique_ptr<Grouper>> groupers(task_group->parallelism());
  for (auto& grouper : groupers) {
    ARROW_ASSIGN_OR_RAISE(grouper, Grouper::Make(key_descrs, ctx));
  }

  std::mutex mutex;
  std::unordered_map<std::thread::id, size_t> thread_ids;

  int i = 0;
  for (ValueDescr& key_descr : key_descrs) {
    out_fields.push_back(field("key_" + std::to_string(i++), std::move(key_descr.type)));
  }

  ARROW_ASSIGN_OR_RAISE(auto key_batch_iterator,
                        ExecBatchIterator::Make(keys, ctx->exec_chunksize()));

  // start "streaming" execution
  ExecBatch key_batch, argument_batch;
  while ((argument_batch_iterator == NULLPTR ||
          argument_batch_iterator->Next(&argument_batch)) &&
         key_batch_iterator->Next(&key_batch)) {
    if (key_batch.length == 0) continue;

    task_group->Append([&, key_batch, argument_batch] {
      size_t thread_index;
      {
        std::unique_lock<std::mutex> lock(mutex);
        auto it = thread_ids.emplace(std::this_thread::get_id(), thread_ids.size()).first;
        thread_index = it->second;
        DCHECK_LT(static_cast<int>(thread_index), task_group->parallelism());
      }

      auto grouper = groupers[thread_index].get();

      // compute a batch of group ids
      ARROW_ASSIGN_OR_RAISE(Datum id_batch, grouper->Consume(key_batch));

      // consume group ids with HashAggregateKernels
      for (size_t i = 0; i < kernels.size(); ++i) {
        KernelContext batch_ctx{ctx};
        batch_ctx.SetState(states[thread_index][i].get());
        ARROW_ASSIGN_OR_RAISE(auto batch, ExecBatch::Make({argument_batch[i], id_batch}));
        RETURN_NOT_OK(kernels[i]->resize(&batch_ctx, grouper->num_groups()));
        RETURN_NOT_OK(kernels[i]->consume(&batch_ctx, batch));
      }

      return Status::OK();
    });
  }

  RETURN_NOT_OK(task_group->Finish());

  // Merge if necessary
  for (size_t thread_index = 1; thread_index < thread_ids.size(); ++thread_index) {
    ARROW_ASSIGN_OR_RAISE(ExecBatch other_keys, groupers[thread_index]->GetUniques());
    ARROW_ASSIGN_OR_RAISE(Datum transposition, groupers[0]->Consume(other_keys));
    groupers[thread_index].reset();

    for (size_t idx = 0; idx < kernels.size(); ++idx) {
      KernelContext batch_ctx{ctx};
      batch_ctx.SetState(states[0][idx].get());

      RETURN_NOT_OK(kernels[idx]->resize(&batch_ctx, groupers[0]->num_groups()));
      RETURN_NOT_OK(kernels[idx]->merge(&batch_ctx, std::move(*states[thread_index][idx]),
                                        *transposition.array()));
      states[thread_index][idx].reset();
    }
  }

  // Finalize output
  ArrayDataVector out_data(arguments.size() + keys.size());
  auto it = out_data.begin();

  for (size_t idx = 0; idx < kernels.size(); ++idx) {
    KernelContext batch_ctx{ctx};
    batch_ctx.SetState(states[0][idx].get());
    Datum out;
    RETURN_NOT_OK(kernels[idx]->finalize(&batch_ctx, &out));
    *it++ = out.array();
  }

  ARROW_ASSIGN_OR_RAISE(ExecBatch out_keys, groupers[0]->GetUniques());
  for (const auto& key : out_keys.values) {
    *it++ = key.array();
  }

  int64_t length = out_data[0]->length;
  return ArrayData::Make(struct_(std::move(out_fields)), length,
                         {/*null_bitmap=*/nullptr}, std::move(out_data),
                         /*null_count=*/0);
}

namespace {
const FunctionDoc hash_count_doc{
    "Count the number of null / non-null values in each group",
    ("By default, non-null values are counted.\n"
     "This can be changed through ScalarAggregateOptions."),
    {"array", "group_id_array"},
    "CountOptions"};

const FunctionDoc hash_sum_doc{"Sum values in each group",
                               ("Null values are ignored."),
                               {"array", "group_id_array"},
                               "ScalarAggregateOptions"};

const FunctionDoc hash_product_doc{
    "Compute the product of values in each group",
    ("Null values are ignored.\n"
     "On integer overflow, the result will wrap around as if the calculation\n"
     "was done with unsigned integers."),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_mean_doc{
    "Compute the mean of values in each group",
    ("Null values are ignored.\n"
     "For integers and floats, NaN is returned if min_count = 0 and\n"
     "there are no values. For decimals, null is returned instead."),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_stddev_doc{
    "Compute the standard deviation of values in each group",
    ("The number of degrees of freedom can be controlled using VarianceOptions.\n"
     "By default (`ddof` = 0), the population standard deviation is calculated.\n"
     "Nulls are ignored.  If there are not enough non-null values in the array\n"
     "to satisfy `ddof`, null is returned."),
    {"array", "group_id_array"}};

const FunctionDoc hash_variance_doc{
    "Compute the variance of values in each group",
    ("The number of degrees of freedom can be controlled using VarianceOptions.\n"
     "By default (`ddof` = 0), the population variance is calculated.\n"
     "Nulls are ignored.  If there are not enough non-null values in the array\n"
     "to satisfy `ddof`, null is returned."),
    {"array", "group_id_array"}};

const FunctionDoc hash_tdigest_doc{
    "Compute approximate quantiles of values in each group",
    ("The T-Digest algorithm is used for a fast approximation.\n"
     "By default, the 0.5 quantile (i.e. median) is returned.\n"
     "Nulls and NaNs are ignored.\n"
     "Nulls are returned if there are no valid data points."),
    {"array", "group_id_array"},
    "TDigestOptions"};

const FunctionDoc hash_approximate_median_doc{
    "Compute approximate medians of values in each group",
    ("The T-Digest algorithm is used for a fast approximation.\n"
     "Nulls and NaNs are ignored.\n"
     "Nulls are returned if there are no valid data points."),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_min_max_doc{
    "Compute the minimum and maximum of values in each group",
    ("Null values are ignored by default.\n"
     "This can be changed through ScalarAggregateOptions."),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_min_or_max_doc{
    "Compute the minimum or maximum of values in each group",
    ("Null values are ignored by default.\n"
     "This can be changed through ScalarAggregateOptions."),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_any_doc{"Whether any element in each group evaluates to true",
                               ("Null values are ignored."),
                               {"array", "group_id_array"},
                               "ScalarAggregateOptions"};

const FunctionDoc hash_all_doc{"Whether all elements in each group evaluate to true",
                               ("Null values are ignored."),
                               {"array", "group_id_array"},
                               "ScalarAggregateOptions"};

const FunctionDoc hash_count_distinct_doc{
    "Count the distinct values in each group",
    ("Whether nulls/values are counted is controlled by CountOptions.\n"
     "NaNs and signed zeroes are not normalized."),
    {"array", "group_id_array"},
    "CountOptions"};

const FunctionDoc hash_distinct_doc{
    "Keep the distinct values in each group",
    ("Whether nulls/values are kept is controlled by CountOptions.\n"
     "NaNs and signed zeroes are not normalized."),
    {"array", "group_id_array"},
    "CountOptions"};

const FunctionDoc hash_one_doc{"Get one value from each group",
                               ("Null values are also returned."),
                               {"array", "group_id_array"}};

const FunctionDoc hash_list_doc{"List all values in each group",
                                ("Null values are also returned."),
                                {"array", "group_id_array"}};
}  // namespace

void RegisterHashAggregateBasic(FunctionRegistry* registry) {
  static auto default_count_options = CountOptions::Defaults();
  static auto default_scalar_aggregate_options = ScalarAggregateOptions::Defaults();
  static auto default_tdigest_options = TDigestOptions::Defaults();
  static auto default_variance_options = VarianceOptions::Defaults();

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_count", Arity::Binary(), hash_count_doc, &default_count_options);

    DCHECK_OK(func->AddKernel(
        MakeKernel(ValueDescr::ARRAY, HashAggregateInit<GroupedCountImpl>)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_sum", Arity::Binary(), hash_sum_doc, &default_scalar_aggregate_options);
    DCHECK_OK(AddHashAggKernels({boolean()}, GroupedSumFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(SignedIntTypes(), GroupedSumFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(UnsignedIntTypes(), GroupedSumFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(FloatingPointTypes(), GroupedSumFactory::Make, func.get()));
    // Type parameters are ignored
    DCHECK_OK(AddHashAggKernels({decimal128(1, 1), decimal256(1, 1)},
                                GroupedSumFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({null()}, GroupedSumFactory::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_product", Arity::Binary(), hash_product_doc,
        &default_scalar_aggregate_options);
    DCHECK_OK(AddHashAggKernels({boolean()}, GroupedProductFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(SignedIntTypes(), GroupedProductFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(UnsignedIntTypes(), GroupedProductFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(FloatingPointTypes(), GroupedProductFactory::Make, func.get()));
    // Type parameters are ignored
    DCHECK_OK(AddHashAggKernels({decimal128(1, 1), decimal256(1, 1)},
                                GroupedProductFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({null()}, GroupedProductFactory::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_mean", Arity::Binary(), hash_mean_doc, &default_scalar_aggregate_options);
    DCHECK_OK(AddHashAggKernels({boolean()}, GroupedMeanFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(SignedIntTypes(), GroupedMeanFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(UnsignedIntTypes(), GroupedMeanFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(FloatingPointTypes(), GroupedMeanFactory::Make, func.get()));
    // Type parameters are ignored
    DCHECK_OK(AddHashAggKernels({decimal128(1, 1), decimal256(1, 1)},
                                GroupedMeanFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({null()}, GroupedMeanFactory::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_stddev", Arity::Binary(), hash_stddev_doc, &default_variance_options);
    DCHECK_OK(AddHashAggKernels(SignedIntTypes(),
                                GroupedVarStdFactory<VarOrStd::Std>::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(UnsignedIntTypes(),
                                GroupedVarStdFactory<VarOrStd::Std>::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(FloatingPointTypes(),
                                GroupedVarStdFactory<VarOrStd::Std>::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({decimal128(1, 1), decimal256(1, 1)},
                                GroupedVarStdFactory<VarOrStd::Std>::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_variance", Arity::Binary(), hash_variance_doc, &default_variance_options);
    DCHECK_OK(AddHashAggKernels(SignedIntTypes(),
                                GroupedVarStdFactory<VarOrStd::Var>::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(UnsignedIntTypes(),
                                GroupedVarStdFactory<VarOrStd::Var>::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(FloatingPointTypes(),
                                GroupedVarStdFactory<VarOrStd::Var>::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({decimal128(1, 1), decimal256(1, 1)},
                                GroupedVarStdFactory<VarOrStd::Var>::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  HashAggregateFunction* tdigest_func = nullptr;
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_tdigest", Arity::Binary(), hash_tdigest_doc, &default_tdigest_options);
    DCHECK_OK(
        AddHashAggKernels(SignedIntTypes(), GroupedTDigestFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(UnsignedIntTypes(), GroupedTDigestFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(FloatingPointTypes(), GroupedTDigestFactory::Make, func.get()));
    // Type parameters are ignored
    DCHECK_OK(AddHashAggKernels({decimal128(1, 1), decimal256(1, 1)},
                                GroupedTDigestFactory::Make, func.get()));
    tdigest_func = func.get();
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_approximate_median", Arity::Binary(), hash_approximate_median_doc,
        &default_scalar_aggregate_options);
    DCHECK_OK(func->AddKernel(MakeApproximateMedianKernel(tdigest_func)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  HashAggregateFunction* min_max_func = nullptr;
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_min_max", Arity::Binary(), hash_min_max_doc,
        &default_scalar_aggregate_options);
    DCHECK_OK(AddHashAggKernels(NumericTypes(), GroupedMinMaxFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(TemporalTypes(), GroupedMinMaxFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(BaseBinaryTypes(), GroupedMinMaxFactory::Make, func.get()));
    // Type parameters are ignored
    DCHECK_OK(AddHashAggKernels({null(), boolean(), decimal128(1, 1), decimal256(1, 1),
                                 month_interval(), fixed_size_binary(1)},
                                GroupedMinMaxFactory::Make, func.get()));
    min_max_func = func.get();
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_min", Arity::Binary(), hash_min_or_max_doc,
        &default_scalar_aggregate_options);
    DCHECK_OK(func->AddKernel(MakeMinOrMaxKernel<MinOrMax::Min>(min_max_func)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_max", Arity::Binary(), hash_min_or_max_doc,
        &default_scalar_aggregate_options);
    DCHECK_OK(func->AddKernel(MakeMinOrMaxKernel<MinOrMax::Max>(min_max_func)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_any", Arity::Binary(), hash_any_doc, &default_scalar_aggregate_options);
    DCHECK_OK(func->AddKernel(MakeKernel(boolean(), HashAggregateInit<GroupedAnyImpl>)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_all", Arity::Binary(), hash_all_doc, &default_scalar_aggregate_options);
    DCHECK_OK(func->AddKernel(MakeKernel(boolean(), HashAggregateInit<GroupedAllImpl>)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_count_distinct", Arity::Binary(), hash_count_distinct_doc,
        &default_count_options);
    DCHECK_OK(func->AddKernel(
        MakeKernel(ValueDescr::ARRAY, GroupedDistinctInit<GroupedCountDistinctImpl>)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_distinct", Arity::Binary(), hash_distinct_doc, &default_count_options);
    DCHECK_OK(func->AddKernel(
        MakeKernel(ValueDescr::ARRAY, GroupedDistinctInit<GroupedDistinctImpl>)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>("hash_one", Arity::Binary(),
                                                        hash_one_doc);
    DCHECK_OK(AddHashAggKernels(NumericTypes(), GroupedOneFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(TemporalTypes(), GroupedOneFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(BaseBinaryTypes(), GroupedOneFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({null(), boolean(), decimal128(1, 1), decimal256(1, 1),
                                 month_interval(), fixed_size_binary(1)},
                                GroupedOneFactory::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>("hash_list", Arity::Binary(),
                                                        hash_list_doc);
    DCHECK_OK(AddHashAggKernels(NumericTypes(), GroupedListFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(TemporalTypes(), GroupedListFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(BaseBinaryTypes(), GroupedListFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({null(), boolean(), decimal128(1, 1), decimal256(1, 1),
                                 month_interval(), fixed_size_binary(1)},
                                GroupedListFactory::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
