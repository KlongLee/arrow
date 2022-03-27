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

#include "arrow/array/array_base.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/base_arithmetic_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/result.h"
#include "arrow/util/bit_util.h"
#include "arrow/visit_type_inline.h"

namespace arrow {
namespace compute {
namespace internal {

template <typename OutType, typename ArgType, typename Op, typename OptionsType>
struct CumulativeGeneric {
  using OutValue = typename GetOutputType<OutType>::T;
  using ArgValue = typename GetViewType<ArgType>::T;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = OptionsWrapper<OptionsType>::Get(ctx);
    auto start = UnboxScalar<ArgType>::Unbox(*options.start);
    bool skip_nulls = options.skip_nulls;

    switch (batch[0].kind()) {
      case Datum::ARRAY: {
        return Call(ctx, *batch[0].array(), start, out, skip_nulls);
      }
      case Datum::CHUNKED_ARRAY: {
        const auto& input = batch[0].chunked_array();

        for (const auto& chunk : input->chunks()) {
          RETURN_NOT_OK(Call(ctx, *chunk->data(), start, out, skip_nulls));
        }
      }
      default:
        return Status::NotImplemented(
            "Unsupported input type for function 'cumulative_<operator>': ",
            batch[0].ToString());
    }

    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& arg0, ArgValue& partial_scan,
                     Datum* out, bool skip_nulls) {
    Status st = Status::OK();
    ArrayData* out_arr = out->mutable_array();
    auto out_data = out_arr->GetMutableValues<OutValue>(1);

    if (skip_nulls) {
      VisitArrayValuesInline<ArgType>(
          arg0,
          [&](ArgValue v) {
            partial_scan = Op::template Call<OutValue, ArgValue, ArgValue>(
                ctx, v, partial_scan, &st);
            *out_data++ = partial_scan;
          },
          [&]() {
            // null
            *out_data++ = OutValue{};
          });
    } else {
      bool encounted_null = false;
      auto start_null_idx = arg0.offset;

      VisitArrayValuesInline<ArgType>(
          arg0,
          [&](ArgValue v) {
            if (encounted_null) {
              *out_data++ = OutValue{};
            } else {
              partial_scan = Op::template Call<OutValue, ArgValue, ArgValue>(
                  ctx, v, partial_scan, &st);
              *out_data++ = partial_scan;
              ++start_null_idx;
            }
          },
          [&]() {
            // null
            *out_data++ = OutValue{};
            encounted_null = true;
          });

      auto out_bitmap = out_arr->GetMutableValues<uint8_t>(0);
      auto null_length = arg0.length - (start_null_idx - arg0.offset);
      arrow::bit_util::SetBitsTo(out_bitmap, start_null_idx, null_length, false);
    }

    return st;
  }
};

const FunctionDoc cumulative_sum_doc{
    "Compute the cumulative sum over an array of numbers",
    ("`values` must be an array of numeric type values.\n"
     "Return an array which is the cumulative sum computed over `values.`"),
    {"values"},
    "CumulativeGenericOptions"};

void RegisterVectorCumulativeSum(FunctionRegistry* registry) {
  auto options = CumulativeGenericOptions::Defaults();
  auto cumulative_sum = std::make_shared<VectorFunction>("cumulative_sum", Arity::Unary(),
                                                         &cumulative_sum_doc, &options);

  std::vector<std::shared_ptr<DataType>> types;
  types.insert(types.end(), NumericTypes().begin(), NumericTypes().end());

  for (const auto& ty : NumericTypes()) {
    VectorKernel kernel;
    kernel.can_execute_chunkwise = false;
    kernel.null_handling = NullHandling::type::INTERSECTION;
    kernel.mem_allocation = MemAllocation::type::PREALLOCATE;
    kernel.signature = KernelSignature::Make({InputType(ty)}, OutputType(ty));
    kernel.exec =
        ArithmeticExecFromOp<CumulativeGeneric, Add, CumulativeGenericOptions>(ty);
    kernel.init = OptionsWrapper<CumulativeGenericOptions>::Init;
    DCHECK_OK(cumulative_sum->AddKernel(std::move(kernel)));
  }

  DCHECK_OK(registry->AddFunction(std::move(cumulative_sum)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
