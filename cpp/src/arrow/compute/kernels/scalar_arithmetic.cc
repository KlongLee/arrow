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
#include <limits>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::AddWithOverflow;
using internal::DivideWithOverflow;
using internal::MultiplyWithOverflow;
using internal::NegateWithOverflow;
using internal::SubtractWithOverflow;

namespace compute {
namespace internal {

using applicator::ScalarBinaryEqualTypes;
using applicator::ScalarBinaryNotNullEqualTypes;
using applicator::ScalarUnary;
using applicator::ScalarUnaryNotNull;

namespace {

template <typename T>
using is_unsigned_integer = std::integral_constant<bool, std::is_integral<T>::value &&
                                                             std::is_unsigned<T>::value>;

template <typename T>
using is_signed_integer =
    std::integral_constant<bool, std::is_integral<T>::value && std::is_signed<T>::value>;

template <typename T>
using enable_if_signed_integer = enable_if_t<is_signed_integer<T>::value, T>;

template <typename T>
using enable_if_unsigned_integer = enable_if_t<is_unsigned_integer<T>::value, T>;

template <typename T>
using enable_if_integer =
    enable_if_t<is_signed_integer<T>::value || is_unsigned_integer<T>::value, T>;

template <typename T>
using enable_if_floating_point = enable_if_t<std::is_floating_point<T>::value, T>;

template <typename T, typename Unsigned = typename std::make_unsigned<T>::type>
constexpr Unsigned to_unsigned(T signed_) {
  return static_cast<Unsigned>(signed_);
}

struct AbsoluteValue {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T arg, Status*) {
    return std::fabs(arg);
  }

  template <typename T, typename Arg>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, T arg, Status*) {
    return arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T arg, Status* st) {
    return (arg < 0) ? arrow::internal::SafeSignedNegate(arg) : arg;
  }
};

struct AbsoluteValueChecked {
  template <typename T, typename Arg>
  static enable_if_signed_integer<T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == std::numeric_limits<Arg>::min()) {
      *st = Status::Invalid("overflow");
      return arg;
    }
    return std::abs(arg);
  }

  template <typename T, typename Arg>
  static enable_if_unsigned_integer<T> Call(KernelContext* ctx, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return std::fabs(arg);
  }
};

struct Add {
  template <typename T>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return left + right;
  }

  template <typename T>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, T left, T right,
                                                      Status*) {
    return left + right;
  }

  template <typename T>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return arrow::internal::SafeSignedAdd(left, right);
  }
};

struct AddChecked {
  template <typename T, typename Arg0, typename Arg1>
  enable_if_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(AddWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left + right;
  }
};

struct Subtract {
  template <typename T>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return left - right;
  }

  template <typename T>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, T left, T right,
                                                      Status*) {
    return left - right;
  }

  template <typename T>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return arrow::internal::SafeSignedSubtract(left, right);
  }
};

struct SubtractChecked {
  template <typename T, typename Arg0, typename Arg1>
  enable_if_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(SubtractWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left - right;
  }
};

struct Multiply {
  static_assert(std::is_same<decltype(int8_t() * int8_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(uint8_t() * uint8_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(int16_t() * int16_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(uint16_t() * uint16_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(int32_t() * int32_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(uint32_t() * uint32_t()), uint32_t>::value, "");
  static_assert(std::is_same<decltype(int64_t() * int64_t()), int64_t>::value, "");
  static_assert(std::is_same<decltype(uint64_t() * uint64_t()), uint64_t>::value, "");

  template <typename T>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return left * right;
  }

  template <typename T>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, T left, T right,
                                                      Status*) {
    return left * right;
  }

  template <typename T>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return to_unsigned(left) * to_unsigned(right);
  }

  // Multiplication of 16 bit integer types implicitly promotes to signed 32 bit
  // integer. However, some inputs may nevertheless overflow (which triggers undefined
  // behaviour). Therefore we first cast to 32 bit unsigned integers where overflow is
  // well defined.
  template <typename T = void>
  static constexpr int16_t Call(KernelContext*, int16_t left, int16_t right, Status*) {
    return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
  }
  template <typename T = void>
  static constexpr uint16_t Call(KernelContext*, uint16_t left, uint16_t right, Status*) {
    return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
  }
};

struct MultiplyChecked {
  template <typename T, typename Arg0, typename Arg1>
  enable_if_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(MultiplyWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left * right;
  }
};

struct Divide {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    return left / right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    T result;
    if (ARROW_PREDICT_FALSE(DivideWithOverflow(left, right, &result))) {
      if (right == 0) {
        *st = Status::Invalid("divide by zero");
      } else {
        result = 0;
      }
    }
    return result;
  }
};

struct DivideChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result;
    if (ARROW_PREDICT_FALSE(DivideWithOverflow(left, right, &result))) {
      if (right == 0) {
        *st = Status::Invalid("divide by zero");
      } else {
        *st = Status::Invalid("overflow");
      }
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    if (ARROW_PREDICT_FALSE(right == 0)) {
      *st = Status::Invalid("divide by zero");
      return 0;
    }
    return left / right;
  }
};

struct Negate {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, Arg arg, Status*) {
    return -arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, Arg arg, Status*) {
    return ~arg + 1;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, Arg arg, Status* st) {
    return arrow::internal::SafeSignedNegate(arg);
  }
};

struct NegateChecked {
  template <typename T, typename Arg>
  static enable_if_signed_integer<T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(NegateWithOverflow(arg, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg>
  static enable_if_unsigned_integer<T> Call(KernelContext* ctx, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    DCHECK(false) << "This is included only for the purposes of instantiability from the "
                     "arithmetic kernel generator";
    return 0;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return -arg;
  }
};

struct Power {
  ARROW_NOINLINE
  static uint64_t IntegerPower(uint64_t base, uint64_t exp) {
    // right to left O(logn) power
    uint64_t pow = 1;
    while (exp) {
      pow *= (exp & 1) ? base : 1;
      base *= base;
      exp >>= 1;
    }
    return pow;
  }

  template <typename T>
  static enable_if_integer<T> Call(KernelContext*, T base, T exp, Status* st) {
    if (exp < 0) {
      *st = Status::Invalid("integers to negative integer powers are not allowed");
      return 0;
    }
    return static_cast<T>(IntegerPower(base, exp));
  }

  template <typename T>
  static enable_if_floating_point<T> Call(KernelContext*, T base, T exp, Status*) {
    return std::pow(base, exp);
  }
};

struct PowerChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer<T> Call(KernelContext*, Arg0 base, Arg1 exp, Status* st) {
    if (exp < 0) {
      *st = Status::Invalid("integers to negative integer powers are not allowed");
      return 0;
    } else if (exp == 0) {
      return 1;
    }
    // left to right O(logn) power with overflow checks
    bool overflow = false;
    uint64_t bitmask =
        1ULL << (63 - BitUtil::CountLeadingZeros(static_cast<uint64_t>(exp)));
    T pow = 1;
    while (bitmask) {
      overflow |= MultiplyWithOverflow(pow, pow, &pow);
      if (exp & bitmask) {
        overflow |= MultiplyWithOverflow(pow, base, &pow);
      }
      bitmask >>= 1;
    }
    if (overflow) {
      *st = Status::Invalid("overflow");
    }
    return pow;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 base, Arg1 exp, Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return std::pow(base, exp);
  }
};

struct Minimum {
  template <typename T>
  static enable_if_floating_point<T> Call(T left, T right) {
    return std::fmin(left, right);
  }

  template <typename T>
  static enable_if_integer<T> Call(T left, T right) {
    return std::min(left, right);
  }
};

struct Maximum {
  template <typename T>
  static enable_if_floating_point<T> Call(T left, T right) {
    return std::fmax(left, right);
  }

  template <typename T>
  static enable_if_integer<T> Call(T left, T right) {
    return std::max(left, right);
  }
};

// Generate a kernel given an arithmetic functor
template <template <typename... Args> class KernelGenerator, typename Op>
ArrayKernelExec ArithmeticExecFromOp(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return KernelGenerator<Int8Type, Int8Type, Op>::Exec;
    case Type::UINT8:
      return KernelGenerator<UInt8Type, UInt8Type, Op>::Exec;
    case Type::INT16:
      return KernelGenerator<Int16Type, Int16Type, Op>::Exec;
    case Type::UINT16:
      return KernelGenerator<UInt16Type, UInt16Type, Op>::Exec;
    case Type::INT32:
      return KernelGenerator<Int32Type, Int32Type, Op>::Exec;
    case Type::UINT32:
      return KernelGenerator<UInt32Type, UInt32Type, Op>::Exec;
    case Type::INT64:
    case Type::TIMESTAMP:
      return KernelGenerator<Int64Type, Int64Type, Op>::Exec;
    case Type::UINT64:
      return KernelGenerator<UInt64Type, UInt64Type, Op>::Exec;
    case Type::FLOAT:
      return KernelGenerator<FloatType, FloatType, Op>::Exec;
    case Type::DOUBLE:
      return KernelGenerator<DoubleType, DoubleType, Op>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

struct ArithmeticFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;

    EnsureDictionaryDecoded(values);

    // Only promote types for binary functions
    if (values->size() == 2) {
      ReplaceNullWithOtherType(values);

      if (auto type = CommonNumeric(*values)) {
        ReplaceTypes(type, values);
      }
    }

    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

struct ArithmeticVarArgsFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;

    EnsureDictionaryDecoded(values);

    if (auto type = CommonNumeric(*values)) {
      ReplaceTypes(type, values);
    }

    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

template <typename Op>
std::shared_ptr<ScalarFunction> MakeArithmeticFunction(std::string name,
                                                       const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Binary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarBinaryEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  return func;
}

// Like MakeArithmeticFunction, but for arithmetic ops that need to run
// only on non-null output.
template <typename Op>
std::shared_ptr<ScalarFunction> MakeArithmeticFunctionNotNull(std::string name,
                                                              const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Binary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarBinaryNotNullEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  return func;
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeUnaryArithmeticFunction(std::string name,
                                                            const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Unary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarUnary, Op>(ty);
    DCHECK_OK(func->AddKernel({ty}, ty, exec));
  }
  return func;
}

// Like MakeUnaryArithmeticFunction, but for arithmetic ops that need to run
// only on non-null output.
template <typename Op>
std::shared_ptr<ScalarFunction> MakeUnaryArithmeticFunctionNotNull(
    std::string name, const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Unary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarUnaryNotNull, Op>(ty);
    DCHECK_OK(func->AddKernel({ty}, ty, exec));
  }
  return func;
}

// Like MakeUnaryArithmeticFunction, but for signed arithmetic ops that need to run
// only on non-null output.
template <typename Op>
std::shared_ptr<ScalarFunction> MakeUnarySignedArithmeticFunctionNotNull(
    std::string name, const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Unary(), doc);
  for (const auto& ty : NumericTypes()) {
    if (!arrow::is_unsigned_integer(ty->id())) {
      auto exec = ArithmeticExecFromOp<ScalarUnaryNotNull, Op>(ty);
      DCHECK_OK(func->AddKernel({ty}, ty, exec));
    }
  }
  return func;
}

using MinMaxState = OptionsWrapper<ElementWiseAggregateOptions>;

// Implement a variadic scalar min/max kernel.
template <typename OutType, typename Op>
struct ScalarMinMax {
  using OutValue = typename GetOutputType<OutType>::T;

  static void ExecScalar(const ExecBatch& batch,
                         const ElementWiseAggregateOptions& options, Scalar* out) {
    // All arguments are scalar
    OutValue value{};
    bool valid = false;
    for (const auto& arg : batch.values) {
      // Ignore non-scalar arguments so we can use it in the mixed-scalar-and-array case
      if (!arg.is_scalar()) continue;
      const auto& scalar = *arg.scalar();
      if (!scalar.is_valid) {
        if (options.skip_nulls) continue;
        out->is_valid = false;
        return;
      }
      if (!valid) {
        value = UnboxScalar<OutType>::Unbox(scalar);
        valid = true;
      } else {
        value = Op::Call(value, UnboxScalar<OutType>::Unbox(scalar));
      }
    }
    out->is_valid = valid;
    if (valid) {
      BoxScalar<OutType>::Box(value, out);
    }
  }

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ElementWiseAggregateOptions& options = MinMaxState::Get(ctx);
    const auto descrs = batch.GetDescriptors();
    bool all_scalar = true;
    bool any_scalar = false;
    size_t first_array_index = batch.values.size();
    for (size_t i = 0; i < batch.values.size(); i++) {
      const auto& datum = batch.values[i];
      all_scalar &= datum.descr().shape == ValueDescr::SCALAR;
      any_scalar |= datum.descr().shape == ValueDescr::SCALAR;
      if (first_array_index >= batch.values.size() &&
          datum.descr().shape == ValueDescr::ARRAY) {
        first_array_index = i;
      }
    }
    if (all_scalar) {
      ExecScalar(batch, options, out->scalar().get());
      return Status::OK();
    }

    ArrayData* output = out->mutable_array();

    // Exactly one array (output = input)
    if (batch.values.size() == 1) {
      *output = *batch[0].array();
      return Status::OK();
    }

    // At least one array, two or more arguments
    DCHECK_GE(first_array_index, 0);
    DCHECK_LT(first_array_index, batch.values.size());
    DCHECK(batch.values[first_array_index].is_array());
    if (any_scalar) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Scalar> temp_scalar,
                            MakeScalar(out->type(), 0));
      ExecScalar(batch, options, temp_scalar.get());
      if (options.skip_nulls || temp_scalar->is_valid) {
        // Promote to output array
        ARROW_ASSIGN_OR_RAISE(auto array, MakeArrayFromScalar(*temp_scalar, batch.length,
                                                              ctx->memory_pool()));
        *output = *array->data();
        if (!temp_scalar->is_valid) {
          // MakeArrayFromScalar reuses the same buffer for null/data in
          // this case, allocate a real one since we'll be writing to it
          ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(batch.length));
          ::arrow::internal::BitmapXor(output->buffers[0]->data(), /*left_offset=*/0,
                                       output->buffers[0]->data(), /*right_offset=*/0,
                                       batch.length, /*out_offset=*/0,
                                       output->buffers[0]->mutable_data());
        }
      } else {
        // Abort early
        ARROW_ASSIGN_OR_RAISE(auto array, MakeArrayFromScalar(*temp_scalar, batch.length,
                                                              ctx->memory_pool()));
        *output = *array->data();
        return Status::OK();
      }
    } else {
      // Copy first array argument to output array
      const ArrayData& input = *batch.values[first_array_index].array();
      ARROW_ASSIGN_OR_RAISE(output->buffers[1],
                            ctx->Allocate(batch.length * sizeof(OutValue)));
      if (options.skip_nulls && input.buffers[0]) {
        // Don't copy the bitmap if !options.skip_nulls since we'll precompute it later
        ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(batch.length));
        ::arrow::internal::CopyBitmap(input.buffers[0]->data(), input.offset,
                                      batch.length, output->buffers[0]->mutable_data(),
                                      /*dest_offset=*/0);
      }
      // This won't work for nested or variable-sized types
      std::memcpy(output->buffers[1]->mutable_data(),
                  input.buffers[1]->data() + (input.offset * sizeof(OutValue)),
                  batch.length * sizeof(OutValue));
    }

    if (!options.skip_nulls) {
      // We can precompute the validity buffer in this case
      // AND together the validity buffers of all arrays
      ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(batch.length));
      bool first = true;
      for (const auto& arg : batch.values) {
        if (!arg.is_array()) continue;
        auto arr = arg.array();
        if (!arr->buffers[0]) continue;
        if (first) {
          ::arrow::internal::CopyBitmap(arr->buffers[0]->data(), arr->offset,
                                        batch.length, output->buffers[0]->mutable_data(),
                                        /*dest_offset=*/0);
          first = false;
        } else {
          ::arrow::internal::BitmapAnd(output->buffers[0]->data(), /*left_offset=*/0,
                                       arr->buffers[0]->data(), arr->offset, batch.length,
                                       /*out_offset=*/0,
                                       output->buffers[0]->mutable_data());
        }
      }
    }
    const size_t start_at = any_scalar ? first_array_index : (first_array_index + 1);
    for (size_t i = start_at; i < batch.values.size(); i++) {
      if (!batch.values[i].is_array()) continue;
      OutputArrayWriter<OutType> writer(out->mutable_array());
      const ArrayData& arr = *batch.values[i].array();
      ArrayIterator<OutType> out_it(*output);
      int64_t index = 0;
      VisitArrayValuesInline<OutType>(
          arr,
          [&](OutValue value) {
            auto u = out_it();
            if (!output->buffers[0] ||
                BitUtil::GetBit(output->buffers[0]->data(), index)) {
              writer.Write(Op::Call(u, value));
            } else {
              writer.Write(value);
            }
            index++;
          },
          [&]() {
            // RHS is null, preserve the LHS
            writer.values++;
            index++;
            out_it();
          });
      // When not skipping nulls, we pre-compute the validity buffer
      if (options.skip_nulls && output->buffers[0]) {
        if (arr.buffers[0]) {
          ::arrow::internal::BitmapOr(
              output->buffers[0]->data(), /*left_offset=*/0, arr.buffers[0]->data(),
              /*right_offset=*/arr.offset, batch.length, /*out_offset=*/0,
              output->buffers[0]->mutable_data());
        } else {
          output->buffers[0] = nullptr;
        }
      }
    }
    output->null_count = -1;
    return Status::OK();
  }
};

template <typename Op>
std::shared_ptr<ScalarFunction> MakeScalarMinMax(std::string name,
                                                 const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticVarArgsFunction>(name, Arity::VarArgs(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = GeneratePhysicalNumeric<ScalarMinMax, Op>(ty);
    ScalarKernel kernel{KernelSignature::Make({ty}, ty, /*is_varargs=*/true), exec,
                        MinMaxState::Init};
    kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::type::NO_PREALLOCATE;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  for (const auto& ty : TemporalTypes()) {
    auto exec = GeneratePhysicalNumeric<ScalarMinMax, Op>(ty);
    ScalarKernel kernel{KernelSignature::Make({ty}, ty, /*is_varargs=*/true), exec,
                        MinMaxState::Init};
    kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::type::NO_PREALLOCATE;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  return func;
}

const FunctionDoc absolute_value_doc{
    "Calculate the absolute value of the argument element-wise",
    ("Results will wrap around on integer overflow.\n"
     "Use function \"abs_checked\" if you want overflow\n"
     "to return an error."),
    {"x"}};

const FunctionDoc absolute_value_checked_doc{
    "Calculate the absolute value of the argument element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"abs\"."),
    {"x"}};

const FunctionDoc add_doc{"Add the arguments element-wise",
                          ("Results will wrap around on integer overflow.\n"
                           "Use function \"add_checked\" if you want overflow\n"
                           "to return an error."),
                          {"x", "y"}};

const FunctionDoc add_checked_doc{
    "Add the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"add\"."),
    {"x", "y"}};

const FunctionDoc sub_doc{"Subtract the arguments element-wise",
                          ("Results will wrap around on integer overflow.\n"
                           "Use function \"subtract_checked\" if you want overflow\n"
                           "to return an error."),
                          {"x", "y"}};

const FunctionDoc sub_checked_doc{
    "Subtract the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"subtract\"."),
    {"x", "y"}};

const FunctionDoc mul_doc{"Multiply the arguments element-wise",
                          ("Results will wrap around on integer overflow.\n"
                           "Use function \"multiply_checked\" if you want overflow\n"
                           "to return an error."),
                          {"x", "y"}};

const FunctionDoc mul_checked_doc{
    "Multiply the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"multiply\"."),
    {"x", "y"}};

const FunctionDoc div_doc{
    "Divide the arguments element-wise",
    ("Integer division by zero returns an error. However, integer overflow\n"
     "wraps around, and floating-point division by zero returns an infinite.\n"
     "Use function \"divide_checked\" if you want to get an error\n"
     "in all the aforementioned cases."),
    {"dividend", "divisor"}};

const FunctionDoc div_checked_doc{
    "Divide the arguments element-wise",
    ("An error is returned when trying to divide by zero, or when\n"
     "integer overflow is encountered."),
    {"dividend", "divisor"}};

const FunctionDoc negate_doc{"Negate the argument element-wise",
                             ("Results will wrap around on integer overflow.\n"
                              "Use function \"negate_checked\" if you want overflow\n"
                              "to return an error."),
                             {"x"}};

const FunctionDoc negate_checked_doc{
    "Negate the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"negate\"."),
    {"x"}};

const FunctionDoc pow_doc{
    "Raise arguments to power element-wise",
    ("Integer to negative integer power returns an error. However, integer overflow\n"
     "wraps around. If either base or exponent is null the result will be null."),
    {"base", "exponent"}};

const FunctionDoc pow_checked_doc{
    "Raise arguments to power element-wise",
    ("An error is returned when integer to negative integer power is encountered,\n"
     "or integer overflow is encountered."),
    {"base", "exponent"}};

const FunctionDoc minimum_doc{
    "Find the element-wise minimum value",
    ("Nulls will be ignored (default) or propagated. "
     "NaN will be taken over null, but not over any valid float."),
    {"*args"},
    "ElementWiseAggregateOptions"};

const FunctionDoc maximum_doc{
    "Find the element-wise maximum value",
    ("Nulls will be ignored (default) or propagated. "
     "NaN will be taken over null, but not over any valid float."),
    {"*args"},
    "ElementWiseAggregateOptions"};
}  // namespace

void RegisterScalarArithmetic(FunctionRegistry* registry) {
  // ----------------------------------------------------------------------
  auto absolute_value =
      MakeUnaryArithmeticFunction<AbsoluteValue>("abs", &absolute_value_doc);
  DCHECK_OK(registry->AddFunction(std::move(absolute_value)));

  // ----------------------------------------------------------------------
  auto absolute_value_checked = MakeUnaryArithmeticFunctionNotNull<AbsoluteValueChecked>(
      "abs_checked", &absolute_value_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(absolute_value_checked)));

  // ----------------------------------------------------------------------
  auto add = MakeArithmeticFunction<Add>("add", &add_doc);
  DCHECK_OK(registry->AddFunction(std::move(add)));

  // ----------------------------------------------------------------------
  auto add_checked =
      MakeArithmeticFunctionNotNull<AddChecked>("add_checked", &add_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(add_checked)));

  // ----------------------------------------------------------------------
  // subtract
  auto subtract = MakeArithmeticFunction<Subtract>("subtract", &sub_doc);

  // Add subtract(timestamp, timestamp) -> duration
  for (auto unit : AllTimeUnits()) {
    InputType in_type(match::TimestampTypeUnit(unit));
    auto exec = ArithmeticExecFromOp<ScalarBinaryEqualTypes, Subtract>(Type::TIMESTAMP);
    DCHECK_OK(subtract->AddKernel({in_type, in_type}, duration(unit), std::move(exec)));
  }

  DCHECK_OK(registry->AddFunction(std::move(subtract)));

  // ----------------------------------------------------------------------
  auto subtract_checked = MakeArithmeticFunctionNotNull<SubtractChecked>(
      "subtract_checked", &sub_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(subtract_checked)));

  // ----------------------------------------------------------------------
  auto multiply = MakeArithmeticFunction<Multiply>("multiply", &mul_doc);
  DCHECK_OK(registry->AddFunction(std::move(multiply)));

  // ----------------------------------------------------------------------
  auto multiply_checked = MakeArithmeticFunctionNotNull<MultiplyChecked>(
      "multiply_checked", &mul_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(multiply_checked)));

  // ----------------------------------------------------------------------
  auto divide = MakeArithmeticFunctionNotNull<Divide>("divide", &div_doc);
  DCHECK_OK(registry->AddFunction(std::move(divide)));

  // ----------------------------------------------------------------------
  auto divide_checked =
      MakeArithmeticFunctionNotNull<DivideChecked>("divide_checked", &div_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(divide_checked)));

  // ----------------------------------------------------------------------
  auto negate = MakeUnaryArithmeticFunction<Negate>("negate", &negate_doc);
  DCHECK_OK(registry->AddFunction(std::move(negate)));

  // ----------------------------------------------------------------------
  auto negate_checked = MakeUnarySignedArithmeticFunctionNotNull<NegateChecked>(
      "negate_checked", &negate_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(negate_checked)));

  // ----------------------------------------------------------------------
  auto power = MakeArithmeticFunction<Power>("power", &pow_doc);
  DCHECK_OK(registry->AddFunction(std::move(power)));

  // ----------------------------------------------------------------------
  auto power_checked =
      MakeArithmeticFunctionNotNull<PowerChecked>("power_checked", &pow_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(power_checked)));

  // ----------------------------------------------------------------------
  auto minimum = MakeScalarMinMax<Minimum>("minimum", &minimum_doc);
  DCHECK_OK(registry->AddFunction(std::move(minimum)));

  auto maximum = MakeScalarMinMax<Maximum>("maximum", &maximum_doc);
  DCHECK_OK(registry->AddFunction(std::move(maximum)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
