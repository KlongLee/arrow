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

#include "arrow/compute/function.h"

#include <cstddef>
#include <memory>
#include <sstream>

#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/datum.h"
#include "arrow/util/cpu_info.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

static const FunctionDoc kEmptyFunctionDoc{};

const FunctionDoc& FunctionDoc::Empty() { return kEmptyFunctionDoc; }

Status CheckArityImpl(const Function* function, int passed_num_args,
                      const char* passed_num_args_label) {
  if (function->arity().is_varargs && passed_num_args < function->arity().num_args) {
    return Status::Invalid("VarArgs function needs at least ", function->arity().num_args,
                           " arguments but ", passed_num_args_label, " only ",
                           passed_num_args);
  }

  if (!function->arity().is_varargs && passed_num_args != function->arity().num_args) {
    return Status::Invalid("Function ", function->name(), " accepts ",
                           function->arity().num_args, " arguments but ",
                           passed_num_args_label, " ", passed_num_args);
  }

  return Status::OK();
}

Status Function::CheckArity(const std::vector<InputType>& in_types) const {
  return CheckArityImpl(this, static_cast<int>(in_types.size()), "kernel accepts");
}

Status Function::CheckArity(const std::vector<ValueDescr>& descrs) const {
  return CheckArityImpl(this, static_cast<int>(descrs.size()),
                        "attempted to look up kernel(s) with");
}

namespace detail {

Status NoMatchingKernel(const Function* func, const std::vector<ValueDescr>& descrs) {
  return Status::NotImplemented("Function ", func->name(),
                                " has no kernel matching input types ",
                                ValueDescr::ToString(descrs));
}

template <typename KernelType>
const KernelType* DispatchExactImpl(const std::vector<KernelType*>& kernels,
                                    const std::vector<ValueDescr>& values) {
  const KernelType* kernel_matches[SimdLevel::MAX] = {nullptr};

  // Validate arity
  for (const auto& kernel : kernels) {
    if (kernel->signature->MatchesInputs(values)) {
      kernel_matches[kernel->simd_level] = kernel;
    }
  }

  // Dispatch as the CPU feature
#if defined(ARROW_HAVE_RUNTIME_AVX512) || defined(ARROW_HAVE_RUNTIME_AVX2)
  auto cpu_info = arrow::internal::CpuInfo::GetInstance();
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    if (kernel_matches[SimdLevel::AVX512]) {
      return kernel_matches[SimdLevel::AVX512];
    }
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    if (kernel_matches[SimdLevel::AVX2]) {
      return kernel_matches[SimdLevel::AVX2];
    }
  }
#endif
  if (kernel_matches[SimdLevel::NONE]) {
    return kernel_matches[SimdLevel::NONE];
  }

  return nullptr;
}

const Kernel* DispatchExactImpl(const Function* func,
                                const std::vector<ValueDescr>& values) {
  if (func->kind() == Function::SCALAR) {
    return DispatchExactImpl(checked_cast<const ScalarFunction*>(func)->kernels(),
                             values);
  }

  if (func->kind() == Function::VECTOR) {
    return DispatchExactImpl(checked_cast<const VectorFunction*>(func)->kernels(),
                             values);
  }

  if (func->kind() == Function::SCALAR_AGGREGATE) {
    return DispatchExactImpl(
        checked_cast<const ScalarAggregateFunction*>(func)->kernels(), values);
  }

  return nullptr;
}

}  // namespace detail

Result<const Kernel*> Function::DispatchExact(
    const std::vector<ValueDescr>& values) const {
  if (kind_ == Function::META) {
    return Status::NotImplemented("Dispatch for a MetaFunction's Kernels");
  }
  RETURN_NOT_OK(CheckArity(values));

  if (auto kernel = detail::DispatchExactImpl(this, values)) {
    return kernel;
  }
  return detail::NoMatchingKernel(this, values);
}

Result<const Kernel*> Function::DispatchBest(std::vector<ValueDescr>* values) const {
  if (kind_ == Function::META) {
    return Status::NotImplemented("Dispatch for a MetaFunction's Kernels");
  }
  RETURN_NOT_OK(CheckArity(*values));

  // first try for an exact match
  if (auto kernel = detail::DispatchExactImpl(this, *values)) {
    return kernel;
  }

  // XXX permit generic conversions here, for example dict -> decoded, null -> any?
  return DispatchExact(*values);
}

Result<Datum> Function::Execute(const std::vector<Datum>& original_args,
                                const FunctionOptions* options, ExecContext* ctx) const {
  if (options == nullptr) {
    options = default_options();
  }
  if (ctx == nullptr) {
    ExecContext default_ctx;
    return Execute(original_args, options, &default_ctx);
  }
  // make a local copy to accommodate implicit casts
  auto args = original_args;

  // type-check Datum arguments here. Really we'd like to avoid this as much as
  // possible
  RETURN_NOT_OK(detail::CheckAllValues(args));
  std::vector<ValueDescr> inputs(args.size());
  for (size_t i = 0; i != args.size(); ++i) {
    inputs[i] = args[i].descr();
  }

  ARROW_ASSIGN_OR_RAISE(auto kernel, DispatchBest(&inputs));
  for (size_t i = 0; i != args.size(); ++i) {
    if (inputs[i] != args[i].descr()) {
      if (inputs[i].shape != args[i].shape()) {
        return Status::NotImplemented(
            "Automatic broadcasting of scalars to arrays for function ", name());
      }

      ARROW_ASSIGN_OR_RAISE(args[i], Cast(args[i], inputs[i].type));
    }
  }

  std::unique_ptr<KernelState> state;

  KernelContext kernel_ctx{ctx};
  if (kernel->init) {
    state = kernel->init(&kernel_ctx, {kernel, inputs, options});
    RETURN_NOT_OK(kernel_ctx.status());
    kernel_ctx.SetState(state.get());
  }

  std::unique_ptr<detail::KernelExecutor> executor;
  if (kind() == Function::SCALAR) {
    executor = detail::KernelExecutor::MakeScalar();
  } else if (kind() == Function::VECTOR) {
    executor = detail::KernelExecutor::MakeVector();
  } else {
    executor = detail::KernelExecutor::MakeScalarAggregate();
  }
  RETURN_NOT_OK(executor->Init(&kernel_ctx, {kernel, inputs, options}));

  auto listener = std::make_shared<detail::DatumAccumulator>();
  RETURN_NOT_OK(executor->Execute(args, listener.get()));
  return executor->WrapResults(args, listener->values());
}

Status Function::Validate() const {
  if (!doc_->summary.empty()) {
    // Documentation given, check its contents
    int arg_count = static_cast<int>(doc_->arg_names.size());
    if (arg_count == arity_.num_args) {
      return Status::OK();
    }
    if (arity_.is_varargs && arg_count == arity_.num_args + 1) {
      return Status::OK();
    }
    return Status::Invalid("In function '", name_,
                           "': ", "number of argument names != function arity");
  }
  return Status::OK();
}

Status ScalarFunction::AddKernel(std::vector<InputType> in_types, OutputType out_type,
                                 ArrayKernelExec exec, KernelInit init) {
  RETURN_NOT_OK(CheckArity(in_types));

  if (arity_.is_varargs && in_types.size() != 1) {
    return Status::Invalid("VarArgs signatures must have exactly one input type");
  }
  auto sig =
      KernelSignature::Make(std::move(in_types), std::move(out_type), arity_.is_varargs);
  kernels_.emplace_back(std::move(sig), exec, init);
  return Status::OK();
}

Status ScalarFunction::AddKernel(ScalarKernel kernel) {
  RETURN_NOT_OK(CheckArity(kernel.signature->in_types()));
  if (arity_.is_varargs && !kernel.signature->is_varargs()) {
    return Status::Invalid("Function accepts varargs but kernel signature does not");
  }
  kernels_.emplace_back(std::move(kernel));
  return Status::OK();
}

Status VectorFunction::AddKernel(std::vector<InputType> in_types, OutputType out_type,
                                 ArrayKernelExec exec, KernelInit init) {
  RETURN_NOT_OK(CheckArity(in_types));

  if (arity_.is_varargs && in_types.size() != 1) {
    return Status::Invalid("VarArgs signatures must have exactly one input type");
  }
  auto sig =
      KernelSignature::Make(std::move(in_types), std::move(out_type), arity_.is_varargs);
  kernels_.emplace_back(std::move(sig), exec, init);
  return Status::OK();
}

Status VectorFunction::AddKernel(VectorKernel kernel) {
  RETURN_NOT_OK(CheckArity(kernel.signature->in_types()));
  if (arity_.is_varargs && !kernel.signature->is_varargs()) {
    return Status::Invalid("Function accepts varargs but kernel signature does not");
  }
  kernels_.emplace_back(std::move(kernel));
  return Status::OK();
}

Status ScalarAggregateFunction::AddKernel(ScalarAggregateKernel kernel) {
  RETURN_NOT_OK(CheckArity(kernel.signature->in_types()));
  if (arity_.is_varargs && !kernel.signature->is_varargs()) {
    return Status::Invalid("Function accepts varargs but kernel signature does not");
  }
  kernels_.emplace_back(std::move(kernel));
  return Status::OK();
}

Result<Datum> MetaFunction::Execute(const std::vector<Datum>& args,
                                    const FunctionOptions* options,
                                    ExecContext* ctx) const {
  RETURN_NOT_OK(
      CheckArityImpl(this, static_cast<int>(args.size()), "attempted to Execute with"));

  if (options == nullptr) {
    options = default_options();
  }
  return ExecuteImpl(args, options, ctx);
}

}  // namespace compute
}  // namespace arrow
