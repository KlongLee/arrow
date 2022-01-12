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

#include "arrow/compute/api.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {

namespace compute {
namespace internal {

namespace {

template <typename AllocateMem>
struct VectorMisbehaveExec {
    static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
      // allocate new buffers even though we've promised not to
      ARROW_ASSIGN_OR_RAISE(out->mutable_array()->buffers[0], ctx->AllocateBitmap(8));
      ARROW_ASSIGN_OR_RAISE(out->mutable_array()->buffers[1], ctx->Allocate(64));
      return Status::OK();
    }
};

void AddVectorMisbehaveKernels(const std::shared_ptr<VectorFunction>& Vector_function) {
  VectorKernel kernel({int32()}, int32(),
                      VectorMisbehaveExec</*AllocateMem=*/std::false_type>::Exec);
  kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::PREALLOCATE;
  kernel.can_write_into_slices = true;
  kernel.can_execute_chunkwise = false;
  kernel.output_chunked = false;

  DCHECK_OK(Vector_function->AddKernel(std::move(kernel)));
}

const FunctionDoc misbehave_doc{
        "Test kernel that does nothing but allocate memory "
        "while it shouldn't",
        "This Kernel only exists for testing purposes.\n"
        "It allocates memory while it promised not to \n"
        "(because of MemAllocation::PREALLOCATE).",
        {}};
}  // namespace

}  // namespace internal
using arrow::compute::internal::AddVectorMisbehaveKernels;
std::shared_ptr<const VectorFunction> CreateVectorMisbehaveFunction() {
  auto func = std::make_shared<VectorFunction>("vector_misbehave", Arity::Unary(),
                                               &misbehave_doc);
  AddVectorMisbehaveKernels(func);
  return func;
}
}  // namespace compute
}  // namespace arrow
