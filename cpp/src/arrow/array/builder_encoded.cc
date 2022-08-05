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

#include "arrow/array/builder_encoded.h"
#include "arrow/array/builder_primitive.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "arrow/scalar.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle_util.h"

namespace arrow {

// ----------------------------------------------------------------------
// RunLengthEncodedBuilder

RunLengthEncodedBuilder::RunLengthEncodedBuilder(
    MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& run_end_builder,
    const std::shared_ptr<ArrayBuilder>& value_builder, std::shared_ptr<DataType> type)
    : ArrayBuilder(pool),
      type_(internal::checked_pointer_cast<RunLengthEncodedType>(type)) {
  children_ = {std::move(run_end_builder), std::move(value_builder)};
}

Status RunLengthEncodedBuilder::Resize(int64_t) {
  return Status::NotImplemented(
      "Resizing an RLE for a given number of logical elements is not possible, since the "
      "physical length will vary depending on the contents. To allocate memory for a "
      "certain number of runs, use ResizePhysical.");
}

void RunLengthEncodedBuilder::Reset() {
  capacity_ = length_ = null_count_ = 0;
  value_builder().Reset();
  run_end_builder().Reset();
  current_value_.reset();
  run_start_ = 0;
}

Status RunLengthEncodedBuilder::AppendNull() { return AppendNulls(1); }

Status RunLengthEncodedBuilder::AppendNulls(int64_t length) {
  if (length == 0) {
    return Status::OK();
  }
  if (current_value_) {
    RETURN_NOT_OK(FinishRun());
    current_value_ = {};
  }
  return AddLength(length);
}

Status RunLengthEncodedBuilder::AppendEmptyValue() { return AppendNull(); }

Status RunLengthEncodedBuilder::AppendEmptyValues(int64_t length) {
  return AppendNulls(length);
}

Status RunLengthEncodedBuilder::AppendScalar(const Scalar& scalar, int64_t n_repeats) {
  if (n_repeats == 0) {
    return Status::OK();
  }
  if (!current_value_ || !current_value_->Equals(scalar)) {
    RETURN_NOT_OK(FinishRun());
    current_value_ = scalar.shared_from_this();
  }
  return AddLength(n_repeats);
}

Status RunLengthEncodedBuilder::AppendScalars(const ScalarVector& scalars) {
  for (auto scalar : scalars) {
    RETURN_NOT_OK(AppendScalar(*scalar, 1));
  }
  return Status::OK();
}

Status RunLengthEncodedBuilder::AppendArraySlice(const ArraySpan& array, int64_t offset,
                                                 int64_t length) {
  // Finish eventual runs started using AppendScalars() and others before. We don't
  // attempt to merge them with the runs from the appended array slice for now
  RETURN_NOT_OK(FinishRun());

  ARROW_DCHECK(offset + length < array.length);
  ARROW_DCHECK(array.type->Equals(type_));
  // Create a slice of the slice for the part we actually want to add
  ArraySpan to_append = array;
  to_append.SetSlice(array.offset + offset, length);
  const int64_t physical_offset = rle_util::GetPhysicalOffset(to_append);
  int64_t physical_length = 0;

  // TODO: single visit and RETURN_NOT_OK
  rle_util::VisitMergedRuns(
      to_append, to_append,
      [this, &physical_length](int64_t run_length, int64_t, int64_t) {
        physical_length++;
        length_ += run_length;
        assert(run_end_builder().Append(length_).ok());
      });
  RETURN_NOT_OK(value_builder().AppendArraySlice(rle_util::DataArray(to_append),
                                                 physical_offset, physical_length));
  // next run is not merged either
  run_start_ = length_;
  return Status::OK();
}

std::shared_ptr<DataType> RunLengthEncodedBuilder::type() const { return type_; }

Status RunLengthEncodedBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  ARROW_ASSIGN_OR_RAISE(auto run_ends_array, run_end_builder().Finish());
  ARROW_ASSIGN_OR_RAISE(auto values_array, value_builder().Finish());
  ARROW_ASSIGN_OR_RAISE(
      auto rle_array, RunLengthEncodedArray::Make(run_ends_array, values_array, length_));
  *out = std::move(rle_array->data());
  return Status::OK();
}

Status RunLengthEncodedBuilder::FinishRun() {
  if (length_ - run_start_ == 0) {
    return Status::OK();
  }
  if (current_value_) {
    RETURN_NOT_OK(value_builder().AppendScalar(*current_value_));
  } else {
    RETURN_NOT_OK(value_builder().AppendNull());
  }
  RETURN_NOT_OK(run_end_builder().Append(length_));
  current_value_.reset();
  run_start_ = 0;
  return Status::OK();
}

Status RunLengthEncodedBuilder::ResizePhyiscal(int64_t capacity) {
  RETURN_NOT_OK(value_builder().Resize(capacity));
  return run_end_builder().Resize(capacity);
}

Status RunLengthEncodedBuilder::AddLength(int64_t added_length) {
  if (ARROW_PREDICT_FALSE(added_length < 0)) {
    return Status::Invalid("Added length must be positive (requested: ", added_length,
                           ")");
  }
  if (ARROW_PREDICT_FALSE(added_length > std::numeric_limits<int32_t>::max() ||
                          length_ + added_length > std::numeric_limits<int32_t>::max())) {
    return Status::Invalid(
        "Run-length encoded array length must fit in a 32-bit signed integer (adding ",
        added_length, " items to array of length ", length_, ")");
  }

  length_ += added_length;
  return Status::OK();
}

Int32Builder& RunLengthEncodedBuilder::run_end_builder() {
  return internal::checked_cast<Int32Builder&>(*children_[0]);
}

ArrayBuilder& RunLengthEncodedBuilder::value_builder() { return *children_[1]; };

}  // namespace arrow
