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
#include <cassert>
#include <cstdint>

#include "arrow/array/data.h"
#include "arrow/type_traits.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace ree_util {

/// \brief Get the child array holding the run ends from an REE array
inline const ArraySpan& RunEndsArray(const ArraySpan& span) { return span.child_data[0]; }

/// \brief Get the child array holding the data values from an REE array
inline const ArraySpan& ValuesArray(const ArraySpan& span) { return span.child_data[1]; }

/// \brief Get a pointer to run ends values of an REE array
template <typename RunEndsType>
const RunEndsType* RunEnds(const ArraySpan& span) {
  assert(RunEndsArray(span).type->id() == CTypeTraits<RunEndsType>::ArrowType::type_id);
  return RunEndsArray(span).GetValues<RunEndsType>(1);
}

namespace internal {

/// \brief Uses binary-search to find the physical offset given a logical offset
/// and run-end values
///
/// \return the physical offset or run_ends_size if the physical offset is not
/// found in run_ends
template <typename RunEndsType>
int64_t FindPhysicalIndex(const RunEndsType* run_ends, int64_t run_ends_size, int64_t i,
                          int64_t absolute_offset) {
  auto it = std::upper_bound(run_ends, run_ends + run_ends_size, absolute_offset + i);
  int64_t result = std::distance(run_ends, it);
  assert(result <= run_ends_size);
  return result;
}

/// \brief Uses binary-search to calculate the number of physical values (and
/// run-ends) necessary to represent the logical range of values from
/// offset to length
template <typename RunEndsType>
int64_t FindPhysicalLength(int64_t length, int64_t offset, const RunEndsType* run_ends,
                           int64_t run_ends_size) {
  // The physical length is calculated by finding the offset of the last element
  // and adding 1 to it, so first we ensure there is at least one element.
  if (length == 0) {
    return 0;
  }
  const int64_t physical_offset =
      FindPhysicalIndex<RunEndsType>(run_ends, run_ends_size, 0, offset);
  const int64_t physical_index_of_last = FindPhysicalIndex<RunEndsType>(
      run_ends + physical_offset, run_ends_size - physical_offset, length - 1, offset);

  assert(physical_index_of_last < run_ends_size - physical_offset);
  return physical_index_of_last + 1;
}

/// \brief Find the physical index into the values array of the REE ArraySpan
///
/// This function uses binary-search, so it has a O(log N) cost.
template <typename RunEndsType>
int64_t FindPhysicalIndex(const ArraySpan& span, int64_t i, int64_t absolute_offset) {
  const int64_t run_ends_size = RunEndsArray(span).length;
  return FindPhysicalIndex(RunEnds<RunEndsType>(span), run_ends_size, i, absolute_offset);
}

/// \brief Find the physical length of an REE ArraySpan
///
/// The physical length of an REE is the number of physical values (and
/// run-ends) necessary to represent the logical range of values from
/// offset to length.
///
/// Avoid calling this function if the physical length can be estabilished in
/// some other way (e.g. when iterating over the runs sequentially until the
/// end). This function uses binary-search, so it has a O(log N) cost.
template <typename RunEndsType>
int64_t FindPhysicalLength(const ArraySpan& span) {
  return FindPhysicalLength(
      /*logical_length=*/span.length,
      /*logical_offset=*/span.offset,
      /*run_ends=*/RunEnds<RunEndsType>(span),
      /*run_ends_size=*/RunEndsArray(span).length);
}

}  // namespace internal

/// \brief Find the physical index into the values array of the REE ArraySpan
///
/// This function uses binary-search, so it has a O(log N) cost.
int64_t FindPhysicalIndex(const ArraySpan& span, int64_t i, int64_t absolute_offset);

/// \brief Find the physical length of an REE ArraySpan
///
/// The physical length of an REE is the number of physical values (and
/// run-ends) necessary to represent the logical range of values from
/// offset to length.
///
/// Avoid calling this function if the physical length can be estabilished in
/// some other way (e.g. when iterating over the runs sequentially until the
/// end). This function uses binary-search, so it has a O(log N) cost.
int64_t FindPhysicalLength(const ArraySpan& span);

/// \brief Iterate over two run-end encoded arrays in segments of runs that are inside
/// run boundaries in each input
template <typename... RunEndsTypes>
class MergedRunsIterator {
 public:
  static constexpr size_t NUM_INPUTS = sizeof...(RunEndsTypes);
  template <typename... InputTypes>
  explicit MergedRunsIterator(InputTypes&... array_spans) : inputs(array_spans...) {
    static_assert(sizeof...(InputTypes) == sizeof...(RunEndsTypes),
                  "number of run ends types and input ArraySpans must be the same");
    if constexpr (NUM_INPUTS == 0) {
      // end interator
      logical_length_ = 0;
    } else {
      logical_length_ = FindCommonLength();
      if (!isEnd()) {
        FindMergedRun();
      }
    }
  }

  /*explicit MergedRunsIterator(ArraySpan array_span) : inputs(Input<int32_t>(array_span))
  {
    //static_assert(sizeof...(InputTypes) == sizeof...(RunEndsTypes), "number of run ends
  types and input ArraySpans must be the same"); if constexpr (NUM_INPUTS == 0) {
      // end interator
      logical_length_ = 0;
    } else {
      logical_length_ = FindCommonLength();
      if (!isEnd()) {
        FindMergedRun();
      }
    }
  }
  explicit MergedRunsIterator(ArraySpan array_span_a, ArraySpan array_span_b) :
  inputs(Input<int32_t>(array_span_a), Input<int32_t>(array_span_b)) {
    //static_assert(sizeof...(InputTypes) == sizeof...(RunEndsTypes), "number of run ends
  types and input ArraySpans must be the same"); if constexpr (NUM_INPUTS == 0) {
      // end interator
      logical_length_ = 0;
    } else {
      logical_length_ = FindCommonLength();
      if (!isEnd()) {
        FindMergedRun();
      }
    }
  }*/

  MergedRunsIterator(const MergedRunsIterator& other) = default;

  MergedRunsIterator& operator++() {
    logical_position_ = merged_run_end_;
    IncrementInputs();
    if (!isEnd()) {
      FindMergedRun();
    }
    return *this;
  }

  MergedRunsIterator& operator++(int) {
    MergedRunsIterator prev = *this;
    ++(*this);
    return prev;
  }

  template <typename... OthersInputs>
  bool operator==(const MergedRunsIterator<OthersInputs...>& other) const {
    return (isEnd() && other.isEnd()) ||
           (!isEnd() && !other.isEnd() && logical_position_ == other.logical_position());
  }

  template <typename... OthersInputs>
  bool operator!=(const MergedRunsIterator<OthersInputs...>& other) const {
    return !(*this == other);
  }

  /// \brief returns a physical index into the values array buffers of a given input,
  /// pointing to the value of the current run. The index includes the array offset, so it
  /// can be used to access a buffer directly
  template <size_t input_id>
  int64_t index_into_buffer() const {
    auto& input = std::get<input_id>(inputs);
    return input.run_index + ValuesArray(input.array_span).offset;
  }
  /// \brief returns a physical index into the values array of a given input, pointing to
  /// the value of the current run
  template <size_t input_id>
  int64_t index_into_array() const {
    return std::get<input_id>(inputs).run_index;
  }
  /// \brief returns the logical length of the current run
  int64_t run_length() const { return merged_run_end_ - logical_position_; }
  /// \brief returns the accumulated length of all runs from the beginning of the array
  /// including the current one
  int64_t accumulated_run_length() const { return merged_run_end_; }

  bool isEnd() const { return logical_position_ == logical_length_; }
  int64_t logical_position() const { return logical_position_; }

 private:
  template <typename RunEndsType>
  struct Input {
    explicit Input(const ArraySpan& array_span) : array_span{array_span} {
      run_ends = RunEnds<RunEndsType>(array_span);
      run_index = ree_util::internal::FindPhysicalOffset(
          run_ends, RunEndsArray(array_span).length, array_span.offset);
      // actual value found later by FindMergedRun:
      current_run_end = 0;
    }

    const ArraySpan& array_span;
    const RunEndsType* run_ends;
    int64_t run_index;
    int64_t current_run_end;
  };

  template <size_t input_id = 0>
  void FindMergedRun() {
    if constexpr (input_id == 0) {
      merged_run_end_ = std::numeric_limits<int64_t>::max();
    }
    auto& input = std::get<input_id>(inputs);
    // logical indices of the end of the run we are currently in each input
    input.current_run_end = input.run_ends[input.run_index] - input.array_span.offset;
    // the logical length may end in the middle of a run, in case the array was sliced
    input.current_run_end = std::min(input.current_run_end, logical_length_);
    assert(input.current_run_end > logical_position_);
    merged_run_end_ = std::min(merged_run_end_, input.current_run_end);
    if constexpr (input_id < NUM_INPUTS - 1) {
      FindMergedRun<input_id + 1>();
    }
  }

  template <size_t input_id = 0>
  int64_t FindCommonLength() {
    int64_t our_length = std::get<input_id>(inputs).array_span.length;
    if constexpr (input_id < NUM_INPUTS - 1) {
      [[maybe_unused]] int64_t other_length = FindCommonLength<input_id + 1>();
      assert(our_length == other_length &&
             "MergedRunsIteratror can only be used on arrays of the same length");
    }
    return our_length;
  }

  template <size_t input_id = 0>
  void IncrementInputs() {
    auto& input = std::get<input_id>(inputs);
    if (logical_position_ == input.current_run_end) {
      input.run_index++;
    }
    if constexpr (input_id < NUM_INPUTS - 1) {
      IncrementInputs<input_id + 1>();
    }
  }

  std::tuple<Input<RunEndsTypes>...> inputs;
  int64_t logical_position_ = 0;
  int64_t logical_length_ = 0;
  int64_t merged_run_end_ = 0;
};

}  // namespace ree_util
}  // namespace arrow
