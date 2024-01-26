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

#include <atomic>
#include <cassert>
#include <cstdint>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"

namespace arrow::internal {

struct ChunkLocation {
  /// \brief Index of the chunk in the array of chunks
  ///
  /// The value is always in the range `[0, chunks.size()]`. `chunks.size()` is used
  /// to represent out-of-bounds locations.
  int64_t chunk_index;

  /// \brief Index of the value in the chunk
  ///
  /// The value is undefined if chunk_index >= chunks.size()
  int64_t index_in_chunk;
};

/// \brief An utility that incrementally resolves logical indices into
/// physical indices in a chunked array.
struct ARROW_EXPORT ChunkResolver {
 private:
  /// \brief Array containing `chunks.size() + 1` offsets.
  ///
  /// `offsets_[i]` is the starting logical index of chunk `i`. `offsets_[0]` is always 0
  /// and `offsets_[chunks.size()]` is the logical length of the chunked array.
  std::vector<int64_t> offsets_;

  /// \brief Cache of the index of the last resolved chunk.
  ///
  /// \invariant `cached_chunk_ in [0, chunks.size()]`
  mutable std::atomic<int64_t> cached_chunk_;

 public:
  explicit ChunkResolver(const ArrayVector& chunks);
  explicit ChunkResolver(const std::vector<const Array*>& chunks);
  explicit ChunkResolver(const RecordBatchVector& batches);

  ChunkResolver(ChunkResolver&& other) noexcept
      : offsets_(std::move(other.offsets_)),
        cached_chunk_(other.cached_chunk_.load(std::memory_order_relaxed)) {}

  ChunkResolver& operator=(ChunkResolver&& other) {
    offsets_ = std::move(other.offsets_);
    cached_chunk_.store(other.cached_chunk_.load(std::memory_order_relaxed));
    return *this;
  }

  /// \brief Resolve a logical index to a ChunkLocation.
  ///
  /// The returned ChunkLocation contains the chunk index and the within-chunk index
  /// equivalent to the logical index.
  ///
  /// \pre index >= 0
  /// \post location.chunk_index in [0, chunks.size()]
  /// \param index The logical index to resolve
  /// \return ChunkLocation with a valid chunk_index if index is within
  ///         bounds, or with chunk_index == chunks.size() if logical index is
  ///         `>= chunked_array.length()`.
  inline ChunkLocation Resolve(const int64_t index) const {
    // It is common for algorithms sequentially processing arrays to make consecutive
    // accesses at a relatively small distance from each other, hence often falling in the
    // same chunk.
    //
    // This is guaranteed when merging (assuming each side of the merge uses its
    // own resolver), and is the most common case in recursive invocations of
    // partitioning.
    const auto cached_chunk = cached_chunk_.load(std::memory_order_relaxed);
    const auto num_offsets = static_cast<int64_t>(offsets_.size());
    if (num_offsets <= 1) {
      return {0, index};
    }
    const int64_t* offsets = offsets_.data();
    if (ARROW_PREDICT_TRUE(index >= offsets[cached_chunk]) &&
        (cached_chunk + 1 == num_offsets || index < offsets[cached_chunk + 1])) {
      return {cached_chunk, index - offsets[cached_chunk]};
    }
    const auto chunk_index = Bisect(index, offsets, /*lo=*/0, /*hi=*/num_offsets);
    assert(chunk_index < static_cast<int64_t>(offsets_.size()));
    cached_chunk_.store(chunk_index, std::memory_order_relaxed);
    return {chunk_index, index - offsets[chunk_index]};
  }

 private:
  /// \brief Find the index of the chunk that contains the logical index.
  ///
  /// Any non-negative index is accepted. When `hi=num_offsets`, the largest
  /// possible return value is `num_offsets-1` which is equal to
  /// `chunks.size()`. The is returned when the logical index is out-of-bounds.
  ///
  /// \pre index >= 0
  /// \pre lo >= 0 && hi <= offsets_.size()
  /// \return `lo` if `lo == hi` or otherwise a chunk index in `[lo, hi)`.
  static inline int64_t Bisect(int64_t index, const int64_t* offsets, int64_t lo,
                               int64_t hi) {
    // Like std::upper_bound(), but hand-written as it can help the compiler.
    auto n = hi - lo;
    while (n > 1) {
      const int64_t m = n >> 1;
      const int64_t mid = lo + m;
      if (index >= offsets[mid]) {
        lo = mid;
        n -= m;
      } else {
        n = m;
      }
    }
    return lo;
  }
};

}  // namespace arrow::internal
