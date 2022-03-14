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

#include <cstdint>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

struct ChunkLocation {
  int64_t chunk_index, index_in_chunk;
};

/// \class ChunkResolver
/// \brief An object that resolves an array chunk depending on the index
struct ChunkResolver {
  /// \brief Construct a ChunkResolver from a vector of Arrays
  explicit ChunkResolver(const ArrayVector& chunks);

  /// \brief Construct a ChunkResolver from a vector of C-style Array pointers
  explicit ChunkResolver(const std::vector<const Array*>& chunks);

  /// \brief Construct a ChunkResolver from a vector of RecordBatches
  explicit ChunkResolver(const RecordBatchVector& batches);

  /// \brief Return a ChunkLocation containing the chunk index and in-chunk value index of
  /// the chunked array at logical index
  inline ChunkLocation Resolve(const int64_t index) const {
    // It is common for the algorithms below to make consecutive accesses at
    // a relatively small distance from each other, hence often falling in
    // the same chunk.
    // This is trivial when merging (assuming each side of the merge uses
    // its own resolver), but also in the inner recursive invocations of
    // partitioning.
    if (offsets_.size() <= 1) {
      return {0, index};
    }
    const bool cache_hit =
        (index >= offsets_[cached_chunk_] && index < offsets_[cached_chunk_ + 1]);
    if (ARROW_PREDICT_TRUE(cache_hit)) {
      return {cached_chunk_, index - offsets_[cached_chunk_]};
    }
    auto chunk_index = Bisect(index);
    cached_chunk_ = chunk_index;
    return {cached_chunk_, index - offsets_[cached_chunk_]};
  }

 protected:
  /// \brief Find the chunk index corresponding to a value index using binary search
  /// (bisect) algorithm
  inline int64_t Bisect(const int64_t index) const {
    // Like std::upper_bound(), but hand-written as it can help the compiler.
    // Search [lo, lo + n)
    int64_t lo = 0;
    auto n = static_cast<int64_t>(offsets_.size());
    while (n > 1) {
      const int64_t m = n >> 1;
      const int64_t mid = lo + m;
      if (static_cast<int64_t>(index) >= offsets_[mid]) {
        lo = mid;
        n -= m;
      } else {
        n = m;
      }
    }
    return lo;
  }

 private:
  const std::vector<int64_t> offsets_;
  mutable int64_t cached_chunk_ = 0;
};

}  // namespace internal
}  // namespace arrow
