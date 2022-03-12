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
#include <utility>
#include <vector>

#include "arrow/type_fwd.h"

namespace arrow {
namespace internal {

struct ChunkLocation {
  int64_t chunk_index, index_in_chunk;
};

// An object that resolves an array chunk depending on the index.
struct ChunkResolver {
  explicit ChunkResolver(std::vector<int64_t> lengths)
      : num_chunks_(static_cast<int64_t>(lengths.size())),
        offsets_(MakeEndOffsets(std::move(lengths))) {}

  explicit ChunkResolver(const ArrayVector& chunks);

  explicit ChunkResolver(const RecordBatchVector& batches);

  inline ChunkLocation Resolve(int64_t index) const {
    // It is common for the algorithms below to make consecutive accesses at
    // a relatively small distance from each other, hence often falling in
    // the same chunk.
    // This is trivial when merging (assuming each side of the merge uses
    // its own resolver), but also in the inner recursive invocations of
    // partitioning.
    const bool cache_hit =
        (index >= offsets_[cached_chunk_] && index < offsets_[cached_chunk_ + 1]);
    if (ARROW_PREDICT_TRUE(cache_hit)) {
      return {cached_chunk_, index - offsets_[cached_chunk_]};
    } else {
      return ResolveMissBisect(index);
    }
  }

 protected:
  inline ChunkLocation ResolveMissBisect(int64_t index) const {
    // Like std::upper_bound(), but hand-written as it can help the compiler.
    const int64_t* raw_offsets = offsets_.data();
    // Search [lo, lo + n)
    int64_t lo = 0, n = num_chunks_;
    while (n > 1) {
      int64_t m = n >> 1;
      int64_t mid = lo + m;
      if (index >= raw_offsets[mid]) {
        lo = mid;
        n -= m;
      } else {
        n = m;
      }
    }
    cached_chunk_ = lo;
    return {lo, index - offsets_[lo]};
  }

  static inline std::vector<int64_t> MakeEndOffsets(std::vector<int64_t> lengths) {
    int64_t offset = 0;
    for (auto& v : lengths) {
      const auto this_length = v;
      v = offset;
      offset += this_length;
    }
    lengths.push_back(offset);
    return lengths;
  }

  int64_t num_chunks_;
  std::vector<int64_t> offsets_;
  mutable int64_t cached_chunk_ = 0;
};

}  // namespace internal
}  // namespace arrow
