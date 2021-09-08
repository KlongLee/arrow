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

#include "benchmark/benchmark.h"

#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {
constexpr auto kSeed = 0x0ff1ce;

Result<std::shared_ptr<Array>> TopKWithSorting(const Array& values, int64_t n) {
  ARROW_ASSIGN_OR_RAISE(auto sorted, SortIndices(values, SortOrder::Descending));
  auto head_k_indices = sorted->Slice(0, n);
  return head_k_indices;
}

static void TopKBenchmark(benchmark::State& state, const std::shared_ptr<Array>& values,
                          int64_t k) {
  for (auto _ : state) {
    ABORT_NOT_OK(TopK(*values, TopKOptions(k)).status());
  }
  state.SetItemsProcessed(state.iterations() * values->length());
}

static void TopKInt64(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);

  auto min = std::numeric_limits<int64_t>::min();
  auto max = std::numeric_limits<int64_t>::max();
  auto values = rand.Int64(array_size, min, max, args.null_proportion);

  TopKBenchmark(state, values, array_size / 8);
}

BENCHMARK(TopKInt64)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 100})
    ->Args({1 << 23, 100})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

}  // namespace compute
}  // namespace arrow
