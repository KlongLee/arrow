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

#include <vector>
#ifdef _MSC_VER
#include <intrin.h>
#else
#include <immintrin.h>
#endif

#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/bit-util.h"

#include "arrow/compute/benchmark-util.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/comparator.h"
#include "arrow/compute/test-util.h"

namespace arrow {
namespace compute {

#include <cassert>
#include <cmath>
#include <iostream>
#include <random>

static void BenchCompareKernel(benchmark::State& state) {
  const int64_t array_size = state.range(0) / sizeof(int64_t);
  const double null_percent = static_cast<double>(state.range(1)) / 100.0;
  auto rand = random::RandomArrayGenerator(0x94378165);
  auto array = std::static_pointer_cast<NumericArray<Int64Type>>(
      rand.Int64(array_size, -100, 100, null_percent));

  CompareOptions ge(GREATER_EQUAL);

  FunctionContext ctx;
  for (auto _ : state) {
    Datum out;
    ABORT_NOT_OK(Compare(&ctx, Datum(array), Datum(int64_t(0)), ge, &out));
    benchmark::DoNotOptimize(out);
  }

  state.counters["size"] = static_cast<double>(state.range(0));
  state.counters["null_percent"] = static_cast<double>(state.range(1));
  state.SetBytesProcessed(state.iterations() * array_size * sizeof(int64_t));
}

BENCHMARK(BenchCompareKernel)->Apply(BenchmarkSetArgs);

}  // namespace compute
}  // namespace arrow
