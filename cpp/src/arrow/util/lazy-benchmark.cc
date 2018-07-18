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

#include <algorithm>
#include <iterator>
#include <vector>

#include <benchmark/benchmark.h>

#include "arrow/util/lazy.h"

namespace arrow {

static constexpr int64_t kSize = 100000000;

std::vector<int> generate_junk(int64_t size) {
  std::vector<int> v(size);
  for (unsigned index = 0; index < size; ++index) {
    v[index] = index ^ 4311 % size;
  }
  return v;
}

// baseline
void BM_for_loop(benchmark::State& state) {
  auto source = generate_junk(kSize);
  std::vector<int> target(kSize);

  for (auto _ : state) {
    for (unsigned index = 0, limit = source.size(); index != limit; ++index)
      target[index] = source[index] + 1;
  }
}

BENCHMARK(BM_for_loop)->Repetitions(3)->Unit(benchmark::kMillisecond);

// for comparison: pure copy without any changes
void BM_std_copy(benchmark::State& state) {
  auto source = generate_junk(kSize);
  std::vector<int> target(kSize);

  for (auto _ : state) {
    std::copy(source.begin(), source.end(), target.begin());
  }
}

BENCHMARK(BM_std_copy)->Repetitions(3)->Unit(benchmark::kMillisecond);

// std::copy with a lazy iterator
void BM_lazy_copy(benchmark::State& state) {
  auto source = generate_junk(kSize);
  std::vector<int> target(kSize);
  auto lazy_range = internal::MakeLazyRange(
      [&source](int64_t index) { return source[index]; }, source.size());

  for (auto _ : state) {
    std::copy(lazy_range.begin(), lazy_range.end(), target.begin());
  }
}

BENCHMARK(BM_lazy_copy)->Repetitions(3)->Unit(benchmark::kMillisecond);

// for loop with a post-increment of a lazy operator
void BM_lazy_postinc(benchmark::State& state) {
  auto source = generate_junk(kSize);
  std::vector<int> target(kSize);
  auto lazy_range = internal::MakeLazyRange(
      [&source](int64_t index) { return source[index]; }, source.size());

  for (auto _ : state) {
    auto lazy_iter = lazy_range.begin();
    auto lazy_end = lazy_range.end();
    auto target_iter = target.begin();

    while (lazy_iter != lazy_end) *(target_iter++) = *(lazy_iter++);
  }
}

BENCHMARK(BM_lazy_postinc)->Repetitions(3)->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();

}  // namespace arrow
