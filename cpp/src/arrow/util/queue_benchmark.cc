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
#include <boost/lockfree/spsc_queue.hpp>
#include <iostream>
#include <iterator>
#include <thread>
#include <vector>

#include <benchmark/benchmark.h>

#include "arrow/buffer.h"
#include "arrow/util/queue.h"

namespace arrow {

namespace util {

static constexpr int64_t kSize = 100000;

void throughput(benchmark::State& state) {
  SpscQueue<std::shared_ptr<Buffer>> queue(16);

  std::vector<std::shared_ptr<Buffer>> one;
  std::vector<std::shared_ptr<Buffer>> two;
  one.reserve(kSize);
  two.resize(kSize);
  const uint8_t data[1] = {0};
  for (int64_t i = 0; i < kSize; i++) {
    one.push_back(std::make_shared<Buffer>(data, 1));
  }

  std::vector<std::shared_ptr<Buffer>>* source = &one;
  std::vector<std::shared_ptr<Buffer>>* sink = &two;
  std::vector<std::shared_ptr<Buffer>>* swap = &one;

  for (auto _ : state) {
    std::thread producer([&queue, source] {
      auto itr = std::make_move_iterator(source->begin());
      auto end = std::make_move_iterator(source->end());
      while (itr != end) {
        while (!queue.write(*itr)) {
        }
        itr++;
      }
    });

    std::thread consumer([&queue, sink] {
      auto itr = sink->begin();
      auto end = sink->end();
      while (itr != end) {
        auto next = queue.frontPtr();
        if (next != nullptr) {
          (*itr).swap(*next);
          queue.popFront();
          itr++;
        }
      }
    });

    producer.join();
    consumer.join();
    swap = source;
    source = sink;
    sink = swap;
  }

  state.SetItemsProcessed(state.iterations() * kSize);
}

void throughput_boost(benchmark::State& state) {
  boost::lockfree::spsc_queue<std::shared_ptr<Buffer>> queue(16);

  std::vector<std::shared_ptr<Buffer>> one;
  std::vector<std::shared_ptr<Buffer>> two;
  one.reserve(kSize);
  two.resize(kSize);
  const uint8_t data[1] = {0};
  for (int64_t i = 0; i < kSize; i++) {
    one.push_back(std::make_shared<Buffer>(data, 1));
  }

  std::vector<std::shared_ptr<Buffer>>* source = &one;
  std::vector<std::shared_ptr<Buffer>>* sink = &two;
  std::vector<std::shared_ptr<Buffer>>* swap = &one;

  for (auto _ : state) {
    std::thread producer([&queue, source] {
      auto itr = source->begin();
      auto end = source->end();
      while (itr != end) {
        while (!queue.push(*itr)) {
        }
        itr++;
      }
    });

    std::thread consumer([&queue, sink] {
      auto itr = sink->begin();
      auto end = sink->end();
      while (itr != end) {
        while (queue.read_available() <= 0) {
        }
        (*itr).swap(queue.front());
        queue.pop();
        itr++;
      }
    });

    producer.join();
    consumer.join();
    swap = source;
    source = sink;
    sink = swap;
  }

  state.SetItemsProcessed(state.iterations() * kSize);
}

BENCHMARK(throughput)->UseRealTime();
BENCHMARK(throughput_boost)->UseRealTime();

}  // namespace util
}  // namespace arrow
