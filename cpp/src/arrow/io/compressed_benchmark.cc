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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/compression.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "compressed.h"

namespace arrow::io {

std::vector<uint8_t> MakeCompressibleData(int data_size) {
  // XXX This isn't a real-world corpus so doesn't really represent the
  // comparative qualities of the algorithms

  // First make highly compressible data
  std::string base_data =
      "Apache Arrow is a cross-language development platform for in-memory data";
  int nrepeats = static_cast<int>(1 + data_size / base_data.size());

  std::vector<uint8_t> data(base_data.size() * nrepeats);
  for (int i = 0; i < nrepeats; ++i) {
    std::memcpy(data.data() + i * base_data.size(), base_data.data(), base_data.size());
  }
  data.resize(data_size);

  // Then randomly mutate some bytes so as to make things harder
  std::mt19937 engine(42);
  std::exponential_distribution<> offsets(0.05);
  std::uniform_int_distribution<> values(0, 255);

  int64_t pos = 0;
  while (pos < data_size) {
    data[pos] = static_cast<uint8_t>(values(engine));
    pos += static_cast<int64_t>(offsets(engine));
  }

  return data;
}

class NonZeroCopyBufferReader final : public BufferReader {
 public:
  using BufferReader::BufferReader;

  bool supports_zero_copy() const override { return false; }
};

template <typename BufReader, Compression::type COMPRESSION>
static void CompressionInputBenchmark(::benchmark::State& state) {
  auto data = MakeCompressibleData(state.range(0));
  auto codec = ::arrow::util::Codec::Create(COMPRESSION).ValueOrDie();
  auto max_compress_len = codec->MaxCompressedLen(data.size(), data.data());
  std::shared_ptr<::arrow::ResizableBuffer> buf =
      ::arrow::AllocateResizableBuffer(max_compress_len).ValueOrDie();
  int64_t length =
      codec->Compress(data.size(), data.data(), max_compress_len, buf->mutable_data())
          .ValueOrDie();
  ABORT_NOT_OK(buf->Resize(length));
  for (auto _ : state) {
    state.PauseTiming();
    auto reader = std::make_shared<BufReader>(buf);
    auto inputStream =
        ::arrow::io::CompressedInputStream::Make(codec.get(), reader).ValueOrDie();
    state.ResumeTiming();
    ABORT_NOT_OK(inputStream->Read(data.size(), data.data()));
  }
  state.SetBytesProcessed(length * state.iterations());
}

template <Compression::type COMPRESSION>
static void CompressionInputZeroCopyBenchmark(::benchmark::State& state) {
  CompressionInputBenchmark<::arrow::io::BufferReader, COMPRESSION>(state);
}

template <Compression::type COMPRESSION>
static void CompressionInputNonZeroCopyBenchmark(::benchmark::State& state) {
  CompressionInputBenchmark<NonZeroCopyBufferReader, COMPRESSION>(state);
}

static void CompressedInputArguments(::benchmark::internal::Benchmark* b) {
  b->ArgName("InputBytes")->Arg(8 * 1024)->Arg(64 * 1024)->Arg(1024 * 1024);
}

#ifdef ARROW_WITH_ZLIB
BENCHMARK_TEMPLATE(CompressionInputZeroCopyBenchmark, ::arrow::Compression::GZIP)
    ->Apply(CompressedInputArguments);
BENCHMARK_TEMPLATE(CompressionInputNonZeroCopyBenchmark, ::arrow::Compression::GZIP)
    ->Apply(CompressedInputArguments);
#endif

}  // namespace arrow::io
