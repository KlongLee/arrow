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

#include <cstdint>
#include <memory>
#include <numeric>
#include <string>

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/reader.h>

#include "arrow/buffer.h"
#include "arrow/json/chunker.h"
#include "arrow/json/options.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/util.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace json {

// Use no nested objects and no string literals containing braces in this test.
// This way the positions of '{' and '}' can be used as simple proxies
// for object begin/end.

using util::string_view;

template <typename Lines>
std::string join(Lines&& lines, std::string delimiter) {
  std::string joined;
  for (const auto& line : lines) {
    joined += line + delimiter;
  }
  return joined;
}

std::string PrettyPrint(string_view one_line) {
  rapidjson::Document document;
  document.Parse(one_line.data());
  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
  document.Accept(writer);
  return sb.GetString();
}

string_view View(const std::shared_ptr<Buffer>& buffer) {
  return string_view(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

bool WhitespaceOnly(string_view s) {
  return s.find_first_not_of(" \t\r\n") == string_view::npos;
}

bool WhitespaceOnly(const std::shared_ptr<Buffer>& b) { return WhitespaceOnly(View(b)); }

std::size_t ConsumeWholeObject(std::shared_ptr<Buffer>* buf) {
  auto str = View(*buf);
  auto fail = [buf] {
    *buf = nullptr;
    return string_view::npos;
  };
  if (WhitespaceOnly(str)) return fail();
  auto open_brace = str.find_first_not_of(" \t\r\n");
  if (str.at(open_brace) != '{') return fail();
  auto close_brace = str.find_first_of("}");
  if (close_brace == string_view::npos) return fail();
  if (str.at(close_brace) != '}') return fail();
  auto length = close_brace + 1;
  *buf = SliceBuffer(*buf, length);
  return length;
}

void AssertWholeObjects(Chunker& chunker, const std::shared_ptr<Buffer>& block,
                        int expected_count) {
  std::shared_ptr<Buffer> whole, partial;
  ASSERT_OK(chunker.Process(block, &whole, &partial));
  int count = 0;
  while (whole && !WhitespaceOnly(whole)) {
    if (ConsumeWholeObject(&whole) == string_view::npos) FAIL();
    ++count;
  }
  ASSERT_EQ(count, expected_count);
}

void AssertChunking(Chunker& chunker, std::shared_ptr<Buffer> buf, int total_count) {
  // First chunkize whole JSON block
  AssertWholeObjects(chunker, buf, total_count);

  // Then chunkize incomplete substrings of the block
  for (int i = 0; i != total_count; ++i) {
    // ensure shearing the closing brace off the last object causes it to be chunked out
    auto last_brace = View(buf).find_last_of('}');
    AssertWholeObjects(chunker, SliceBuffer(buf, 0, last_brace), total_count - i - 1);

    // ensure skipping one object reduces the count by one
    ASSERT_NE(ConsumeWholeObject(&buf), string_view::npos);
    AssertWholeObjects(chunker, buf, total_count - i - 1);
  }
}

void AssertChunking(Chunker& chunker, const std::string& str, int total_count) {
  AssertChunking(chunker, std::make_shared<Buffer>(str), total_count);
}

void AssertStraddledChunking(Chunker& chunker, const std::string& str) {
  auto buf = std::make_shared<Buffer>(str);
  auto first_half = SliceBuffer(buf, 0, buf->size() / 2);
  auto second_half = SliceBuffer(buf, buf->size() / 2);
  AssertChunking(chunker, first_half, 1);
  std::shared_ptr<Buffer> first_whole, partial;
  ASSERT_OK(chunker.Process(first_half, &first_whole, &partial));
  ASSERT_TRUE(View(first_half).starts_with(View(first_whole)));
  std::shared_ptr<Buffer> completion, rest;
  ASSERT_OK(chunker.Process(partial, second_half, &completion, &rest));
  ASSERT_TRUE(View(second_half).starts_with(View(completion)));
  std::shared_ptr<Buffer> straddling;
  ASSERT_OK(
      ConcatenateBuffers({partial, completion}, default_memory_pool(), &straddling));
  auto length = ConsumeWholeObject(&straddling);
  ASSERT_NE(length, string_view::npos);
  ASSERT_NE(length, 0);
  auto final_whole = SliceBuffer(second_half, completion->size());
  length = ConsumeWholeObject(&final_whole);
  ASSERT_NE(length, string_view::npos);
  ASSERT_NE(length, 0);
}

std::unique_ptr<Chunker> MakeChunker(bool newlines_in_values) {
  auto options = ParseOptions::Defaults();
  options.newlines_in_values = newlines_in_values;
  return Chunker::Make(options);
}

class BaseChunkerTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override { chunker_ = MakeChunker(GetParam()); }

  std::unique_ptr<Chunker> chunker_;
};

INSTANTIATE_TEST_CASE_P(NoNewlineChunkerTest, BaseChunkerTest, ::testing::Values(false));

INSTANTIATE_TEST_CASE_P(ChunkerTest, BaseChunkerTest, ::testing::Values(true));

constexpr auto object_count = 3;
const std::vector<std::string>& lines() {
  static const std::vector<std::string> l = {R"({"0":"ab","1":"c","2":""})",
                                             R"({"0":"def","1":"","2":"gh"})",
                                             R"({"0":"","1":"ij","2":"kl"})"};
  return l;
}

TEST_P(BaseChunkerTest, Basics) {
  AssertChunking(*chunker_, join(lines(), "\n"), object_count);
}

TEST_P(BaseChunkerTest, Empty) {
  std::string empty = "\n";
  AssertChunking(*chunker_, std::make_shared<Buffer>(empty), 0);
  empty = "\n\n";
  AssertChunking(*chunker_, std::make_shared<Buffer>(empty), 0);
}

TEST(ChunkerTest, PrettyPrinted) {
  std::string pretty[object_count];
  std::transform(std::begin(lines()), std::end(lines()), std::begin(pretty), PrettyPrint);
  auto chunker = MakeChunker(true);
  AssertChunking(*chunker, join(pretty, "\n"), object_count);
}

TEST(ChunkerTest, SingleLine) {
  auto chunker = MakeChunker(true);
  auto single_line = join(lines(), "");
  AssertChunking(*chunker, single_line, object_count);
}

TEST_P(BaseChunkerTest, Straddling) {
  AssertStraddledChunking(*chunker_, join(lines(), "\n"));
}

TEST(ChunkerTest, StraddlingPrettyPrinted) {
  std::string pretty[object_count];
  std::transform(std::begin(lines()), std::end(lines()), std::begin(pretty), PrettyPrint);
  auto chunker = MakeChunker(true);
  AssertStraddledChunking(*chunker, join(pretty, "\n"));
}

TEST(ChunkerTest, StraddlingSingleLine) {
  auto chunker = MakeChunker(true);
  AssertStraddledChunking(*chunker, join(lines(), ""));
}

TEST_P(BaseChunkerTest, StraddlingEmpty) {
  auto joined = join(lines(), "\n");
  auto all = std::make_shared<Buffer>(joined);

  auto first = SliceBuffer(all, 0, lines()[0].size() + 1);
  std::shared_ptr<Buffer> first_whole, partial;
  ASSERT_OK(chunker_->Process(first, &first_whole, &partial));
  ASSERT_TRUE(WhitespaceOnly(partial));

  auto others = SliceBuffer(all, first->size());
  std::shared_ptr<Buffer> completion, rest;
  ASSERT_OK(chunker_->Process(partial, others, &completion, &rest));
  ASSERT_EQ(completion->size(), 0);
  ASSERT_TRUE(rest->Equals(*others));
}

}  // namespace json
}  // namespace arrow
