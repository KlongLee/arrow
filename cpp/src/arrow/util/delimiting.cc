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

#include "arrow/util/delimiting.h"
#include "arrow/buffer.h"

namespace arrow {

BoundaryFinder::~BoundaryFinder() {}

namespace {

Status StraddlingTooLarge() {
  return Status::Invalid(
      "straddling object straddles two block boundaries (try to increase block size?)");
}

class NewlineBoundaryFinder : public BoundaryFinder {
 public:
  Status FindFirst(util::string_view partial, util::string_view block,
                   int64_t* out_pos) override {
    auto pos = block.find_first_of(newline_delimiters);
    if (pos == util::string_view::npos) {
      *out_pos = kNoDelimiterFound;
    } else {
      auto end = block.find_first_not_of(newline_delimiters, pos);
      if (end == util::string_view::npos) {
        end = block.length();
      }
      *out_pos = static_cast<int64_t>(end);
    }
    return Status::OK();
  }

  Status FindLast(util::string_view block, int64_t* out_pos) override {
    auto pos = block.find_last_of(newline_delimiters);
    if (pos == util::string_view::npos) {
      *out_pos = kNoDelimiterFound;
    } else {
      auto end = block.find_first_not_of(newline_delimiters, pos);
      if (end == util::string_view::npos) {
        end = block.length();
      }
      *out_pos = static_cast<int64_t>(end);
    }
    return Status::OK();
  }

 protected:
  static constexpr const char* newline_delimiters = "\r\n";
};

}  // namespace

std::shared_ptr<BoundaryFinder> MakeNewlineBoundaryFinder() {
  return std::make_shared<NewlineBoundaryFinder>();
}

Chunker::~Chunker() {}

Chunker::Chunker(std::shared_ptr<BoundaryFinder> delimiter)
    : boundary_finder_(delimiter) {}

Status Chunker::Process(std::shared_ptr<Buffer> block, std::shared_ptr<Buffer>* whole,
                        std::shared_ptr<Buffer>* partial) {
  int64_t last_pos = -1;
  RETURN_NOT_OK(boundary_finder_->FindLast(util::string_view(*block), &last_pos));
  if (last_pos == BoundaryFinder::kNoDelimiterFound) {
    // No delimiter found
    *whole = SliceBuffer(block, 0, 0);
    *partial = block;
    return Status::OK();
  } else {
    *whole = SliceBuffer(block, 0, last_pos);
    *partial = SliceBuffer(block, last_pos);
  }
  return Status::OK();
}

Status Chunker::ProcessWithPartial(std::shared_ptr<Buffer> partial,
                                   std::shared_ptr<Buffer> block,
                                   std::shared_ptr<Buffer>* completion,
                                   std::shared_ptr<Buffer>* rest) {
  if (partial->size() == 0) {
    // If partial is empty, don't bother looking for completion
    *completion = SliceBuffer(block, 0, 0);
    *rest = block;
    return Status::OK();
  }
  int64_t first_pos = -1;
  RETURN_NOT_OK(boundary_finder_->FindFirst(util::string_view(*partial),
                                            util::string_view(*block), &first_pos));
  if (first_pos == BoundaryFinder::kNoDelimiterFound) {
    // No delimiter in block => the current object is too large for block size
    return StraddlingTooLarge();
  } else {
    *completion = SliceBuffer(block, 0, first_pos);
    *rest = SliceBuffer(block, first_pos);
    return Status::OK();
  }
}

Status Chunker::ProcessFinal(std::shared_ptr<Buffer> partial,
                             std::shared_ptr<Buffer> block,
                             std::shared_ptr<Buffer>* completion,
                             std::shared_ptr<Buffer>* rest) {
  if (partial->size() == 0) {
    // If partial is empty, don't bother looking for completion
    *completion = SliceBuffer(block, 0, 0);
    *rest = block;
    return Status::OK();
  }
  int64_t first_pos = -1;
  RETURN_NOT_OK(boundary_finder_->FindFirst(util::string_view(*partial),
                                            util::string_view(*block), &first_pos));
  if (first_pos == BoundaryFinder::kNoDelimiterFound) {
    // No delimiter in block => it's entirely a completion of partial
    *completion = block;
    *rest = SliceBuffer(block, 0, 0);
  } else {
    *completion = SliceBuffer(block, 0, first_pos);
    *rest = SliceBuffer(block, first_pos);
  }
  return Status::OK();
}

}  // namespace arrow
