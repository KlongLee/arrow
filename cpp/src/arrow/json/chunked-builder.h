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

#include "arrow/array.h"
#include "arrow/json/converter.h"
#include "arrow/util/task-group.h"

namespace arrow {
namespace json {

class ARROW_EXPORT ChunkedArrayBuilder {
 public:
  virtual ~ChunkedArrayBuilder() = default;

  /// Spawn a task that will try to convert and insert the given JSON block
  virtual void Insert(int64_t block_index,
                      const std::shared_ptr<Field>& unconverted_field,
                      const std::shared_ptr<Array>& unconverted) = 0;

  /// Return the final chunked array.
  virtual Status Finish(std::shared_ptr<ChunkedArray>* out) = 0;

  std::shared_ptr<internal::TaskGroup> task_group() { return task_group_; }

 protected:
  explicit ChunkedArrayBuilder(const std::shared_ptr<internal::TaskGroup>& task_group)
      : task_group_(task_group) {}

  std::shared_ptr<internal::TaskGroup> task_group_;
};

/// create a chunked builder
///
/// if unexpected fields and promotion need to be handled, promotion_graph must be
/// non-null
Status MakeChunkedArrayBuilder(const std::shared_ptr<internal::TaskGroup>& task_group,
                               MemoryPool* pool, const PromotionGraph* promotion_graph,
                               const std::shared_ptr<DataType>& type,
                               std::unique_ptr<ChunkedArrayBuilder>* out);

}  // namespace json
}  // namespace arrow
