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

#include "arrow/compute/exec/options.h"
#include "arrow/util/logging.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/io/util_internal.h"
#include "arrow/util/async_generator.h"
#include "arrow/table.h"

namespace arrow {
namespace compute {

std::string ToString(JoinType t) {
  switch (t) {
    case JoinType::LEFT_SEMI:
      return "LEFT_SEMI";
    case JoinType::RIGHT_SEMI:
      return "RIGHT_SEMI";
    case JoinType::LEFT_ANTI:
      return "LEFT_ANTI";
    case JoinType::RIGHT_ANTI:
      return "RIGHT_ANTI";
    case JoinType::INNER:
      return "INNER";
    case JoinType::LEFT_OUTER:
      return "LEFT_OUTER";
    case JoinType::RIGHT_OUTER:
      return "RIGHT_OUTER";
    case JoinType::FULL_OUTER:
      return "FULL_OUTER";
  }
  ARROW_LOG(FATAL) << "Invalid variant of arrow::compute::JoinType";
  std::abort();
}

Result<std::shared_ptr<SourceNodeOptions>> SourceNodeOptions::FromTable(const Table& table) {
  std::shared_ptr<RecordBatchReader> reader = std::make_shared<TableBatchReader>(table);

  // Map the RecordBatchReader to a SourceNode
  ARROW_ASSIGN_OR_RAISE(
    auto batch_gen,
    MakeReaderGenerator(std::move(reader), arrow::io::internal::GetIOThreadPool()));

  return std::shared_ptr<SourceNodeOptions>(new SourceNodeOptions(table.schema(), batch_gen));
}


std::pair<std::shared_ptr<SinkNodeOptions>, std::shared_ptr<RecordBatchReader>> SinkNodeOptions::MakeForRecordBatchReader(std::shared_ptr<Schema> schema) {
  // finally, pipe the project node into a sink node
  AsyncGenerator<util::optional<compute::ExecBatch>> sink_gen;
  auto node_options = std::shared_ptr<SinkNodeOptions>(new compute::SinkNodeOptions(&sink_gen));

  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<RecordBatchReader> sink_reader = compute::MakeGeneratorReader(
      schema, std::move(sink_gen), default_memory_pool());

  return std::make_pair(node_options, sink_reader);
}

}  // namespace compute
}  // namespace arrow
