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

#include <functional>
#include <memory>
#include <vector>

#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/schema_util.h"
#include "arrow/compute/exec/task_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {
namespace compute {

class AsofJoinSchema {
 public:
  std::shared_ptr<Schema> MakeOutputSchema(const std::vector<ExecNode*>& inputs,
                                           const AsofJoinNodeOptions& options
                                           );

};

class AsofJoinImpl {
 public:
  static Result<std::unique_ptr<AsofJoinImpl>> MakeBasic();

};

}  // namespace compute
}  // namespace arrow
