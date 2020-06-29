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

#include <string>

#include "arrow/util/config.h"  // IWYU pragma: export
#include "arrow/util/visibility.h"

namespace arrow {

struct BuildInfo {
  int major;
  int minor;
  int patch;
  std::string version_string;
  std::string so_version;
  std::string full_so_version;
  std::string git_id;
  std::string git_description;
  std::string package_kind;
};

/// Get runtime build info.
///
/// The returned values correspond to exact loaded version of the Arrow library,
/// rather than the values frozen at application compile-time through the `ARROW_*`
/// preprocessor definitions.
ARROW_EXPORT
const BuildInfo& GetBuildInfo();

}  // namespace arrow
