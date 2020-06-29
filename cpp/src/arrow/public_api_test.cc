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

#include <sstream>
#include <string>

#include "arrow/api.h"      // IWYU pragma: keep
#include "arrow/io/api.h"   // IWYU pragma: keep
#include "arrow/ipc/api.h"  // IWYU pragma: keep

#ifdef DCHECK
#error "DCHECK should not be visible from Arrow public headers."
#endif

#ifdef ASSIGN_OR_RAISE
#error "ASSIGN_OR_RAISE should not be visible from Arrow public headers."
#endif

#ifdef ARROW_UTIL_PARALLEL_H
#error "arrow/util/parallel.h is an internal header"
#endif

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace arrow {

TEST(Misc, BuildInfo) {
  const auto& info = GetBuildInfo();
  ASSERT_EQ(info.major, ARROW_VERSION_MAJOR);
  ASSERT_GE(info.minor, ARROW_VERSION_MINOR);
  ASSERT_GE(info.patch, ARROW_VERSION_PATCH);
  std::stringstream ss;
  ss << info.major << "." << info.minor << "." << info.patch;
  ASSERT_THAT(info.version_string, ::testing::HasSubstr(ss.str()));
  ASSERT_THAT(info.full_so_version, ::testing::HasSubstr(info.so_version));
}

}  // namespace arrow
