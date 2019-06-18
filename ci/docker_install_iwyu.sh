#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -eu

: ${IWYU_REPO:="https://github.com/include-what-you-use/include-what-you-use.git"}
: ${IWYU_BRANCH:="clang_7.0"}
: ${IWYU_SRC:="/tmp/iwyu"}
: ${IWYU_HOME:="/opt/iwyu"}

git clone "${IWYU_REPO}" "${IWYU_SRC}"
git -C "${IWYU_SRC}" checkout ${IWYU_BRANCH}

mkdir -p "${IWYU_HOME}"
pushd "${IWYU_HOME}"

# Build IWYU for current Clang
export CC=clang-7
export CXX=clang++-7

cmake -DCMAKE_PREFIX_PATH=/usr/lib/llvm-7 "${IWYU_SRC}"
make -j4

popd
