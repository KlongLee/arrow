#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Run this from cpp/ directory. flatc is expected to be in your path.
# The output directory can be passed as a positional argumenet,
# it defaults to /cpp/src/generated.

set -euo pipefail

CWD="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
SOURCE_DIR="$CWD/../src"
FORMAT_DIR="$CWD/../../format"
FLATC="flatc"

OUT_DIR="${1:-$SOURCE_DIR/generated}"
FILES=($(find "$FORMAT_DIR" -name '*.fbs'))
FILES+=("$SOURCE_DIR/arrow/ipc/feather.fbs")

$FLATC --cpp --cpp-std c++11 \
  --scoped-enums \
  -o "$OUT_DIR" \
  "${FILES[@]}"

PLASMA_FBS=("$SOURCE_DIR"/plasma/{plasma,common}.fbs)

$FLATC --cpp --cpp-std c++11 \
  -o "$SOURCE_DIR/plasma" \
  --gen-object-api \
  --scoped-enums \
  "${PLASMA_FBS[@]}"
