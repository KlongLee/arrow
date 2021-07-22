#!/usr/bin/env bash
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

set -ex

: ${R_BIN:=R}

source_dir=${1}/r

pushd ${source_dir}

printenv

if [ "$ARROW_USE_PKG_CONFIG" != "false" ]; then
  export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}
  export R_LD_LIBRARY_PATH=${LD_LIBRARY_PATH}
fi
export _R_CHECK_COMPILATION_FLAGS_KNOWN_=${ARROW_R_CXXFLAGS}
if [ "$ARROW_R_DEV" = "TRUE" ]; then
  # These are used in the Arrow C++ build and are not a problem
  export _R_CHECK_COMPILATION_FLAGS_KNOWN_="${_R_CHECK_COMPILATION_FLAGS_KNOWN_} -Wno-attributes -msse4.2"
  # Note that NOT_CRAN=true means (among other things) that optional dependencies are built
  export NOT_CRAN=true
fi
: ${TEST_R_WITH_ARROW:=TRUE}
export TEST_R_WITH_ARROW=$TEST_R_WITH_ARROW

export _R_CHECK_CRAN_INCOMING_REMOTE_=FALSE
# --run-donttest was used in R < 4.0, this is used now
export _R_CHECK_DONTTEST_EXAMPLES_=$TEST_R_WITH_ARROW
# Not all Suggested packages are needed for checking, so in case they aren't installed don't fail
export _R_CHECK_FORCE_SUGGESTS_=FALSE
export _R_CHECK_LIMIT_CORES_=FALSE
export _R_CHECK_TESTS_NLINES_=0

# By default, aws-sdk tries to contact a non-existing local ip host
# to retrieve metadata. Disable this so that S3FileSystem tests run faster.
export AWS_EC2_METADATA_DISABLED=TRUE

# Hack so that texlive2020 doesn't pollute the home dir
export TEXMFCONFIG=/tmp/texmf-config
export TEXMFVAR=/tmp/texmf-var

if [[ "$DEVTOOLSET_VERSION" -gt 0 ]]; then
  # enable the devtoolset version to use it
  source /opt/rh/devtoolset-$DEVTOOLSET_VERSION/enable
fi

# Make sure we aren't writing to the home dir (CRAN _hates_ this but there is no official check)
BEFORE=$(ls -alh ~/)

SCRIPT="as_cran <- !identical(tolower(Sys.getenv('NOT_CRAN')), 'true')
# Install the dev version of duckdb, remove this when 0.2.8 is released
install.packages('https://github.com/duckdb/duckdb/releases/download/master-builds/duckdb_r_src.tar.gz', repos = NULL)

  run_donttest <- identical(tolower(Sys.getenv('_R_CHECK_DONTTEST_EXAMPLES_', 'true')), 'true')
  if (as_cran) {
    rcmdcheck::rcmdcheck(args = c('--as-cran', if (run_donttest) '--run-donttest'), error_on = 'warning', check_dir = 'check', timeout = 3600)
  } else {
    if (nzchar(Sys.which('minio'))) {
      message('Running minio for S3 tests (if build supports them)')
      minio_dir <- tempfile()
      dir.create(minio_dir)
      pid <- sys::exec_background('minio', c('server', minio_dir))
      on.exit(tools::pskill(pid))
    }
    rcmdcheck::rcmdcheck(build_args = '--no-build-vignettes', args = c('--no-manual', '--ignore-vignettes', if (run_donttest) '--run-donttest'), error_on = 'warning', check_dir = 'check', timeout = 3600)
  }"
echo "$SCRIPT" | ${R_BIN} --no-save

AFTER=$(ls -alh ~/)
if [ "$NOT_CRAN" != "true" ] && [ "$BEFORE" != "$AFTER" ]; then
  ls -alh ~/.cmake/packages
  exit 1
fi
popd
