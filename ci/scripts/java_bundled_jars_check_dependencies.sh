#!/bin/bash

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

set -e

function check_dynamic_dependencies(){
  local so_dep=$1
  local library=$2
  shift 2
  local whitelist=("$@")

  # print the shared library dependencies
  $so_dep "$library" | tee dependencies_temp_file.txt 

  # exit if any shared library not in whitelisted set is found
  echo "Checking shared dependencies"
  awk '{print $1}' dependencies_temp_file.txt | \
  while read -r line
  do
    found=false
  
    for item in "${whitelist[@]}"
    do
    if [[ "$line" == *"$item"* ]] ; then
      found=true
    fi
  done

  if [[ "$found" == false ]] ; then
    echo "Unexpected shared dependency found in $library : $line"
    exit 1
  fi
  done

  rm dependencies_temp_file.txt
}