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

set -e

if [[ "$OSTYPE" == "darwin20" ]]; then
  brew install node
  npm install -g azurite
  which azurite
elif [[ "$OSTYPE" == "msys" ]]; then
  choco install nodejs.install
  npm install -g azurite
else
  apt-get update
  apt-get -y install npm
  npm install -g azurite
  which azurite
fi
echo "node version = `node --version`"
echo "azurite version = `azurite --version`"
AZURITE_DIR=${1}/azurite
mkdir $AZURITE_DIR

# Start azurite
azurite --silent --location $AZURITE_DIR --debug $AZURITE_DIR/debug.log & 
