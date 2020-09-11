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

#include "arrow/util/base64.h"

#include "parquet/exception.h"
#include "parquet/key_toolkit_internal.h"
#include "parquet/kms_client_factory.h"
#include "parquet/remote_kms_client.h"
#include "parquet/test_in_memory_kms.h"

namespace parquet {
namespace encryption {

std::unordered_map<std::string, std::string> TestOnlyInMemoryKms::master_key_map_;

void TestOnlyInMemoryKms::InitializeMasterKeys(
    const std::unordered_map<std::string, std::string>& master_keys_map) {
  master_key_map_ = master_keys_map;
}

void TestOnlyInMemoryKms::InitializeInternal() {}

std::string TestOnlyInMemoryKms::WrapKeyInServer(
    const std::string& key_bytes, const std::string& master_key_identifier) {
  // Always use the latest key version for writing
  if (master_key_map_.find(master_key_identifier) == master_key_map_.end()) {
    throw ParquetException("Key not found: " + master_key_identifier);
  }
  const std::string& master_key = master_key_map_.at(master_key_identifier);

  std::string aad = master_key_identifier;
  return internal::EncryptKeyLocally(key_bytes, master_key, aad);
}

std::string TestOnlyInMemoryKms::UnwrapKeyInServer(
    const std::string& wrapped_key, const std::string& master_key_identifier) {
  if (master_key_map_.find(master_key_identifier) == master_key_map_.end()) {
    throw ParquetException("Key not found: " + master_key_identifier);
  }
  const std::string& master_key = master_key_map_.at(master_key_identifier);

  std::string aad = master_key_identifier;
  return internal::DecryptKeyLocally(wrapped_key, master_key, aad);
}

std::string TestOnlyInMemoryKms::GetMasterKeyFromServer(
    const std::string& master_key_identifier) {
  // Always return the latest key version
  return master_key_map_.at(master_key_identifier);
}

}  // namespace encryption
}  // namespace parquet
