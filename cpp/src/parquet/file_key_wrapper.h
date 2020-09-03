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

#include <memory>
#include <string>
#include <unordered_map>

#include "parquet/file_key_material_store.h"
#include "parquet/key_encryption_key.h"
#include "parquet/kms_client.h"
#include "parquet/kms_client_factory.h"

namespace parquet {
namespace encryption {

// This class will generate "key metadata" from "data encryption key" and "master key",
// following these steps:
// 1. Wrap "data encryption key". There are 2 modes:
// 1.1. single wrapping: encrypt "data encryption key" directly with "master encryption
// key" 1.2. double wrapping: 2 steps: 1.2.1. "key encryption key" is randomized (see
// structure of KeyEncryptionKey class) 1.2.2. "data encryption key" is encrypted with the
// above "key encryption key"
// 2. Create "key material" (see structure in KeyMaterial class)
// 3. Create "key metadata" with "key material" inside or a reference to outside "key
// material" (see structure in KeyMetadata class).
//    Currently we don't support the case "key material" stores outside "key metadata"
//    yet.
class FileKeyWrapper {
 public:
  static constexpr int kKeyEncryptionKeyLength = 16;
  static constexpr int kKeyEncryptionKeyIdLength = 16;

  /// kms_client_factory and kms_connection_config is to create KmsClient if it's not in
  /// the cache yet. cache_entry_lifetime_seconds is life time of KmsClient in the cache.
  /// key_material_store is to store "key material" outside parquet file, NULL if "key
  /// material" is stored inside parquet file.
  FileKeyWrapper(std::shared_ptr<KmsClientFactory> kms_client_factory,
                 const KmsConnectionConfig& kms_connection_config,
                 std::shared_ptr<FileKeyMaterialStore> key_material_store,
                 uint64_t cache_entry_lifetime_seconds, bool double_wrapping,
                 bool is_wrap_locally);

  std::string GetEncryptionKeyMetadata(const std::string& data_key,
                                       const std::string& master_key_id,
                                       bool is_footer_key);

 private:
  KeyEncryptionKey CreateKeyEncryptionKey(const std::string& master_key_id);

  // A map of Master Encryption Key ID -> KeyEncryptionKey, for the current token
  std::unordered_map<std::string, KeyEncryptionKey> kek_per_master_key_id_;

  std::shared_ptr<KmsClient> kms_client_;
  KmsConnectionConfig kms_connection_config_;
  std::shared_ptr<FileKeyMaterialStore> key_material_store_;
  const uint64_t cache_entry_lifetime_ms_;
  const bool double_wrapping_;
};

}  // namespace encryption
}  // namespace parquet
