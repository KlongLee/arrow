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

#include "parquet/encryption.h"
#include "parquet/key_material.h"
#include "parquet/key_toolkit.h"
#include "parquet/kms_client.h"
#include "parquet/kms_client_factory.h"
#include "parquet/platform.h"

namespace parquet {
namespace encryption {

// This class will retrieve the key from "key metadata", following these steps:
// 1. Parse "key metadata" (see structure in KeyMetadata class).
// 2. Retrieve "key material" which can be stored inside or outside "key metadata"
//    Currently we don't support the case "key material" stores outside "key metadata"
//    yet.
// 3. Unwrap the "data encryption key" from "key material". There are 2 modes:
// 3.1. single wrapping: decrypt the wrapped "data encryption key" directly with "master
// encryption key" 3.2. double wrapping: 2 steps: 3.2.1. "key encryption key" is decrypted
// with "master encryption key" 3.2.2. "data encryption key" is decrypted with the above
// "key encryption key"
class PARQUET_EXPORT FileKeyUnwrapper : public DecryptionKeyRetriever {
 public:
  /// kms_client_factory and kms_connection_config is to create KmsClient if it's not in
  /// the cache yet. cache_entry_lifetime_seconds is life time of KmsClient in the cache.
  FileKeyUnwrapper(std::shared_ptr<KmsClientFactory> kms_client_factory,
                   const KmsConnectionConfig& kms_connection_config,
                   uint64_t cache_lifetime_seconds, bool is_wrap_locally);

  std::string GetKey(const std::string& key_metadata) const override;

 private:
  KeyWithMasterId GetDataEncryptionKey(const KeyMaterial& key_material) const;
  std::shared_ptr<KmsClient> GetKmsClientFromConfigOrKeyMaterial(
      const KeyMaterial& key_material) const;

  // A map of Key Encryption Key (KEK) ID -> KEK bytes, for the current token
  mutable std::map<std::string, std::string> kek_per_kek_id_;
  std::shared_ptr<KmsClientFactory> kms_client_factory_;
  mutable KmsConnectionConfig kms_connection_config_;
  const uint64_t cache_entry_lifetime_ms_;
  const bool is_wrap_locally_;
};

}  // namespace encryption
}  // namespace parquet
