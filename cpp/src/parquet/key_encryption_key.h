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

#include <cstdint>
#include <vector>

#include "arrow/util/base64.h"

namespace parquet {
namespace encryption {

// In the double wrapping mode, each DEK is encrypted with a “key encryption key” (KEK),
// that in turn is encrypted with a "master encryption key" (MEK). In a writer process, a
// random KEK is generated for each MEK ID, and cached in a <MEK-ID : KEK> map. This
// allows to perform an interaction with a KMS server only once for each MEK, in order to
// wrap its KEK. "Data encryption key" (DEK) wrapping is performed locally, and does not
// involve an interaction with a KMS server.
class KeyEncryptionKey {
 public:
  KeyEncryptionKey(const std::string& kek_bytes, const std::string& kek_id,
                   const std::string& encoded_wrapped_kek)
      : kek_bytes_(kek_bytes),
        kek_id_(kek_id),
        encoded_wrapped_kek_(encoded_wrapped_kek) {
    encoded_kek_id_ = arrow::util::base64_encode(reinterpret_cast<uint8_t*>(&kek_id_[0]),
                                                 kek_id_.size());
  }

  const std::string& kek_bytes() const { return kek_bytes_; }

  const std::string& kek_id() const { return kek_id_; }

  const std::string& encoded_kek_id() const { return encoded_kek_id_; }

  const std::string& encoded_wrapped_kek() const { return encoded_wrapped_kek_; }

 private:
  std::string kek_bytes_;
  std::string kek_id_;
  std::string encoded_kek_id_;
  std::string encoded_wrapped_kek_;
};

}  // namespace encryption
}  // namespace parquet
