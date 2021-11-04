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

#include <openssl/aes.h>
#include <openssl/evp.h>

namespace gandiva {

/**
 * Encrypt data using aes algorithm
 **/
int32_t aes_encrypt(unsigned char* plaintext, int32_t plaintext_len, unsigned char* key,
                    unsigned char* cipher);

/**
 * Decrypt data using aes algorithm
 **/
int32_t aes_decrypt(unsigned char* ciphertext, int32_t ciphertext_len, unsigned char* key,
                    unsigned char* plaintext);

}  // namespace gandiva
