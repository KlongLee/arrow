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

#include <arrow/util/decimal.h>

extern "C" {

#include <stdio.h>

#include "./types.h"

int print_double(char* msg, double val) { return printf(msg, val); }

int print_float(char* msg, float val) { return printf(msg, val); }

typedef __int128 int128_t;

void divb(const int128_t* x, const int128_t* y, int128_t* z) {
  arrow::Decimal128 xd((const uint8_t*)x);
  arrow::Decimal128 yd((const uint8_t*)y);
  xd /= yd;
  *z = xd.high_bits();
  *z <<= 64;
  *z += xd.low_bits();
}

void mulb(const int128_t* x, const int128_t* y, int128_t* z) {
  arrow::Decimal128 xd((const uint8_t*)x);
  arrow::Decimal128 yd((const uint8_t*)y);
  xd *= yd;
  *z = xd.high_bits();
  *z <<= 64;
  *z += xd.low_bits();
}

}  // extern "C"
