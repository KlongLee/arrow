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

#include "parquet/level_conversion.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>

#include "arrow/util/bit_util.h"

namespace parquet {
namespace internal {

using ::testing::ElementsAreArray;

std::string BitmapToString(const uint8_t* bitmap, int64_t bit_count) {
  return arrow::internal::Bitmap(bitmap, /*offset*/ 0, /*length=*/bit_count).ToString();
}

std::string BitmapToString(const std::vector<uint8_t>& bitmap, int64_t bit_count) {
  return BitmapToString(bitmap.data(), bit_count);
}

TEST(TestColumnReader, DefinitionLevelsToBitmap) {
  // Bugs in this function were exposed in ARROW-3930
  std::vector<int16_t> def_levels = {3, 3, 3, 2, 3, 3, 3, 3, 3};
  std::vector<int16_t> rep_levels = {0, 1, 1, 1, 1, 1, 1, 1, 1};

  std::vector<uint8_t> valid_bits(2, 0);

  const int max_def_level = 3;
  const int max_rep_level = 1;

  int64_t values_read = -1;
  int64_t null_count = 0;
  internal::DefinitionLevelsToBitmap(def_levels.data(), 9, max_def_level, max_rep_level,
                                     &values_read, &null_count, valid_bits.data(),
                                     0 /* valid_bits_offset */);
  ASSERT_EQ(9, values_read);
  ASSERT_EQ(1, null_count);

  // Call again with 0 definition levels, make sure that valid_bits is unmodified
  const uint8_t current_byte = valid_bits[1];
  null_count = 0;
  internal::DefinitionLevelsToBitmap(def_levels.data(), 0, max_def_level, max_rep_level,
                                     &values_read, &null_count, valid_bits.data(),
                                     9 /* valid_bits_offset */);
  ASSERT_EQ(0, values_read);
  ASSERT_EQ(0, null_count);
  ASSERT_EQ(current_byte, valid_bits[1]);
}

TEST(TestColumnReader, DefinitionLevelsToBitmapPowerOfTwo) {
  // PARQUET-1623: Invalid memory access when decoding a valid bits vector that has a
  // length equal to a power of two and also using a non-zero valid_bits_offset.  This
  // should not fail when run with ASAN or valgrind.
  std::vector<int16_t> def_levels = {3, 3, 3, 2, 3, 3, 3, 3};
  std::vector<int16_t> rep_levels = {0, 1, 1, 1, 1, 1, 1, 1};
  std::vector<uint8_t> valid_bits(1, 0);

  const int max_def_level = 3;
  const int max_rep_level = 1;

  int64_t values_read = -1;
  int64_t null_count = 0;

  // Read the latter half of the validity bitmap
  internal::DefinitionLevelsToBitmap(def_levels.data() + 4, 4, max_def_level,
                                     max_rep_level, &values_read, &null_count,
                                     valid_bits.data(), 4 /* valid_bits_offset */);
  ASSERT_EQ(4, values_read);
  ASSERT_EQ(0, null_count);
}

TEST(TestGreaterThanBitmap, GeneratesExpectedBitmasks) {
  std::vector<int16_t> levels = {0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
                                 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
                                 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
                                 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7};
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/0, /*rhs*/ 0), 0);
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ 8), 0);
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ -1),
            0xFFFFFFFFFFFFFFFF);
  // Should be zero padded.
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/47, /*rhs*/ -1),
            0x7FFFFFFFFFFF);
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ 6),
            0x8080808080808080);
}

TEST(TestAppendBitmap, TestOffsetOverwritesCorrectBitsOnExistingByte) {
  auto check_append = [](const std::string& expected_bits, int64_t offset) {
    std::vector<uint8_t> valid_bits = {0x00};
    constexpr int64_t kBitsAfterAppend = 8;
    ASSERT_EQ(
        AppendBitmap(/*new_bits=*/0xFF, /*number_of_bits*/ 8 - offset,
                     /*valid_bits_length=*/valid_bits.size(), offset, valid_bits.data()),
        kBitsAfterAppend);
    EXPECT_EQ(BitmapToString(valid_bits, kBitsAfterAppend), expected_bits);
  };
  check_append("11111111", 0);
  check_append("01111111", 1);
  check_append("00111111", 2);
  check_append("00011111", 3);
  check_append("00001111", 4);
  check_append("00000111", 5);
  check_append("00000011", 6);
  check_append("00000001", 7);
}

TEST(TestAppendBitmap, TestOffsetShiftBitsCorrectly) {
  constexpr uint64_t kPattern = 0x9A9A9A9A9A9A9A9A;
  auto check_append = [&](const std::string& leading_bits, const std::string& middle_bits,
                          const std::string& trailing_bits, int64_t offset) {
    ASSERT_GE(offset, 8);
    std::vector<uint8_t> valid_bits(/*count=*/10, 0);
    valid_bits[0] = 0x99;

    AppendBitmap(/*new_bits=*/kPattern, /*number_of_bits*/ 64,
                 /*valid_bits_length=*/valid_bits.size(), offset, valid_bits.data());
    EXPECT_EQ(valid_bits[0], 0x99);  // shouldn't get chanked.
    EXPECT_EQ(BitmapToString(valid_bits.data() + 1, /*num_bits=*/8), leading_bits);
    for (int x = 2; x < 9; x++) {
      EXPECT_EQ(BitmapToString(valid_bits.data() + x, /*num_bits=*/8), middle_bits);
    }
    EXPECT_EQ(BitmapToString(valid_bits.data() + 9, /*num_bits=*/8), trailing_bits);
  };
  // Original Pattern = "01011001"
  check_append(/*leading_bits= */ "01011001", /*middle_bits=*/"01011001",
               /*trailing_bits=*/"00000000", /*offset=*/8);
  check_append("00101100", "10101100", "10000000", 9);
  check_append("00010110", "01010110", "01000000", 10);
  check_append("00001011", "00101011", "00100000", 11);
  check_append("00000101", "10010101", "10010000", 12);
  check_append("00000010", "11001010", "11001000", 13);
  check_append("00000001", "01100101", "01100100", 14);
  check_append("00000000", "10110010", "10110010", 15);
}

TEST(TestAppendBitmap, AllBytesAreWrittenWithEnoughSpace) {
  std::vector<uint8_t> valid_bits(/*count=*/9, 0);

  uint64_t bitmap = 0xFFFFFFFFFFFFFFFF;
  AppendBitmap(bitmap, /*number_of_bits*/ 7,
               /*valid_bits_length=*/valid_bits.size(),
               /*valid_bits_offset=*/1,
               /*valid_bits=*/valid_bits.data());
  EXPECT_THAT(valid_bits, ElementsAreArray(std::vector<uint8_t>{
                              0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}));
}

TEST(TestAppendBitmap, OnlyApproriateBytesWrittenWhenLessThen8BytesAvailable) {
  std::vector<uint8_t> valid_bits = {0x00, 0x00};

  uint64_t bitmap = 0x1FF;
  AppendBitmap(bitmap, /*number_of_bits*/ 7,
               /*valid_bits_length=*/2,
               /*valid_bits_offset=*/1,
               /*valid_bits=*/valid_bits.data());

  EXPECT_THAT(valid_bits, ElementsAreArray(std::vector<uint8_t>{0xFE, 0x00}));

  AppendBitmap(bitmap, /*number_of_bits*/ 9,
               /*valid_bits_length=*/2,
               /*valid_bits_offset=*/1,
               /*valid_bits=*/valid_bits.data());
  EXPECT_THAT(valid_bits, ElementsAreArray(std::vector<uint8_t>{0xFE, 0x03}));
}

TEST(TestAppendToValidityBitmap, BasicOperation) {
  std::vector<uint8_t> validity_bitmap(/*count*/ 8, 0);
  int64_t valid_bitmap_offset = 1;
  int64_t set_bit_count = 5;
  AppendToValidityBitmap(/*new_bits*/ 0x99, /*new_bit_count=*/31, validity_bitmap.data(),
                         &valid_bitmap_offset, &set_bit_count);
  EXPECT_EQ(BitmapToString(validity_bitmap, valid_bitmap_offset),
            "01001100 10000000 00000000 00000000");
  EXPECT_EQ(set_bit_count, /*5 + 4 set bits=*/9);
}

#if defined(ARROW_HAVE_BMI2)
TEST(TestAppendSelectedBitsToValidityBitmap, BasicOperation) {
  std::vector<uint8_t> validity_bitmap(/*count*/ 8, 0);
  int64_t valid_bitmap_offset = 1;
  int64_t set_bit_count = 5;
  EXPECT_EQ(AppendSelectedBitsToValidityBitmap(
                /*new_bits*/ 0x99, /*selection_bitmap=*/0xB8, validity_bitmap.data(),
                &valid_bitmap_offset, &set_bit_count),
            /*bits_processed=*/4);
  EXPECT_EQ(BitmapToString(validity_bitmap, valid_bitmap_offset), "01101");
  EXPECT_EQ(set_bit_count, /*5 + 3 set bits=*/8);
}
#endif

}  // namespace internal
}  // namespace parquet
