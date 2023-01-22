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

#include "gandiva/function_signature.h"

#include <memory>

#include <gtest/gtest.h>

namespace gandiva {

class TestFunctionSignature : public ::testing::Test {
 protected:
  virtual void SetUp() {
    list_type1_ = arrow::list(arrow::int32());
    list_type2_ = arrow::list(arrow::int32());
    large_list_type1_ = arrow::large_list(arrow::int32());
    large_list_type2_ = arrow::large_list(arrow::int32());
  }

  virtual void TearDown() {
    list_type1_.reset();
    list_type2_.reset();
    large_list_type1_.reset();
    large_list_type2_.reset();
  }

  // virtual void TearDown() {}
  DataTypePtr list_type1_;
  DataTypePtr list_type2_;
  DataTypePtr large_list_type1_;
  DataTypePtr large_list_type2_;
};

TEST_F(TestFunctionSignature, TestToString) {
  EXPECT_EQ(
      FunctionSignature("myfunc", {arrow::int32(), arrow::float32()}, arrow::float64())
          .ToString(),
      "double myfunc(int32, float)");
}

TEST_F(TestFunctionSignature, TestEqualsName) {
  EXPECT_EQ(FunctionSignature("myfunc", {list_type1_}, large_list_type1_),
            FunctionSignature("myfunc", {list_type1_}, large_list_type1_));

  EXPECT_EQ(FunctionSignature("myfunc", {list_type1_}, large_list_type1_),
            FunctionSignature("myfunc", {list_type2_}, large_list_type2_));

  EXPECT_FALSE(FunctionSignature("add", {arrow::int32()}, arrow::int32()) ==
               FunctionSignature("sub", {arrow::int32()}, arrow::int32()));

  EXPECT_EQ(FunctionSignature("extractDay", {arrow::int64()}, arrow::int64()),
            FunctionSignature("extractday", {arrow::int64()}, arrow::int64()));

  EXPECT_EQ(
      FunctionSignature("castVARCHAR", {arrow::utf8(), arrow::int64()}, arrow::utf8()),
      FunctionSignature("castvarchar", {arrow::utf8(), arrow::int64()}, arrow::utf8()));
}

TEST_F(TestFunctionSignature, TestEqualsParamCount) {
  EXPECT_FALSE(
      FunctionSignature("add", {arrow::int32(), arrow::int32()}, arrow::int32()) ==
      FunctionSignature("add", {arrow::int32()}, arrow::int32()));
}

TEST_F(TestFunctionSignature, TestEqualsParamValue) {
  EXPECT_FALSE(FunctionSignature("add", {arrow::int32()}, arrow::int32()) ==
               FunctionSignature("add", {arrow::int64()}, arrow::int32()));

  EXPECT_FALSE(
      FunctionSignature("add", {arrow::int32()}, arrow::int32()) ==
      FunctionSignature("add", {arrow::float32(), arrow::float32()}, arrow::int32()));

  EXPECT_FALSE(
      FunctionSignature("add", {arrow::int32(), arrow::int64()}, arrow::int32()) ==
      FunctionSignature("add", {arrow::int64(), arrow::int32()}, arrow::int32()));

  EXPECT_FALSE(FunctionSignature("extract_month", {arrow::date32()}, arrow::int64()) ==
               FunctionSignature("extract_month", {arrow::date64()}, arrow::date32()));
}

TEST_F(TestFunctionSignature, TestEqualsReturn) {
  EXPECT_FALSE(FunctionSignature("add", {arrow::int32()}, arrow::int64()) ==
               FunctionSignature("add", {arrow::int32()}, arrow::int32()));
}

TEST_F(TestFunctionSignature, TestHash) {
  FunctionSignature f1("myfunc", {list_type1_}, large_list_type1_);
  FunctionSignature f2("myfunc", {list_type2_}, large_list_type2_);
  EXPECT_EQ(f1.Hash(), f2.Hash());

  FunctionSignature f3("extractDay", {arrow::int64()}, arrow::int64());
  FunctionSignature f4("extractday", {arrow::int64()}, arrow::int64());
  EXPECT_EQ(f3.Hash(), f4.Hash());
}

}  // namespace gandiva
