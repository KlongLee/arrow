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
#include "gandiva/parser.h"

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "gandiva/arrow.h"
#include "gandiva/expr_validator.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/llvm_includes.h"
#include "gandiva/llvm_types.h"

namespace gandiva {

class TestParser : public ::testing::Test {
 protected:
  void SetUp() override {
    auto field_x = arrow::field("x", arrow::int32());
    auto field_y = arrow::field("y", arrow::int32());
    auto field_z = arrow::field("z", arrow::int32());
    auto field_v1 = arrow::field("v1", arrow::int64());
    auto field_v2 = arrow::field("v2", arrow::int64());
    auto field_a = arrow::field("a", arrow::boolean());
    auto field_s = arrow::field("s", arrow::utf8());
    auto field_t = arrow::field("t", arrow::time32(arrow::TimeUnit::SECOND));
    auto field_d = arrow::field("d", arrow::date64());

    schema_ = arrow::schema({field_x, field_y, field_z, field_v1, field_v2, field_a,
                             field_s, field_t, field_d});
    auto ret = schema_->GetFieldByName("name");
    parser_ = Parser(schema_);

    validator_ = std::make_unique<ExprValidator>(&types_, schema_);
  }

  Parser parser_{nullptr};
  NodePtr expr_;
  Status status_;
  SchemaPtr schema_;
  llvm::LLVMContext ctx_;
  LLVMTypes types_{ctx_};
  std::unique_ptr<ExprValidator> validator_;
};

Status Validate(ExprValidator& validator, const NodePtr& node) {
  auto field_res = arrow::field("res", node->return_type());
  return validator.Validate(std::make_shared<Expression>(node, field_res));
}

TEST_F(TestParser, TestLiteral) {
  ARROW_EXPECT_OK(parser_.Parse("0123", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const int32) 123");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("65535u16", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const uint16) 65535");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  status_ = parser_.Parse("100000000000000000000", &expr_);
  EXPECT_FALSE(status_.ok());
  EXPECT_EQ(status_.message(), "100000000000000000000:1.1-21: out of range");

  ARROW_EXPECT_OK(parser_.Parse("0.123", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const float) 0.123 raw(3dfbe76d)");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("0.123f32", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const float) 0.123 raw(3dfbe76d)");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("456f64", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const double) 456 raw(407c800000000000)");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("78.999999999f64", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const double) 79 raw(4053bffffffeed1f)");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("78.999999999f32", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const float) 79 raw(429e0000)");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("\"Hello World\"", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const string) 'Hello World'");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("\'Hello World\'", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const string) 'Hello World'");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("\'\t你好\n\'", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const string) '\t你好\n'");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("true", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const bool) 1");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("false", &expr_));
  EXPECT_EQ(expr_->ToString(), "(const bool) 0");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));
}

TEST_F(TestParser, TestField) {
  ARROW_EXPECT_OK(parser_.Parse("a", &expr_));
  EXPECT_EQ(expr_->ToString(), "(bool) a");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  status_ = parser_.Parse("k", &expr_);
  EXPECT_FALSE(status_.ok());
  EXPECT_EQ(status_.message(), "k:1.1: not defined in schema");
}

TEST_F(TestParser, TestInfixFunction) {
  ARROW_EXPECT_OK(parser_.Parse("-2147483648i32", &expr_));
  // negative of INT_MIN is also INT_MIN
  EXPECT_EQ(expr_->ToString(), "int32 negative((const int32) -2147483648)");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  status_ = parser_.Parse("-2147483648u64", &expr_);
  EXPECT_FALSE(status_.ok());
  EXPECT_TRUE(status_.IsTypeError());

  ARROW_EXPECT_OK(parser_.Parse("-0.123", &expr_));
  EXPECT_EQ(expr_->ToString(), "float negative((const float) 0.123 raw(3dfbe76d))");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("-0.123f32", &expr_));
  EXPECT_EQ(expr_->ToString(), "float negative((const float) 0.123 raw(3dfbe76d))");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("x + 1", &expr_));
  EXPECT_EQ(expr_->ToString(), "int32 add((int32) x, (const int32) 1)");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("x+(-x)+(x+1)", &expr_));
  EXPECT_EQ(expr_->ToString(),
            "int32 add(int32 add((int32) x, int32 negative((int32) x)), int32 "
            "add((int32) x, (const int32) 1))");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("x-3*5-y/z+-1|~5", &expr_));
  EXPECT_EQ(expr_->ToString(),
            "int32 bitwise_or(int32 add(int32 subtract(int32 subtract((int32) x, int32 "
            "multiply((const int32) 3, (const int32) 5)), int32 div((int32) y, (int32) "
            "z)), int32 negative((const int32) 1)), int32 bitwise_not((const int32) 5))");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("!((5<6) == (7>=8))", &expr_));
  EXPECT_EQ(expr_->ToString(),
            "bool not(bool equal(bool less_than((const int32) 5, (const int32) 6), bool "
            "greater_than_or_equal_to((const int32) 7, (const int32) 8)))");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("-1-2-3-4-5-6-7-8", &expr_));
  EXPECT_EQ(expr_->ToString(),
            "int32 subtract(int32 subtract(int32 subtract(int32 subtract(int32 "
            "subtract(int32 subtract(int32 subtract(int32 negative((const int32) 1), "
            "(const int32) 2), (const int32) 3), (const int32) 4), (const int32) 5), "
            "(const int32) 6), (const int32) 7), (const int32) 8)");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));
}

TEST_F(TestParser, TestNamedFunction) {
  ARROW_EXPECT_OK(parser_.Parse(
      "not(equal(less_than(5, 6), greater_than_or_equal_to(7, 8)))", &expr_));
  EXPECT_EQ(expr_->ToString(),
            "bool not(bool equal(bool less_than((const int32) 5, (const int32) 6), bool "
            "greater_than_or_equal_to((const int32) 7, (const int32) 8)))");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));
}

TEST_F(TestParser, TestIf) {
  ARROW_EXPECT_OK(parser_.Parse("if(x == 7, x + 5, x - 6)", &expr_));
  EXPECT_EQ(expr_->ToString(),
            "if (bool equal((int32) x, (const int32) 7)) { int32 add((int32) x, (const "
            "int32) 5) } else { int32 subtract((int32) x, (const int32) 6) }");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("if(x == 7) {x + 5} else {x - 6}", &expr_));
  EXPECT_EQ(expr_->ToString(),
            "if (bool equal((int32) x, (const int32) 7)) { int32 add((int32) x, (const "
            "int32) 5) } else { int32 subtract((int32) x, (const int32) 6) }");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));
}

TEST_F(TestParser, TestBoolean) {
  ARROW_EXPECT_OK(parser_.Parse("x <= 7 and x > 2", &expr_));
  EXPECT_EQ(expr_->ToString(),
            "bool less_than_or_equal_to((int32) x, (const int32) 7) && bool "
            "greater_than((int32) x, (const int32) 2)");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("(x <= 7 && x > 2) || (x < 0 && !(x < -10))", &expr_));
  EXPECT_EQ(expr_->ToString(),
            "bool less_than_or_equal_to((int32) x, (const int32) 7) && bool "
            "greater_than((int32) x, (const int32) 2) || bool less_than((int32) x, "
            "(const int32) 0) && bool not(bool less_than((int32) x, int32 "
            "negative((const int32) 10)))");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));
}

TEST_F(TestParser, TestTypeInference) {
  status_ = parser_.Parse(
      "!(3+8*10<10*3+8) && 3<10 and 8<10 or 3<8 && 1>0 || 2>0 and 3>0", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(
      expr_->ToString(),
      "bool not(bool less_than(int32 add((const int32) 3, int32 multiply((const int32) "
      "8, (const int32) 10)), int32 add(int32 multiply((const int32) 10, (const int32) "
      "3), (const int32) 8))) && bool less_than((const int32) 3, (const int32) 10) && "
      "bool less_than((const int32) 8, (const int32) 10) || bool less_than((const int32) "
      "3, (const int32) 8) && bool greater_than((const int32) 1, (const int32) 0) || "
      "bool greater_than((const int32) 2, (const int32) 0) && bool greater_than((const "
      "int32) 3, (const int32) 0)");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("if(v1 > 0, v1, -999)", &expr_));
  EXPECT_EQ(expr_->ToString(),
            "if (bool greater_than((int64) v1, (const int64) 0)) { (int64) v1 } else { "
            "int64 negative((const int64) 999) }");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(
      parser_.Parse("if(v1 > 0 and v2 > 0, v2 * 1000000 / v1, -999)", &expr_));
  EXPECT_EQ(
      expr_->ToString(),
      "if (bool greater_than((int64) v1, (const int64) 0) && bool greater_than((int64) "
      "v2, (const int64) 0)) { int64 div(int64 multiply((int64) v2, (const int64) "
      "1000000), (int64) v1) } else { int64 negative((const int64) 999) }");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));

  ARROW_EXPECT_OK(parser_.Parse("if(v1 > 0, v1+1, 1)", &expr_));
  EXPECT_EQ(expr_->ToString(),
            "if (bool greater_than((int64) v1, (const int64) 0)) { int64 add((int64) v1, "
            "(const int64) 1) } else { (const int64) 1 }");
  ARROW_EXPECT_OK(Validate(*validator_, expr_));
}

}  // namespace gandiva
