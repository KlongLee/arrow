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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)

#endif

// [[arrow::export]]
std::shared_ptr<arrow::dataset::FieldExpression> dataset___expr__field_ref(std::string name) {
  return std::make_shared<arrow::dataset::FieldExpression>(std::move(name));
}

// [[arrow::export]]
std::shared_ptr<arrow::dataset::ComparisonExpression> dataset___expr__equal(
    const std::shared_ptr<arrow::dataset::Expression>& lhs,
    const std::shared_ptr<arrow::dataset::Expression>& rhs) {
  return arrow::dataset::equal(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<arrow::dataset::ScalarExpression> dataset___expr__scalar(Rcpp::NumericVector x) {
  // TODO(npr) OMG this is a terrible hack
  return arrow::dataset::scalar(x[0]);
}
