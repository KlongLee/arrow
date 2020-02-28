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

#include "arrow/engine/logical_plan.h"

#include <utility>

#include "arrow/engine/expression.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace engine {

//
// LogicalPlan
//

LogicalPlan::LogicalPlan(std::shared_ptr<Expr> root) : root_(std::move(root)) {
  DCHECK_NE(root_, nullptr);
}

const ExprType& LogicalPlan::type() const { return root()->type(); }

bool LogicalPlan::Equals(const LogicalPlan& other) const {
  if (this == &other) {
    return true;
  }

  return root()->Equals(other.root());
}

std::string LogicalPlan::ToString() const { return root_->ToString(); }

//
// LogicalPlanBuilder
//

LogicalPlanBuilder::LogicalPlanBuilder(LogicalPlanBuilderOptions options)
    : catalog_(options.catalog) {}

using ResultExpr = LogicalPlanBuilder::ResultExpr;

#define ERROR_IF_TYPE(cond, ErrorType, ...)  \
  do {                                       \
    if (ARROW_PREDICT_FALSE(cond)) {         \
      return Status::ErrorType(__VA_ARGS__); \
    }                                        \
  } while (false)

#define ERROR_IF(cond, ...) ERROR_IF_TYPE(cond, Invalid, __VA_ARGS__)
//
// Leaf builder.
//

ResultExpr LogicalPlanBuilder::Scalar(const std::shared_ptr<arrow::Scalar>& scalar) {
  return ScalarExpr::Make(scalar);
}

ResultExpr LogicalPlanBuilder::Field(const std::shared_ptr<Expr>& input,
                                     const std::string& field_name) {
  ERROR_IF(input == nullptr, "Input expression must be non-null");

  auto expr_type = input->type();
  ERROR_IF(!expr_type.IsTable(), "Input expression does not have a Table shape.");

  auto field = expr_type.schema()->GetFieldByName(field_name);
  ERROR_IF_TYPE(field == nullptr, KeyError, "Cannot reference field '", field_name,
                "' in schema.");

  return FieldRefExpr::Make(std::move(field));
}

ResultExpr LogicalPlanBuilder::Field(const std::shared_ptr<Expr>& input,
                                     int field_index) {
  ERROR_IF(input == nullptr, "Input expression must be non-null");
  ERROR_IF_TYPE(field_index < 0, KeyError, "Field index must be positive");

  auto expr_type = input->type();
  ERROR_IF(!expr_type.IsTable(), "Input expression does not have a Table shape.");

  auto schema = expr_type.schema();
  auto num_fields = schema->num_fields();
  ERROR_IF_TYPE(field_index >= num_fields, KeyError, "Field index ", field_index,
                " out of bounds.");

  auto field = expr_type.schema()->field(field_index);
  ERROR_IF_TYPE(field == nullptr, KeyError, "Field at index ", field_index, " is null.");

  return FieldRefExpr::Make(std::move(field));
}

//
// Relational
//

ResultExpr LogicalPlanBuilder::Scan(const std::string& table_name) {
  ERROR_IF(catalog_ == nullptr, "Cannot scan from an empty catalog");
  ARROW_ASSIGN_OR_RAISE(auto table, catalog_->Get(table_name));
  return ScanRelExpr::Make(std::move(table));
}

ResultExpr LogicalPlanBuilder::Filter(const std::shared_ptr<Expr>& input,
                                      const std::shared_ptr<Expr>& predicate) {
  ERROR_IF(input == nullptr, "Input expression can't be null.");
  ERROR_IF(predicate == nullptr, "Predicate expression can't be null.");
  return FilterRelExpr::Make(std::move(input), std::move(predicate));
}

ResultExpr LogicalPlanBuilder::Project(
    const std::shared_ptr<Expr>& input,
    const std::vector<std::shared_ptr<Expr>>& expressions) {
  ERROR_IF(input == nullptr, "Input expression can't be null.");
  ERROR_IF(expressions.empty(), "Must have at least one expression.");
  return ProjectionRelExpr::Make(input, expressions);
}

ResultExpr LogicalPlanBuilder::Mutate(
    const std::shared_ptr<Expr>& input,
    const std::vector<std::shared_ptr<Expr>>& expressions) {
  return Project(input, expressions);
}

ResultExpr LogicalPlanBuilder::Project(const std::shared_ptr<Expr>& input,
                                       const std::vector<std::string>& column_names) {
  ERROR_IF(input == nullptr, "Input expression can't be null.");
  ERROR_IF(column_names.empty(), "Must have at least one column name.");

  std::vector<std::shared_ptr<Expr>> expressions{column_names.size()};
  for (size_t i = 0; i < column_names.size(); i++) {
    ARROW_ASSIGN_OR_RAISE(expressions[i], Field(input, column_names[i]));
  }

  // TODO(fsaintjacques): preserve field names.
  return Project(input, expressions);
}

ResultExpr LogicalPlanBuilder::Select(const std::shared_ptr<Expr>& input,
                                      const std::vector<std::string>& column_names) {
  return Project(input, column_names);
}

ResultExpr LogicalPlanBuilder::Project(const std::shared_ptr<Expr>& input,
                                       const std::vector<int>& column_indices) {
  ERROR_IF(input == nullptr, "Input expression can't be null.");
  ERROR_IF(column_indices.empty(), "Must have at least one column index.");

  std::vector<std::shared_ptr<Expr>> expressions{column_indices.size()};
  for (size_t i = 0; i < column_indices.size(); i++) {
    ARROW_ASSIGN_OR_RAISE(expressions[i], Field(input, column_indices[i]));
  }

  // TODO(fsaintjacques): preserve field names.
  return Project(input, expressions);
}

ResultExpr LogicalPlanBuilder::Select(const std::shared_ptr<Expr>& input,
                                      const std::vector<int>& column_indices) {
  return Project(input, column_indices);
}

#undef ERROR_IF

}  // namespace engine
}  // namespace arrow
