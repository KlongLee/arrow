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
#include <utility>

#include "arrow/array/array_nested.h"
#include "arrow/array/util.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/scanner.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;
using internal::Executor;

namespace dataset {

inline RecordBatchIterator FilterRecordBatch(RecordBatchIterator it, Expression filter,
                                             MemoryPool* pool) {
  return MakeMaybeMapIterator(
      [=](std::shared_ptr<RecordBatch> in) -> Result<std::shared_ptr<RecordBatch>> {
        compute::ExecContext exec_context{pool};
        ARROW_ASSIGN_OR_RAISE(Datum mask,
                              ExecuteScalarExpression(filter, Datum(in), &exec_context));

        if (mask.is_scalar()) {
          const auto& mask_scalar = mask.scalar_as<BooleanScalar>();
          if (mask_scalar.is_valid && mask_scalar.value) {
            return std::move(in);
          }
          return in->Slice(0, 0);
        }

        ARROW_ASSIGN_OR_RAISE(
            Datum filtered,
            compute::Filter(in, mask, compute::FilterOptions::Defaults(), &exec_context));
        return filtered.record_batch();
      },
      std::move(it));
}

inline RecordBatchIterator ProjectRecordBatch(RecordBatchIterator it,
                                              Expression projection, MemoryPool* pool) {
  return MakeMaybeMapIterator(
      [=](std::shared_ptr<RecordBatch> in) -> Result<std::shared_ptr<RecordBatch>> {
        compute::ExecContext exec_context{pool};
        ARROW_ASSIGN_OR_RAISE(Datum projected, ExecuteScalarExpression(
                                                   projection, Datum(in), &exec_context));

        DCHECK_EQ(projected.type()->id(), Type::STRUCT);
        if (projected.shape() == ValueDescr::SCALAR) {
          // Only virtual columns are projected. Broadcast to an array
          ARROW_ASSIGN_OR_RAISE(
              projected, MakeArrayFromScalar(*projected.scalar(), in->num_rows(), pool));
        }

        ARROW_ASSIGN_OR_RAISE(
            auto out, RecordBatch::FromStructArray(projected.array_as<StructArray>()));

        return out->ReplaceSchemaMetadata(in->schema()->metadata());
      },
      std::move(it));
}

class FilterAndProjectScanTask : public ScanTask {
 public:
  explicit FilterAndProjectScanTask(std::shared_ptr<ScanTask> task, Expression partition)
      : ScanTask(task->options(), task->fragment()),
        task_(std::move(task)),
        partition_(std::move(partition)) {}

  Result<RecordBatchIterator> Execute() override {
    ARROW_ASSIGN_OR_RAISE(auto it, task_->Execute());

    ARROW_ASSIGN_OR_RAISE(Expression simplified_filter,
                          SimplifyWithGuarantee(options()->filter, partition_));

    ARROW_ASSIGN_OR_RAISE(Expression simplified_projection,
                          SimplifyWithGuarantee(options()->projection, partition_));

    RecordBatchIterator filter_it =
        FilterRecordBatch(std::move(it), simplified_filter, options_->pool);

    return ProjectRecordBatch(std::move(filter_it), simplified_projection,
                              options_->pool);
  }

 private:
  std::shared_ptr<ScanTask> task_;
  Expression partition_;
};

/// \brief GetScanTaskIterator transforms an Iterator<Fragment> in a
/// flattened Iterator<ScanTask>.
inline Result<ScanTaskIterator> GetScanTaskIterator(
    FragmentIterator fragments, std::shared_ptr<ScanOptions> options) {
  // Fragment -> ScanTaskIterator
  auto fn = [options](std::shared_ptr<Fragment> fragment) -> Result<ScanTaskIterator> {
    ARROW_ASSIGN_OR_RAISE(auto scan_task_it, fragment->Scan(options));

    auto partition = fragment->partition_expression();
    // Apply the filter and/or projection to incoming RecordBatches by
    // wrapping the ScanTask with a FilterAndProjectScanTask
    auto wrap_scan_task =
        [partition](std::shared_ptr<ScanTask> task) -> std::shared_ptr<ScanTask> {
      return std::make_shared<FilterAndProjectScanTask>(std::move(task), partition);
    };

    return MakeMapIterator(wrap_scan_task, std::move(scan_task_it));
  };

  // Iterator<Iterator<ScanTask>>
  auto maybe_scantask_it = MakeMaybeMapIterator(fn, std::move(fragments));

  // Iterator<ScanTask>
  return MakeFlattenIterator(std::move(maybe_scantask_it));
}

inline Status NestedFieldRefsNotImplemented() {
  // TODO(ARROW-11259) Several functions (for example, IpcScanTask::Make) assume that
  // only top level fields will be materialized.
  return Status::NotImplemented("Nested field references in scans.");
}

inline Status SetProjection(ScanOptions* options, const Expression& projection) {
  ARROW_ASSIGN_OR_RAISE(options->projection, projection.Bind(*options->dataset_schema));

  if (options->projection.type()->id() != Type::STRUCT) {
    return Status::Invalid("Projection ", projection.ToString(),
                           " cannot yield record batches");
  }
  options->projected_schema = ::arrow::schema(
      checked_cast<const StructType&>(*options->projection.type()).fields(),
      options->dataset_schema->metadata());

  return Status::OK();
}

inline Status SetProjection(ScanOptions* options, std::vector<Expression> exprs,
                            std::vector<std::string> names) {
  compute::ProjectOptions project_options{std::move(names)};

  for (size_t i = 0; i < exprs.size(); ++i) {
    if (auto ref = exprs[i].field_ref()) {
      if (!ref->name()) return NestedFieldRefsNotImplemented();

      // set metadata and nullability for plain field references
      ARROW_ASSIGN_OR_RAISE(auto field, ref->GetOne(*options->dataset_schema));
      project_options.field_nullability[i] = field->nullable();
      project_options.field_metadata[i] = field->metadata();
    }
  }

  return SetProjection(options,
                       call("project", std::move(exprs), std::move(project_options)));
}

inline Status SetProjection(ScanOptions* options, std::vector<std::string> names) {
  std::vector<Expression> exprs(names.size());
  for (size_t i = 0; i < exprs.size(); ++i) {
    exprs[i] = field_ref(names[i]);
  }
  return SetProjection(options, std::move(exprs), std::move(names));
}

inline Status SetFilter(ScanOptions* options, const Expression& filter) {
  for (const auto& ref : FieldsInExpression(filter)) {
    if (!ref.name()) return NestedFieldRefsNotImplemented();

    RETURN_NOT_OK(ref.FindOne(*options->dataset_schema));
  }
  ARROW_ASSIGN_OR_RAISE(options->filter, filter.Bind(*options->dataset_schema));
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
