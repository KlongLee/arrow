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

#include "arrow/dataset/file_base.h"

#include <algorithm>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/iterator.h"
#include "arrow/util/task_group.h"

namespace arrow {
namespace dataset {

Result<std::shared_ptr<arrow::io::RandomAccessFile>> FileSource::Open() const {
  if (type() == PATH) {
    return filesystem()->OpenInputFile(path());
  }

  return std::make_shared<::arrow::io::BufferReader>(buffer());
}

Result<std::shared_ptr<Fragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<ScanOptions> options) {
  return MakeFragment(std::move(source), std::move(options), scalar(true));
}

Result<std::shared_ptr<Fragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<Expression> partition_expression) {
  return std::make_shared<FileFragment>(std::move(source), shared_from_this(), options,
                                        std::move(partition_expression));
}
Result<std::shared_ptr<arrow::io::OutputStream>> FileSource::OpenWritable() const {
  if (!writable_) {
    return Status::Invalid("file source '", path(), "' is not writable");
  }

  if (type() == PATH) {
    return filesystem()->OpenOutputStream(path());
  }

  auto b = internal::checked_pointer_cast<ResizableBuffer>(buffer());
  return std::make_shared<::arrow::io::BufferOutputStream>(b);
}

Result<std::shared_ptr<WriteTask>> FileFormat::WriteFragment(
    FileSource destination, std::shared_ptr<Fragment> fragment) {
  return Status::NotImplemented("writing fragment of format ", type_name());
}

Result<ScanTaskIterator> FileFragment::Scan(std::shared_ptr<ScanContext> context) {
  return format_->ScanFile(source_, scan_options_, std::move(context));
}

FileSystemDataset::FileSystemDataset(std::shared_ptr<Schema> schema,
                                     std::shared_ptr<Expression> root_partition,
                                     std::shared_ptr<FileFormat> format,
                                     std::shared_ptr<fs::FileSystem> filesystem,
                                     fs::PathForest forest,
                                     ExpressionVector file_partitions)
    : Dataset(std::move(schema), std::move(root_partition)),
      format_(std::move(format)),
      filesystem_(std::move(filesystem)),
      forest_(std::move(forest)),
      partitions_(std::move(file_partitions)) {
  DCHECK_EQ(static_cast<size_t>(forest_.size()), partitions_.size());
}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    std::vector<fs::FileInfo> infos) {
  ExpressionVector partitions(infos.size(), scalar(true));
  return Make(std::move(schema), std::move(root_partition), std::move(format),
              std::move(filesystem), std::move(infos), std::move(partitions));
}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    std::vector<fs::FileInfo> infos, ExpressionVector partitions) {
  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::Make(std::move(infos), &partitions));
  return Make(std::move(schema), std::move(root_partition), std::move(format),
              std::move(filesystem), std::move(forest), std::move(partitions));
}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    fs::PathForest forest, ExpressionVector partitions) {
  return std::shared_ptr<FileSystemDataset>(new FileSystemDataset(
      std::move(schema), std::move(root_partition), std::move(format),
      std::move(filesystem), std::move(forest), std::move(partitions)));
}

std::vector<std::string> FileSystemDataset::files() const {
  std::vector<std::string> files;

  DCHECK_OK(forest_.Visit([&](fs::PathForest::Ref ref) {
    if (ref.info().IsFile()) {
      files.push_back(ref.info().path());
    }
    return Status::OK();
  }));

  return files;
}

using arrow::internal::GetCpuThreadPool;
using arrow::internal::TaskGroup;

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Write(
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    const std::vector<std::string>& base_dirs, bool use_threads) {
  auto roots = forest_.roots();
  DCHECK_EQ(static_cast<size_t>(roots.size()), base_dirs.size());

  auto scan_options = ScanOptions::Make(schema_);
  auto task_group =
      use_threads ? TaskGroup::MakeThreaded(GetCpuThreadPool()) : TaskGroup::MakeSerial();

  auto new_files = forest_.stats();

  int root_i = -1;
  auto collect_fragments = [&](fs::PathForest::Ref ref) -> Status {
    if (!ref.parent()) {
      ++root_i;
      new_files[ref.i].set_path(base_dirs[root_i]);
    } else {
      // remove old base dir, tack on new base dir
      const auto& old_root = roots[root_i].stats().path();
      auto rel = fs::internal::RemoveAncestor(old_root, ref.stats().path());
      DCHECK(rel.has_value());

      auto new_path = fs::internal::ConcatAbstractPath(
          base_dirs[root_i], rel->to_string() + "." + format->type_name());
      new_files[ref.i].set_path(std::move(new_path));
    }

    if (ref.stats().IsFile()) {
      // generate a fragment for this file
      FileSource src(ref.stats().path(), filesystem_.get());
      ARROW_ASSIGN_OR_RAISE(auto fragment,
                            format_->MakeFragment(std::move(src), scan_options));

      FileSource dest(new_files[ref.i].path(), filesystem.get());
      ARROW_ASSIGN_OR_RAISE(auto write_task,
                            format->WriteFragment(std::move(dest), std::move(fragment)));

      task_group->Append([write_task] { return write_task->Execute().status(); });
    } else {
      auto new_path = &new_files[ref.i].path();
      task_group->Append([&, new_path] { return filesystem->CreateDir(*new_path); });
    }

    return Status::OK();
  };

  RETURN_NOT_OK(forest_.Visit(collect_fragments));

  RETURN_NOT_OK(task_group->Finish());

  ARROW_ASSIGN_OR_RAISE(auto forest,
                        fs::PathForest::MakeFromPreSorted(std::move(new_files)));

  return Make(schema_, partition_expression_, std::move(format), std::move(filesystem),
              std::move(forest), partitions_);
}

std::string FileSystemDataset::ToString() const {
  std::string repr = "FileSystemDataset:";

  if (forest_.size() == 0) {
    return repr + " []";
  }

  DCHECK_OK(forest_.Visit([&](fs::PathForest::Ref ref) {
    repr += "\n" + ref.info().path();

    if (!partitions_[ref.i]->Equals(true)) {
      repr += ": " + partitions_[ref.i]->ToString();
    }

    return Status::OK();
  }));

  return repr;
}

util::optional<std::pair<std::string, std::shared_ptr<Scalar>>> GetKey(
    const Expression& expr) {
  if (expr.type() != ExpressionType::COMPARISON) {
    return util::nullopt;
  }

  const auto& cmp = internal::checked_cast<const ComparisonExpression&>(expr);
  if (cmp.op() != compute::CompareOperator::EQUAL) {
    return util::nullopt;
  }

  // TODO(bkietz) allow this ordering to be flipped

  if (cmp.left_operand()->type() != ExpressionType::FIELD) {
    return util::nullopt;
  }

  if (cmp.right_operand()->type() != ExpressionType::SCALAR) {
    return util::nullopt;
  }

  return std::make_pair(
      internal::checked_cast<const FieldExpression&>(*cmp.left_operand()).name(),
      internal::checked_cast<const ScalarExpression&>(*cmp.right_operand()).value());
}

std::shared_ptr<Expression> FoldingAnd(const std::shared_ptr<Expression>& l,
                                       const std::shared_ptr<Expression>& r) {
  if (l->Equals(true)) return r;
  if (r->Equals(true)) return l;
  return and_(l, r);
}

FragmentIterator FileSystemDataset::GetFragmentsImpl(
    std::shared_ptr<ScanOptions> root_options) {
  FragmentVector fragments;
  std::vector<std::shared_ptr<ScanOptions>> options(forest_.size());

  ExpressionVector fragment_partitions(forest_.size());

  auto collect_fragments = [&](fs::PathForest::Ref ref) -> fs::PathForest::MaybePrune {
    auto partition = partitions_[ref.i];

    // if available, copy parent's filter and projector
    // (which are appropriately simplified and loaded with default values)
    if (auto parent = ref.parent()) {
      options[ref.i].reset(new ScanOptions(*options[parent.i]));
      fragment_partitions[ref.i] =
          FoldingAnd(fragment_partitions[parent.i], partitions_[ref.i]);
    } else {
      options[ref.i].reset(new ScanOptions(*root_options));
      fragment_partitions[ref.i] = FoldingAnd(partition_expression_, partitions_[ref.i]);
    }

    // simplify filter by partition information
    auto filter = options[ref.i]->filter->Assume(partition);
    options[ref.i]->filter = filter;

    if (filter->IsNull() || filter->Equals(false)) {
      // directories (and descendants) which can't satisfy the filter are pruned
      return fs::PathForest::Prune;
    }

    // if possible, extract a partition key and pass it to the projector
    auto projector = &options[ref.i]->projector;
    if (auto name_value = GetKey(*partition)) {
      auto index = projector->schema()->GetFieldIndex(name_value->first);
      if (index != -1) {
        RETURN_NOT_OK(projector->SetDefaultValue(index, std::move(name_value->second)));
      }
    }

    if (ref.info().IsFile()) {
      // generate a fragment for this file
      FileSource src(ref.info().path(), filesystem_.get());
      ARROW_ASSIGN_OR_RAISE(
          auto fragment,
          format_->MakeFragment(src, options[ref.i], fragment_partitions[ref.i]));
      fragments.push_back(std::move(fragment));
    }

    return fs::PathForest::Continue;
  };

  auto status = forest_.Visit(collect_fragments);
  if (!status.ok()) {
    return MakeErrorIterator<std::shared_ptr<Fragment>>(status);
  }

  return MakeVectorIterator(std::move(fragments));
}

}  // namespace dataset
}  // namespace arrow
