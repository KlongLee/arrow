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

#include "arrow/engine/substrait/relation_internal.h"

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec/options.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"
#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"

namespace arrow {
namespace engine {

static std::shared_ptr<::arrow::dataset::Partitioning> EmptyPartitioning() {
  class EmptyPartitioning : public ::arrow::dataset::Partitioning {
   public:
    EmptyPartitioning() : ::arrow::dataset::Partitioning(::arrow::schema({})) {}

    std::string type_name() const override { return "empty"; }

    Result<compute::Expression> Parse(const std::string& path) const override {
      return compute::literal(true);
    }

    Result<std::string> Format(const compute::Expression& expr) const override {
      return "";
    }

    Result<PartitionedBatches> Partition(
        const std::shared_ptr<RecordBatch>& batch) const override {
      return PartitionedBatches{{batch}, {compute::literal(true)}};
    }
  };

  return std::make_shared<EmptyPartitioning>();
}

template <typename RelMessage>
Status CheckRelCommon(const RelMessage& rel) {
  if (rel.has_common()) {
    if (rel.common().has_emit()) {
      return Status::NotImplemented("substrait::RelCommon::Emit");
    }
    if (rel.common().has_hint()) {
      return Status::NotImplemented("substrait::RelCommon::Hint");
    }
    if (rel.common().has_advanced_extension()) {
      return Status::NotImplemented("substrait::RelCommon::advanced_extension");
    }
  }
  if (rel.has_advanced_extension()) {
    return Status::NotImplemented("substrait AdvancedExtensions");
  }
  return Status::OK();
}

Result<compute::Declaration> FromProtoInternal(const substrait::Rel& rel,
                                               const ExtensionSet& ext_set,
                                               std::vector<std::string>& names) {
  static bool dataset_init = false;
  if (!dataset_init) {
    dataset_init = true;
    dataset::internal::Initialize();
  }

  switch (rel.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kRead: {
      const auto& read = rel.read();
      RETURN_NOT_OK(CheckRelCommon(read));

      ARROW_ASSIGN_OR_RAISE(auto base_schema, FromProto(read.base_schema(), ext_set));

      auto scan_options = std::make_shared<dataset::ScanOptions>();

      if (read.has_filter()) {
        ARROW_ASSIGN_OR_RAISE(scan_options->filter, FromProto(read.filter(), ext_set));
      }

      if (read.has_projection()) {
        // NOTE: scan_options->projection is not used by the scanner and thus can't be
        // used for this
        return Status::NotImplemented("substrait::ReadRel::projection");
      }

      if (!read.has_local_files()) {
        return Status::NotImplemented(
            "substrait::ReadRel with read_type other than LocalFiles");
      }

      if (read.local_files().has_advanced_extension()) {
        return Status::NotImplemented(
            "substrait::ReadRel::LocalFiles::advanced_extension");
      }

      std::shared_ptr<dataset::FileFormat> format;
      auto filesystem = std::make_shared<fs::LocalFileSystem>();
      std::vector<std::shared_ptr<dataset::FileFragment>> fragments;

      for (const auto& item : read.local_files().items()) {
        if (item.path_type_case() !=
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile) {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles with "
              "path_type other than uri_file");
        }

        util::string_view uri_file{item.uri_file()};

        if (item.format() ==
            substrait::ReadRel::LocalFiles::FileOrFiles::FILE_FORMAT_PARQUET) {
          format = std::make_shared<dataset::ParquetFileFormat>();
        } else if (uri_file.ends_with(".arrow")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else if (uri_file.ends_with(".feather")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles::format "
              "other than FILE_FORMAT_PARQUET and not recognized");
        }

        if (!uri_file.starts_with("file:///")) {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles::uri_file "
              "with other than local filesystem (file:///)");
        }
        auto path = item.uri_file().substr(7);

        if (item.partition_index() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::partition_index");
        }

        if (item.start() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::start offset");
        }

        if (item.length() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::length");
        }

        ARROW_ASSIGN_OR_RAISE(auto fragment, format->MakeFragment(dataset::FileSource{
                                                 std::move(path), filesystem}));
        fragments.push_back(std::move(fragment));
      }

      ARROW_ASSIGN_OR_RAISE(
          auto ds, dataset::FileSystemDataset::Make(
                       std::move(base_schema), /*root_partition=*/compute::literal(true),
                       std::move(format), std::move(filesystem), std::move(fragments)));

      return compute::Declaration{
          "scan", dataset::ScanNodeOptions{std::move(ds), std::move(scan_options)}};
    }

    case substrait::Rel::RelTypeCase::kWrite: {
      const auto& write = rel.write();
      RETURN_NOT_OK(CheckRelCommon(write));

      if (!write.has_input()) {
        return Status::Invalid("substrait::WriteRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(write.input(), ext_set, names));

      if (!write.has_local_files()) {
        return Status::NotImplemented(
            "substrait::WriteRel with write_type other than LocalFiles");
      }

      if (write.local_files().has_advanced_extension()) {
        return Status::NotImplemented(
            "substrait::WriteRel::LocalFiles::advanced_extension");
      }

      std::shared_ptr<dataset::FileFormat> format;
      auto filesystem = std::make_shared<fs::LocalFileSystem>();

      if (write.local_files().items().size() != 1) {
        return Status::NotImplemented(
            "substrait::WriteRel with non-single LocalFiles items");
      }

      dataset::FileSystemDatasetWriteOptions write_options;
      write_options.filesystem = filesystem;
      write_options.partitioning = EmptyPartitioning();

      for (const auto& item : write.local_files().items()) {
        if (item.path_type_case() !=
            substrait::WriteRel_LocalFiles_FileOrFiles::kUriFile) {
          return Status::NotImplemented(
              "substrait::WriteRel::LocalFiles::FileOrFiles with "
              "path_type other than uri_file");
        }

        util::string_view uri_file{item.uri_file()};

        if (item.format() ==
            substrait::WriteRel::LocalFiles::FileOrFiles::FILE_FORMAT_PARQUET) {
          format = std::make_shared<dataset::ParquetFileFormat>();
        } else if (uri_file.ends_with(".arrow")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else if (uri_file.ends_with(".feather")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else {
          return Status::NotImplemented(
              "substrait::WriteRel::LocalFiles::FileOrFiles::format "
              "other than FILE_FORMAT_PARQUET and not recognized");
        }
        write_options.file_write_options = format->DefaultWriteOptions();

        if (!uri_file.starts_with("file:///")) {
          return Status::NotImplemented(
              "substrait::WriteRel::LocalFiles::FileOrFiles::uri_file "
              "with other than local filesystem (file:///)");
        }
        auto path = item.uri_file().substr(7);

        if (item.partition_index() != 0) {
          return Status::NotImplemented(
              "non-default "
              "substrait::WriteRel::LocalFiles::FileOrFiles::partition_index");
        }

        if (item.start_row() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::start_row");
        }

        if (item.number_of_rows() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::number_of_rows");
        }

	auto path_pair = fs::internal::GetAbstractPathParent(path);
        write_options.basename_template = path_pair.second;
	write_options.base_dir = path_pair.first;
      }

      return compute::Declaration::Sequence({
          std::move(input),
	  {"tee", dataset::WriteNodeOptions{std::move(write_options), nullptr}},
      });
    }

    case substrait::Rel::RelTypeCase::kFilter: {
      const auto& filter = rel.filter();
      RETURN_NOT_OK(CheckRelCommon(filter));

      if (!filter.has_input()) {
        return Status::Invalid("substrait::FilterRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(filter.input(), ext_set, names));

      if (!filter.has_condition()) {
        return Status::Invalid("substrait::FilterRel with no condition expression");
      }
      ARROW_ASSIGN_OR_RAISE(auto condition, FromProto(filter.condition(), ext_set));

      return compute::Declaration::Sequence({
          std::move(input),
          {"filter", compute::FilterNodeOptions{std::move(condition)}},
      });
    }

    case substrait::Rel::RelTypeCase::kProject: {
      const auto& project = rel.project();
      RETURN_NOT_OK(CheckRelCommon(project));

      if (!project.has_input()) {
        return Status::Invalid("substrait::ProjectRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input,
                            FromProtoInternal(project.input(), ext_set, names));

      size_t expr_size = static_cast<size_t>(project.expressions_size());
      auto names_begin = names.end() - std::min(expr_size, names.size());
      auto names_iter = names_begin;
      std::vector<std::string> project_names;
      std::vector<compute::Expression> expressions;
      for (const auto& expr : project.expressions()) {
        expressions.emplace_back();
        ARROW_ASSIGN_OR_RAISE(expressions.back(), FromProto(expr, ext_set));
        project_names.push_back(
            names_iter != names.end() ? *names_iter++ : expressions.back().ToString());
      }
      names.erase(names_begin, names.end());

      return compute::Declaration::Sequence({
          std::move(input),
          {"project",
           compute::ProjectNodeOptions{std::move(expressions), std::move(project_names)}
	  },
      });
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::compute::Declaration from Substrait relation ",
      rel.DebugString());
}

Result<compute::Declaration> FromProto(const substrait::Rel& rel,
                                       const ExtensionSet& ext_set,
                                       std::vector<std::string> names) {
  std::vector<std::string> copy_names(names.begin(), names.end());
  return FromProtoInternal(rel, ext_set, copy_names);
}

}  // namespace engine
}  // namespace arrow
