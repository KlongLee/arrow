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

Result<fs::FileInfoVector> GetGlobFiles(const std::shared_ptr<fs::FileSystem>& filesystem,
                                        std::string& path) {
  fs::FileInfoVector results, temp;
  fs::FileSelector selector;
  std::string cur;
  size_t i = 0;

#if _WIN32
  auto split_path = fs::internal::SplitAbstractPath(path, '\\');
  ARROW_ASSIGN_OR_RAISE(auto file, filesystem->GetFileInfo(split_path[i++] + "\\"));
#else
  auto split_path = fs::internal::SplitAbstractPath(path, '/');
  ARROW_ASSIGN_OR_RAISE(auto file, filesystem->GetFileInfo("/"));
#endif
  results.push_back(std::move(file));

  for (; i < split_path.size(); i++) {
    if (split_path[i].find_first_of("*?") == std::string::npos) {
      if (cur.empty())
        cur = split_path[i];
      else
        cur = fs::internal::ConcatAbstractPath(cur, split_path[i]);
      continue;
    } else {
      for (auto res : results) {
        if (res.type() != fs::FileType::Directory) continue;
        selector.base_dir = res.path() + cur;
        ARROW_ASSIGN_OR_RAISE(auto entries, filesystem->GetFileInfo(selector));
        fs::internal::Globber globber(
            fs::internal::ConcatAbstractPath(selector.base_dir, split_path[i]));
        for (auto entry : entries) {
          if (globber.Matches(entry.path())) {
            temp.push_back(std::move(entry));
          }
        }
      }
      results = temp;
      temp.clear();
      cur.clear();
    }
  }

  if (!cur.empty()) {
    for (size_t i = 0; i < results.size(); i++) {
      ARROW_ASSIGN_OR_RAISE(
          results[i], filesystem->GetFileInfo(
                          fs::internal::ConcatAbstractPath(results[i].path(), cur)));
    }
  }

  return results;
}

Result<compute::Declaration> FromProto(const substrait::Rel& rel,
                                       const ExtensionSet& ext_set) {
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
      std::vector<fs::FileInfo> files;

      for (const auto& item : read.local_files().items()) {
        std::string path;
        if (item.path_type_case() ==
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriPath) {
          path = item.uri_path();
        } else if (item.path_type_case() ==
                   substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile) {
          path = item.uri_file();
        } else if (item.path_type_case() ==
                   substrait::ReadRel_LocalFiles_FileOrFiles::kUriFolder) {
          path = item.uri_folder();
        } else {
          path = item.uri_path_glob();
        }

        if (item.format() ==
            substrait::ReadRel::LocalFiles::FileOrFiles::FILE_FORMAT_PARQUET) {
          format = std::make_shared<dataset::ParquetFileFormat>();
        } else if (util::string_view{path}.ends_with(".arrow")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else if (util::string_view{path}.ends_with(".feather")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles::format "
              "other than FILE_FORMAT_PARQUET");
        }

        if (!util::string_view{path}.starts_with("file:///")) {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles::uri_path "
              "with other than local filesystem (file:///)");
        }

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

        path = path.substr(7);
        if (item.path_type_case() ==
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriPath) {
          ARROW_ASSIGN_OR_RAISE(auto file, filesystem->GetFileInfo(path));
          if (file.type() == fs::FileType::File) {
            files.push_back(std::move(file));
          } else if (file.type() == fs::FileType::Directory) {
            fs::FileSelector selector;
            selector.base_dir = path;
            selector.recursive = true;
            ARROW_ASSIGN_OR_RAISE(auto discovered_files,
                                  filesystem->GetFileInfo(selector));
            std::move(files.begin(), files.end(), std::back_inserter(discovered_files));
          }
        }
        if (item.path_type_case() ==
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile) {
          files.emplace_back(path, fs::FileType::File);
        } else if (item.path_type_case() ==
                   substrait::ReadRel_LocalFiles_FileOrFiles::kUriFolder) {
          fs::FileSelector selector;
          selector.base_dir = path;
          selector.recursive = true;
          ARROW_ASSIGN_OR_RAISE(auto discovered_files, filesystem->GetFileInfo(selector));
          std::move(discovered_files.begin(), discovered_files.end(),
                    std::back_inserter(files));
        } else {
          ARROW_ASSIGN_OR_RAISE(auto discovered_files, GetGlobFiles(filesystem, path));
          std::move(discovered_files.begin(), discovered_files.end(),
                    std::back_inserter(files));
        }
      }

      ARROW_ASSIGN_OR_RAISE(auto ds_factory, dataset::FileSystemDatasetFactory::Make(
                                                 std::move(filesystem), std::move(files),
                                                 std::move(format), {}));

      ARROW_ASSIGN_OR_RAISE(auto ds, ds_factory->Finish(std::move(base_schema)));

      return compute::Declaration{
          "scan", dataset::ScanNodeOptions{std::move(ds), std::move(scan_options)}};
    }

    case substrait::Rel::RelTypeCase::kFilter: {
      const auto& filter = rel.filter();
      RETURN_NOT_OK(CheckRelCommon(filter));

      if (!filter.has_input()) {
        return Status::Invalid("substrait::FilterRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(filter.input(), ext_set));

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
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(project.input(), ext_set));

      std::vector<compute::Expression> expressions;
      for (const auto& expr : project.expressions()) {
        expressions.emplace_back();
        ARROW_ASSIGN_OR_RAISE(expressions.back(), FromProto(expr, ext_set));
      }

      return compute::Declaration::Sequence({
          std::move(input),
          {"project", compute::ProjectNodeOptions{std::move(expressions)}},
      });
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::compute::Declaration from Substrait relation ",
      rel.DebugString());
}

}  // namespace engine
}  // namespace arrow
