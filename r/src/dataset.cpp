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

using Rcpp::CharacterVector;
using Rcpp::String;

// Dataset, UnionDataset, FileSystemDataset

// [[arrow::export]]
std::shared_ptr<ds::ScannerBuilder> dataset___Dataset__NewScan(
    const std::shared_ptr<ds::Dataset>& ds) {
  return VALUE_OR_STOP(ds->NewScan());
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___Dataset__schema(
    const std::shared_ptr<ds::Dataset>& dataset) {
  return dataset->schema();
}

// [[arrow::export]]
std::string dataset___Dataset__type_name(const std::shared_ptr<ds::Dataset>& dataset) {
  return dataset->type_name();
}

// [[arrow::export]]
std::shared_ptr<ds::UnionDataset> dataset___UnionDataset__create(
    const ds::DatasetVector& datasets, const std::shared_ptr<arrow::Schema>& schm) {
  return VALUE_OR_STOP(ds::UnionDataset::Make(schm, datasets));
}

// [[arrow::export]]
ds::DatasetVector dataset___UnionDataset__children(
    const std::shared_ptr<ds::UnionDataset>& ds) {
  return ds->children();
}

// [[arrow::export]]
std::shared_ptr<ds::FileFormat> dataset___FileSystemDataset__format(
    const std::shared_ptr<ds::FileSystemDataset>& dataset) {
  return dataset->format();
}

// [[arrow::export]]
std::vector<std::string> dataset___FileSystemDataset__files(
    const std::shared_ptr<ds::FileSystemDataset>& dataset) {
  return dataset->files();
}

// DatasetFactory, UnionDatasetFactory, FileSystemDatasetFactory

// [[arrow::export]]
std::shared_ptr<ds::Dataset> dataset___DatasetFactory__Finish1(
    const std::shared_ptr<ds::DatasetFactory>& factory) {
  return VALUE_OR_STOP(factory->Finish());
}

// [[arrow::export]]
std::shared_ptr<ds::Dataset> dataset___DatasetFactory__Finish2(
    const std::shared_ptr<ds::DatasetFactory>& factory,
    const std::shared_ptr<arrow::Schema>& schema) {
  return VALUE_OR_STOP(factory->Finish(schema));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___DatasetFactory__Inspect(
    const std::shared_ptr<ds::DatasetFactory>& factory) {
  return VALUE_OR_STOP(factory->Inspect());
}

// [[arrow::export]]
std::shared_ptr<ds::DatasetFactory> dataset___UnionDatasetFactory__Make(
    const std::vector<std::shared_ptr<ds::DatasetFactory>>& children) {
  return VALUE_OR_STOP(ds::UnionDatasetFactory::Make(children));
}

// [[arrow::export]]
std::shared_ptr<ds::DatasetFactory> dataset___FileSystemDatasetFactory__Make2(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format,
    const std::shared_ptr<ds::Partitioning>& partitioning) {
  // TODO(fsaintjacques): Make options configurable
  auto options = ds::FileSystemFactoryOptions{};
  if (partitioning != nullptr) {
    options.partitioning = partitioning;
  }

  return VALUE_OR_STOP(
      ds::FileSystemDatasetFactory::Make(fs, *selector, format, options));
}

// [[arrow::export]]
std::shared_ptr<ds::DatasetFactory> dataset___FileSystemDatasetFactory__Make1(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format) {
  return dataset___FileSystemDatasetFactory__Make2(fs, selector, format, nullptr);
}

// [[arrow::export]]
std::shared_ptr<ds::DatasetFactory> dataset___FileSystemDatasetFactory__Make3(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format,
    const std::shared_ptr<ds::PartitioningFactory>& factory) {
  // TODO(fsaintjacques): Make options configurable
  auto options = ds::FileSystemFactoryOptions{};
  if (factory != nullptr) {
    options.partitioning = factory;
  }

  return VALUE_OR_STOP(
      ds::FileSystemDatasetFactory::Make(fs, *selector, format, options));
}

// FileFormat, ParquetFileFormat, IpcFileFormat

// [[arrow::export]]
std::string dataset___FileFormat__type_name(
    const std::shared_ptr<ds::FileFormat>& format) {
  return format->type_name();
}

// [[arrow::export]]
std::shared_ptr<ds::ParquetFileFormat> dataset___ParquetFileFormat__Make(
    bool use_buffered_stream, int64_t buffer_size, CharacterVector dict_columns) {
  auto fmt = std::make_shared<ds::ParquetFileFormat>();

  fmt->reader_options.use_buffered_stream = use_buffered_stream;
  fmt->reader_options.buffer_size = buffer_size;

  auto dict_columns_vector = Rcpp::as<std::vector<std::string>>(dict_columns);
  auto& d = fmt->reader_options.dict_columns;
  std::move(dict_columns_vector.begin(), dict_columns_vector.end(),
            std::inserter(d, d.end()));

  return fmt;
}

// [[arrow::export]]
std::shared_ptr<ds::IpcFileFormat> dataset___IpcFileFormat__Make() {
  return std::make_shared<ds::IpcFileFormat>();
}

// DirectoryPartitioning, HivePartitioning

// [[arrow::export]]
std::shared_ptr<ds::Partitioning> dataset___DirectoryPartitioning(
    const std::shared_ptr<arrow::Schema>& schm) {
  return std::make_shared<ds::DirectoryPartitioning>(schm);
}

// [[arrow::export]]
std::shared_ptr<ds::PartitioningFactory> dataset___DirectoryPartitioning__MakeFactory(
    const std::vector<std::string>& field_names) {
  return ds::DirectoryPartitioning::MakeFactory(field_names);
}

// [[arrow::export]]
std::shared_ptr<ds::Partitioning> dataset___HivePartitioning(
    const std::shared_ptr<arrow::Schema>& schm) {
  return std::make_shared<ds::HivePartitioning>(schm);
}

// [[arrow::export]]
std::shared_ptr<ds::PartitioningFactory> dataset___HivePartitioning__MakeFactory() {
  return ds::HivePartitioning::MakeFactory();
}

// ScannerBuilder, Scanner

// [[arrow::export]]
void dataset___ScannerBuilder__Project(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                       const std::vector<std::string>& cols) {
  STOP_IF_NOT_OK(sb->Project(cols));
}

// [[arrow::export]]
void dataset___ScannerBuilder__Filter(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                      const std::shared_ptr<ds::Expression>& expr) {
  // Expressions converted from R's expressions are typed with R's native type,
  // i.e. double, int64_t and bool.
  auto cast_filter = VALUE_OR_STOP(InsertImplicitCasts(*expr, *sb->schema()));
  STOP_IF_NOT_OK(sb->Filter(cast_filter));
}

// [[arrow::export]]
void dataset___ScannerBuilder__UseThreads(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                          bool threads) {
  STOP_IF_NOT_OK(sb->UseThreads(threads));
}

// [[arrow::export]]
void dataset___ScannerBuilder__BatchSize(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                         int64_t batch_size) {
  STOP_IF_NOT_OK(sb->BatchSize(batch_size));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___ScannerBuilder__schema(
    const std::shared_ptr<ds::ScannerBuilder>& sb) {
  return sb->schema();
}

// [[arrow::export]]
std::shared_ptr<ds::Scanner> dataset___ScannerBuilder__Finish(
    const std::shared_ptr<ds::ScannerBuilder>& sb) {
  return VALUE_OR_STOP(sb->Finish());
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> dataset___Scanner__ToTable(
    const std::shared_ptr<ds::Scanner>& scanner) {
  return VALUE_OR_STOP(scanner->ToTable());
}

#endif
