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

// This module defines an abstract interface for iterating through pages in a
// Parquet column chunk within a row group. It could be extended in the future
// to iterate through all data pages in all chunks in a file.

#include "parquet/bloom_filter_builder.h"

#include <map>
#include <utility>
#include <vector>

#include "arrow/io/interfaces.h"

#include "parquet/bloom_filter.h"
#include "parquet/exception.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"

namespace parquet {

class BloomFilterBuilderImpl : public BloomFilterBuilder {
 public:
  explicit BloomFilterBuilderImpl(const SchemaDescriptor* schema,
                                  WriterProperties properties)
      : schema_(schema), properties_(std::move(properties)) {}
  /// Append a new row group to host all incoming bloom filters.
  void AppendRowGroup() override;

  BloomFilter* GetOrCreateBloomFilter(
      int32_t column_ordinal, const BloomFilterOptions& bloom_filter_options) override;

  /// Serialize all bloom filters with header and bitset in the order of row group and
  /// column id. Column encryption is not implemented yet. The side effect is that it
  /// deletes all bloom filters after they have been flushed.
  void WriteTo(::arrow::io::OutputStream* sink, BloomFilterLocation* location) override;

  void Finish() override { finished_ = true; }

 private:
  /// Make sure column ordinal is not out of bound and the builder is in good state.
  void CheckState(int32_t column_ordinal) const {
    if (finished_) {
      throw ParquetException("BloomFilterBuilder is already finished.");
    }
    if (column_ordinal < 0 || column_ordinal >= schema_->num_columns()) {
      throw ParquetException("Invalid column ordinal: ", column_ordinal);
    }
    if (row_group_bloom_filters_.empty()) {
      throw ParquetException("No row group appended to BloomFilterBuilder.");
    }
    if (schema_->Column(column_ordinal)->physical_type() == Type::BOOLEAN) {
      throw ParquetException("BloomFilterBuilder not supports Boolean.");
    }
  }

  const SchemaDescriptor* schema_;
  WriterProperties properties_;
  bool finished_ = false;

  // vector: row_group_ordinal
  // map: column_ordinal -> bloom filter
  std::vector<std::map<int32_t, std::unique_ptr<BloomFilter>>> row_group_bloom_filters_;
};

std::unique_ptr<BloomFilterBuilder> BloomFilterBuilder::Make(
    const SchemaDescriptor* schema, const WriterProperties& properties) {
  return std::make_unique<BloomFilterBuilderImpl>(schema, properties);
}

void BloomFilterBuilderImpl::AppendRowGroup() { row_group_bloom_filters_.emplace_back(); }

BloomFilter* BloomFilterBuilderImpl::GetOrCreateBloomFilter(
    int32_t column_ordinal, const BloomFilterOptions& bloom_filter_options) {
  CheckState(column_ordinal);
  std::unique_ptr<BloomFilter>& bloom_filter =
      row_group_bloom_filters_.back()[column_ordinal];
  if (bloom_filter == nullptr) {
    auto block_split_bloom_filter =
        std::make_unique<BlockSplitBloomFilter>(properties_.memory_pool());
    block_split_bloom_filter->Init(BlockSplitBloomFilter::OptimalNumOfBytes(
        bloom_filter_options.ndv, bloom_filter_options.fpp));
    bloom_filter = std::move(block_split_bloom_filter);
  }
  return bloom_filter.get();
}

void BloomFilterBuilderImpl::WriteTo(::arrow::io::OutputStream* sink,
                                     BloomFilterLocation* location) {
  if (!finished_) {
    throw ParquetException("Cannot call WriteTo() to unfinished PageIndexBuilder.");
  }
  if (row_group_bloom_filters_.empty()) {
    // Return quickly if there is no bloom filter
    return;
  }

  for (size_t row_group_ordinal = 0; row_group_ordinal < row_group_bloom_filters_.size();
       ++row_group_ordinal) {
    const auto& row_group_bloom_filters = row_group_bloom_filters_[row_group_ordinal];
    // the whole row group has no bloom filter
    if (row_group_bloom_filters.empty()) {
      continue;
    }
    bool has_valid_bloom_filter = false;
    int num_columns = schema_->num_columns();
    std::vector<std::optional<IndexLocation>> locations(num_columns, std::nullopt);

    // serialize bloom filter by ascending order of column id
    for (int32_t column_id = 0; column_id < num_columns; ++column_id) {
      auto iter = row_group_bloom_filters.find(column_id);
      if (iter != row_group_bloom_filters.cend() && iter->second != nullptr) {
        PARQUET_ASSIGN_OR_THROW(int64_t offset, sink->Tell());
        iter->second->WriteTo(sink);
        PARQUET_ASSIGN_OR_THROW(int64_t pos, sink->Tell());
        has_valid_bloom_filter = true;
        locations[column_id] = IndexLocation{offset, static_cast<int32_t>(pos - offset)};
      }
    }
    if (has_valid_bloom_filter) {
      location->bloom_filter_location.emplace(row_group_ordinal, std::move(locations));
    }
  }
}

}  // namespace parquet
