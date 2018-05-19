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

#ifndef ARROW_TABLE_BUILDER_H
#define ARROW_TABLE_BUILDER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/visibility.h"

namespace arrow {

class ArrayBuilder;
class MemoryPool;
class RecordBatch;
class Schema;

/// \class RecordBatchBuilder
/// \brief Helper class for creating record batches iteratively given a known
/// schema
class RecordBatchBuilder {
 public:
  /// \brief Create an initialize a RecordBatchBuilder
  /// \param[in] schema The schema for the record batch
  /// \param[in] pool A MemoryPool to use for allocations
  /// \param[in] builder the created builder instance
  static Status Make(const std::shared_ptr<Schema>& schema, MemoryPool* pool,
                     std::unique_ptr<RecordBatchBuilder>* builder);

  /// \brief Create an initialize a RecordBatchBuilder
  /// \param[in] schema The schema for the record batch
  /// \param[in] pool A MemoryPool to use for allocations
  /// \param[in] initial_capacity The initial capacity for the builders
  /// \param[in] builder the created builder instance
  static Status Make(const std::shared_ptr<Schema>& schema, MemoryPool* pool,
                     int64_t initial_capacity,
                     std::unique_ptr<RecordBatchBuilder>* builder);

  /// \brief Get base pointer to field builder
  /// \param i the field index
  /// \return pointer to ArrayBuilder
  ArrayBuilder* GetField(int i) { return raw_field_builders_[i]; }

  /// \brief Return field builder casted to indicated specific builder type
  /// \param i the field index
  /// \return pointer to template type
  template <typename T>
  T* GetFieldAs(int i) {
    return checked_cast<T*>(raw_field_builders_[i]);
  }

  /// \brief Finish current batch and optionally reset
  /// \param[in] reset_builders the resulting RecordBatch
  /// \param[out] batch the resulting RecordBatch
  /// \return Status
  Status Flush(bool reset_builders, std::shared_ptr<RecordBatch>* batch);

  /// \brief Finish current batch and reset
  /// \param[out] batch the resulting RecordBatch
  /// \return Status
  Status Flush(std::shared_ptr<RecordBatch>* batch);

  /// \brief Set the initial capacity for new builders
  void SetInitialCapacity(int64_t capacity);

  /// \brief The initial capacity for builders
  int64_t initial_capacity() const { return initial_capacity_; }

  /// \brief The number of fields in the schema
  int num_fields() const { return schema_->num_fields(); }

  /// \brief The number of fields in the schema
  std::shared_ptr<Schema> schema() const { return schema_; }

 private:
  RecordBatchBuilder(const std::shared_ptr<Schema>& schema, MemoryPool* pool,
                     int64_t initial_capacity);

  Status CreateBuilders();
  Status InitBuilders();

  std::shared_ptr<Schema> schema_;
  int64_t initial_capacity_;
  MemoryPool* pool_;

  std::vector<std::unique_ptr<ArrayBuilder>> field_builders_;
  std::vector<ArrayBuilder*> raw_field_builders_;
};

}  // namespace arrow

#endif  // ARROW_TABLE_BUILDER_H
