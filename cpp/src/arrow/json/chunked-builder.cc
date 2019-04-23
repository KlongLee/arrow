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

#include "arrow/json/chunked-builder.h"

#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/json/converter.h"
#include "arrow/table.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"
#include "arrow/util/task-group.h"

namespace arrow {

using internal::make_unique;

namespace json {

class NonNestedChunkedArrayBuilder : public ChunkedArrayBuilder {
 public:
  NonNestedChunkedArrayBuilder(const std::shared_ptr<internal::TaskGroup>& task_group,
                               std::shared_ptr<Converter> converter)
      : ChunkedArrayBuilder(task_group), converter_(std::move(converter)) {}

  Status Finish(const std::vector<int64_t>& chunk_lengths,
                std::shared_ptr<ChunkedArray>* out) override {
    RETURN_NOT_OK(task_group_->Finish());
    chunks_.resize(chunk_lengths.size(), nullptr);
    for (size_t i = 0; i < chunk_lengths.size(); ++i) {
      if (chunks_[i] != nullptr) {
        continue;
      }
      auto null_chunk = std::make_shared<NullArray>(chunk_lengths[i]);
      RETURN_NOT_OK(converter_->Convert(null_chunk, &chunks_[i]));
    }
    *out = std::make_shared<ChunkedArray>(std::move(chunks_), converter_->out_type());
    chunks_.clear();
    return Status::OK();
  }

 protected:
  ArrayVector chunks_;
  std::mutex mutex_;
  std::shared_ptr<Converter> converter_;
};

class TypedChunkedArrayBuilder : public NonNestedChunkedArrayBuilder {
 public:
  using NonNestedChunkedArrayBuilder::NonNestedChunkedArrayBuilder;

  void Insert(int64_t block_index, const std::shared_ptr<Field>&,
              const std::shared_ptr<Array>& unconverted) override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (chunks_.size() <= static_cast<size_t>(block_index)) {
      chunks_.resize(static_cast<size_t>(block_index) + 1, nullptr);
    }
    lock.unlock();

    task_group_->Append([this, block_index, unconverted] {
      std::shared_ptr<Array> converted;
      RETURN_NOT_OK(converter_->Convert(unconverted, &converted));
      std::unique_lock<std::mutex> lock(mutex_);
      chunks_[block_index] = std::move(converted);
      return Status::OK();
    });
  }
};

class InferringChunkedArrayBuilder : public NonNestedChunkedArrayBuilder {
 public:
  InferringChunkedArrayBuilder(const std::shared_ptr<internal::TaskGroup>& task_group,
                               const PromotionGraph* promotion_graph,
                               std::shared_ptr<Converter> converter)
      : NonNestedChunkedArrayBuilder(task_group, std::move(converter)),
        promotion_graph_(promotion_graph) {}

  void Insert(int64_t block_index, const std::shared_ptr<Field>& unconverted_field,
              const std::shared_ptr<Array>& unconverted) override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (chunks_.size() <= static_cast<size_t>(block_index)) {
      chunks_.resize(static_cast<size_t>(block_index) + 1, nullptr);
      unconverted_.resize(chunks_.size(), nullptr);
      unconverted_fields_.resize(chunks_.size(), nullptr);
    }
    unconverted_[block_index] = unconverted;
    unconverted_fields_[block_index] = unconverted_field;
    lock.unlock();
    ScheduleConvertChunk(block_index);
  }

  void ScheduleConvertChunk(int64_t block_index) {
    task_group_->Append([this, block_index] {
      return TryConvertChunk(static_cast<size_t>(block_index));
    });
  }

  Status TryConvertChunk(size_t block_index) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto converter = converter_;
    auto unconverted = unconverted_[block_index];
    auto unconverted_field = unconverted_fields_[block_index];
    std::shared_ptr<Array> converted;

    lock.unlock();
    Status st = converter->Convert(unconverted, &converted);
    lock.lock();

    if (converter != converter_) {
      // another task promoted converter; reconvert
      lock.unlock();
      ScheduleConvertChunk(block_index);
      return Status::OK();
    }

    if (st.ok()) {
      // conversion succeeded
      chunks_[block_index] = std::move(converted);
      return Status::OK();
    }

    auto promoted_type =
        promotion_graph_->Promote(converter_->out_type(), unconverted_field);
    if (promoted_type == nullptr) {
      // converter failed, no promotion available
      return st;
    }
    RETURN_NOT_OK(MakeConverter(promoted_type, converter_->pool(), &converter_));

    size_t nchunks = chunks_.size();
    for (size_t i = 0; i < nchunks; ++i) {
      if (i != block_index && chunks_[i]) {
        // We're assuming the chunk was converted using the wrong type
        // (which should be true unless the executor reorders tasks)
        chunks_[i].reset();
        lock.unlock();
        ScheduleConvertChunk(i);
        lock.lock();
      }
    }
    lock.unlock();
    ScheduleConvertChunk(block_index);
    return Status::OK();
  }

  Status Finish(const std::vector<int64_t>& chunk_lengths,
                std::shared_ptr<ChunkedArray>* out) override {
    RETURN_NOT_OK(NonNestedChunkedArrayBuilder::Finish(chunk_lengths, out));
    unconverted_.clear();
    return Status::OK();
  }

 private:
  ArrayVector unconverted_;
  std::vector<std::shared_ptr<Field>> unconverted_fields_;
  const PromotionGraph* promotion_graph_;
};

class ChunkedListArrayBuilder : public ChunkedArrayBuilder {
 public:
  ChunkedListArrayBuilder(const std::shared_ptr<internal::TaskGroup>& task_group,
                          MemoryPool* pool,
                          std::unique_ptr<ChunkedArrayBuilder> value_builder,
                          util::string_view field_name)
      : ChunkedArrayBuilder(task_group),
        pool_(pool),
        value_builder_(std::move(value_builder)),
        field_name_(field_name) {}

  void Insert(int64_t block_index, const std::shared_ptr<Field>&,
              const std::shared_ptr<Array>& unconverted) override {
    auto list_array = static_cast<const ListArray*>(unconverted.get());
    value_builder_->Insert(block_index, list_array->list_type()->value_field(),
                           list_array->values());

    std::unique_lock<std::mutex> lock(mutex_);
    if (null_bitmap_chunks_.size() <= static_cast<size_t>(block_index)) {
      null_bitmap_chunks_.resize(static_cast<size_t>(block_index) + 1, nullptr);
      offset_chunks_.resize(null_bitmap_chunks_.size(), nullptr);
    }
    null_bitmap_chunks_[block_index] = unconverted->null_bitmap();
    offset_chunks_[block_index] = list_array->value_offsets();
  }

  Status Finish(const std::vector<int64_t>& chunk_lengths,
                std::shared_ptr<ChunkedArray>* out) override {
    RETURN_NOT_OK(task_group_->Finish());

    // any child chunks which haven't been inserted will be empty
    std::vector<int64_t> child_chunk_lengths(chunk_lengths.size(), 0);
    std::shared_ptr<ChunkedArray> child_array;
    RETURN_NOT_OK(value_builder_->Finish(child_chunk_lengths, &child_array));

    // add null bitmaps and offsets for chunks which haven't been inserted
    null_bitmap_chunks_.resize(chunk_lengths.size(), nullptr);
    offset_chunks_.resize(null_bitmap_chunks_.size(), nullptr);

    for (size_t i = 0; i < chunk_lengths.size(); ++i) {
      if (null_bitmap_chunks_[i] != nullptr) {
        continue;
      }
      RETURN_NOT_OK(AllocateBitmap(pool_, chunk_lengths[i], &null_bitmap_chunks_[i]));
      std::memset(null_bitmap_chunks_[i]->mutable_data(), 0,
                  null_bitmap_chunks_[i]->size());
      int64_t offsets_length = (chunk_lengths[i] + 1) * sizeof(int32_t);
      RETURN_NOT_OK(AllocateBuffer(pool_, offsets_length, &offset_chunks_[i]));
      std::memset(offset_chunks_[i]->mutable_data(), 0, offsets_length);
    }

    auto type = list(field(field_name_, child_array->type()));
    ArrayVector chunks(null_bitmap_chunks_.size());
    for (size_t i = 0; i < null_bitmap_chunks_.size(); ++i) {
      auto child_chunk = child_array->chunk(static_cast<int>(i));
      chunks[i] =
          std::make_shared<ListArray>(type, child_chunk->length(), offset_chunks_[i],
                                      child_chunk, null_bitmap_chunks_[i]);
    }

    *out = std::make_shared<ChunkedArray>(std::move(chunks), type);
    return Status::OK();
  }

 private:
  std::mutex mutex_;
  MemoryPool* pool_;
  std::unique_ptr<ChunkedArrayBuilder> value_builder_;
  BufferVector offset_chunks_, null_bitmap_chunks_;
  std::string field_name_;
};

class ChunkedStructArrayBuilder : public ChunkedArrayBuilder {
 public:
  ChunkedStructArrayBuilder(
      const std::shared_ptr<internal::TaskGroup>& task_group, MemoryPool* pool,
      const PromotionGraph* promotion_graph,
      std::vector<std::pair<std::string, std::unique_ptr<ChunkedArrayBuilder>>>
          name_builders)
      : ChunkedArrayBuilder(task_group), pool_(pool), promotion_graph_(promotion_graph) {
    for (auto&& name_builder : name_builders) {
      auto index = static_cast<int>(name_to_index_.size());
      name_to_index_.emplace(std::move(name_builder.first), index);
      child_builders_.emplace_back(std::move(name_builder.second));
    }
  }

  void Insert(int64_t block_index, const std::shared_ptr<Field>&,
              const std::shared_ptr<Array>& unconverted) override {
    std::unique_lock<std::mutex> lock(mutex_);

    auto struct_array = std::static_pointer_cast<StructArray>(unconverted);
    if (promotion_graph_ == nullptr) {
      // If unexpected fields are ignored or result in an error then all parsers will emit
      // columns exclusively in the ordering specified in ParseOptions::explicit_schema,
      // so child_builders_ is immutable and no associative lookup is necessary.
      for (int i = 0; i < unconverted->num_fields(); ++i) {
        child_builders_[i]->Insert(block_index, unconverted->type()->child(i),
                                   struct_array->field(i));
      }
    } else {
      auto st = InsertChildren(block_index, struct_array.get());
      if (!st.ok()) {
        return task_group_->Append([st] { return st; });
      }
    }

    if (null_bitmap_chunks_.size() <= static_cast<size_t>(block_index)) {
      null_bitmap_chunks_.resize(static_cast<size_t>(block_index) + 1, nullptr);
      chunk_lengths_.resize(null_bitmap_chunks_.size(), -1);
    }
    null_bitmap_chunks_[block_index] = unconverted->null_bitmap();
    chunk_lengths_[block_index] = unconverted->length();
  }

  // Insert children associatively by name; the unconverted block may have unexpected or
  // differently ordered fields
  Status InsertChildren(int64_t block_index, const StructArray* unconverted) {
    const auto& fields = unconverted->type()->children();

    for (int i = 0; i < unconverted->num_fields(); ++i) {
      auto it = name_to_index_.find(fields[i]->name());

      if (it == name_to_index_.end()) {
        // add a new field to this builder
        auto type = promotion_graph_->Infer(fields[i]);
        DCHECK_NE(type, nullptr)
            << "invalid unconverted_field encountered in conversion: "
            << fields[i]->name() << ":" << *fields[i]->type();

        it = name_to_index_.emplace(fields[i]->name(), name_to_index_.size()).first;

        std::unique_ptr<ChunkedArrayBuilder> child_builder;
        RETURN_NOT_OK(MakeChunkedArrayBuilder(task_group_, pool_, promotion_graph_, type,
                                              &child_builder));
        child_builders_.emplace_back(std::move(child_builder));
      }

      auto unconverted_field = unconverted->type()->child(i);
      child_builders_[it->second]->Insert(block_index, unconverted_field,
                                          unconverted->field(i));
    }

    return Status::OK();
  }

  Status Finish(const std::vector<int64_t>& parent_chunk_lengths,
                std::shared_ptr<ChunkedArray>* out) override {
    RETURN_NOT_OK(task_group_->Finish());

    const auto& chunk_lengths =
        parent_chunk_lengths.size() == 0 ? chunk_lengths_ : parent_chunk_lengths;

    std::vector<std::shared_ptr<Field>> fields(name_to_index_.size());
    std::vector<std::shared_ptr<ChunkedArray>> child_arrays(name_to_index_.size());
    for (auto&& name_index : name_to_index_) {
      auto child_builder = child_builders_[name_index.second].get();

      std::shared_ptr<ChunkedArray> child_array;
      RETURN_NOT_OK(child_builder->Finish(chunk_lengths, &child_array));

      child_arrays[name_index.second] = child_array;
      fields[name_index.second] = field(name_index.first, child_array->type());
    }

    auto type = struct_(std::move(fields));
    ArrayVector chunks(null_bitmap_chunks_.size());
    for (size_t i = 0; i < null_bitmap_chunks_.size(); ++i) {
      ArrayVector child_chunks;
      for (const auto& child_array : child_arrays) {
        child_chunks.push_back(child_array->chunk(i));
      }
      chunks[i] = std::make_shared<StructArray>(type, chunk_lengths_[i], child_chunks,
                                                null_bitmap_chunks_[i]);
    }

    *out = std::make_shared<ChunkedArray>(std::move(chunks), type);
    return Status::OK();
  }

 private:
  std::mutex mutex_;
  MemoryPool* pool_;
  const PromotionGraph* promotion_graph_;
  std::unordered_map<std::string, int> name_to_index_;
  std::vector<std::unique_ptr<ChunkedArrayBuilder>> child_builders_;
  BufferVector null_bitmap_chunks_;
  std::vector<int64_t> chunk_lengths_;
};

Status MakeChunkedArrayBuilder(const std::shared_ptr<internal::TaskGroup>& task_group,
                               MemoryPool* pool, const PromotionGraph* promotion_graph,
                               const std::shared_ptr<DataType>& type,
                               std::unique_ptr<ChunkedArrayBuilder>* out) {
  if (type->id() == Type::STRUCT) {
    std::vector<std::pair<std::string, std::unique_ptr<ChunkedArrayBuilder>>>
        child_builders;
    for (const auto& f : type->children()) {
      std::unique_ptr<ChunkedArrayBuilder> child_builder;
      RETURN_NOT_OK(MakeChunkedArrayBuilder(task_group, pool, promotion_graph, f->type(),
                                            &child_builder));
      child_builders.emplace_back(f->name(), std::move(child_builder));
    }
    *out = make_unique<ChunkedStructArrayBuilder>(task_group, pool, promotion_graph,
                                                  std::move(child_builders));
    return Status::OK();
  }
  if (type->id() == Type::LIST) {
    auto list_type = static_cast<const ListType*>(type.get());
    std::unique_ptr<ChunkedArrayBuilder> value_builder;
    RETURN_NOT_OK(MakeChunkedArrayBuilder(task_group, pool, promotion_graph,
                                          list_type->value_type(), &value_builder));
    *out = make_unique<ChunkedListArrayBuilder>(
        task_group, pool, std::move(value_builder), list_type->value_field()->name());
    return Status::OK();
  }
  std::shared_ptr<Converter> converter;
  RETURN_NOT_OK(MakeConverter(type, pool, &converter));
  if (promotion_graph) {
    *out = make_unique<InferringChunkedArrayBuilder>(task_group, promotion_graph,
                                                     std::move(converter));
  } else {
    *out = make_unique<TypedChunkedArrayBuilder>(task_group, std::move(converter));
  }
  return Status::OK();
}

}  // namespace json
}  // namespace arrow
