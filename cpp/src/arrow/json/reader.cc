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

#include "arrow/json/reader.h"

#include <future>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/readahead.h"
#include "arrow/json/chunked-builder.h"
#include "arrow/json/chunker.h"
#include "arrow/json/converter.h"
#include "arrow/json/parser.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/util/task-group.h"
#include "arrow/util/thread-pool.h"

namespace arrow {

using internal::GetCpuThreadPool;
using internal::ThreadPool;
using io::internal::ReadaheadBuffer;
using io::internal::ReadaheadSpooler;

namespace json {

// read without padding to the left, but pad to the right with block_size/64 bytes
// to leave room for storing scalars from a row which straddles a block boundary
constexpr int32_t kRowsPerBlockGuess = 64;

class SerialTableReader : public TableReader {
 public:
  static constexpr int32_t block_queue_size = 1;

  SerialTableReader(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                    const ReadOptions& read_options, const ParseOptions& parse_options)
      : pool_(pool),
        read_options_(read_options),
        parse_options_(parse_options),
        readahead_(pool_, input, read_options_.block_size, block_queue_size),
        chunker_(Chunker::Make(parse_options_)),
        task_group_(internal::TaskGroup::MakeSerial()) {}

  Status Read(std::shared_ptr<Table>* out) override {
    auto type = parse_options_.explicit_schema
                    ? struct_(parse_options_.explicit_schema->fields())
                    : struct_({});
    auto promotion_graph =
        parse_options_.unexpected_field_behavior == UnexpectedFieldBehavior::InferType
            ? GetPromotionGraph()
            : nullptr;
    RETURN_NOT_OK(
        MakeChunkedArrayBuilder(task_group_, pool_, promotion_graph, type, &builder_));

    ReadaheadBuffer rh;
    RETURN_NOT_OK(readahead_.Read(&rh));
    if (rh.buffer == nullptr) {
      return Status::Invalid("Empty JSON file");
    }

    int64_t block_index = 0;
    for (std::shared_ptr<Buffer> starts_with_whole = rh.buffer;; ++block_index) {
      // get all whole objects entirely inside the current buffer
      std::shared_ptr<Buffer> whole, partial;
      RETURN_NOT_OK(chunker_->Process(starts_with_whole, &whole, &partial));

      std::shared_ptr<ResizableBuffer> storage;
      RETURN_NOT_OK(AllocateScalarStorage(&storage));
      BlockParser parser(pool_, parse_options_, storage);
      RETURN_NOT_OK(parser.Parse(whole));

      RETURN_NOT_OK(readahead_.Read(&rh));

      auto straddling = std::make_shared<Buffer>("");
      if (rh.buffer) {
        // get the completion of a partial row from the previous block
        // FIXME(bkietz) this will just error out if a row spans more than a pair of
        // blocks
        std::shared_ptr<Buffer> completion;
        RETURN_NOT_OK(
            chunker_->Process(partial, rh.buffer, &completion, &starts_with_whole));
        RETURN_NOT_OK(ConcatenateBuffers({partial, completion}, pool_, &straddling));
      }

      if (straddling->size() != 0) {
        RETURN_NOT_OK(parser.Parse(straddling));
      }

      std::shared_ptr<Array> parsed;
      RETURN_NOT_OK(parser.Finish(&parsed));
      builder_->Insert(block_index, field("", parsed->type()), parsed);

      if (rh.buffer == nullptr) {
        break;
      }
    }

    return Finish(out);
  }

  Status Finish(std::shared_ptr<Table>* out) {
    std::shared_ptr<ChunkedArray> array;
    RETURN_NOT_OK(builder_->Finish(&array));

    int num_fields = array->type()->num_children();
    int num_chunks = array->num_chunks();

    std::vector<std::shared_ptr<Column>> columns(num_fields);
    for (int i = 0; i < num_fields; ++i) {
      ArrayVector chunks(num_chunks);
      for (int chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
        chunks[chunk_index] =
            static_cast<const StructArray&>(*array->chunk(chunk_index)).field(i);
      }
      columns[i] = std::make_shared<Column>(array->type()->child(i), chunks);
    }

    *out = Table::Make(schema(array->type()->children()), columns, array->length());
    return Status::OK();
  }

  Status AllocateScalarStorage(std::shared_ptr<ResizableBuffer>* out) {
    auto size = read_options_.block_size * (kRowsPerBlockGuess + 1) / kRowsPerBlockGuess;
    return AllocateResizableBuffer(pool_, size, out);
  }

 private:
  MemoryPool* pool_;
  ReadOptions read_options_;
  ParseOptions parse_options_;
  ReadaheadSpooler readahead_;
  std::unique_ptr<Chunker> chunker_;
  std::shared_ptr<internal::TaskGroup> task_group_;
  std::unique_ptr<ChunkedArrayBuilder> builder_;
};

class ThreadedTableReader : public TableReader {
 public:
  ThreadedTableReader(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                      ThreadPool* thread_pool, const ReadOptions& read_options,
                      const ParseOptions& parse_options)
      : pool_(pool),
        read_options_(read_options),
        parse_options_(parse_options),
        readahead_(pool_, input, read_options_.block_size, thread_pool->GetCapacity()),
        chunker_(Chunker::Make(parse_options_)),
        task_group_(internal::TaskGroup::MakeThreaded(thread_pool)) {}

  Status Read(std::shared_ptr<Table>* out) override {
    auto type = parse_options_.explicit_schema
                    ? struct_(parse_options_.explicit_schema->fields())
                    : struct_({});
    auto promotion_graph =
        parse_options_.unexpected_field_behavior == UnexpectedFieldBehavior::InferType
            ? GetPromotionGraph()
            : nullptr;
    RETURN_NOT_OK(
        MakeChunkedArrayBuilder(task_group_, pool_, promotion_graph, type, &builder_));

    ReadaheadBuffer rh;
    RETURN_NOT_OK(readahead_.Read(&rh));
    if (rh.buffer == nullptr) {
      return Status::Invalid("Empty JSON file");
    }

    int64_t block_index = 0;
    for (std::shared_ptr<Buffer> starts_with_whole = rh.buffer; rh.buffer;
         ++block_index) {
      // get all whole objects entirely inside the current buffer
      std::shared_ptr<Buffer> whole, partial;
      RETURN_NOT_OK(chunker_->Process(starts_with_whole, &whole, &partial));

      // set up a promise for the completion of partial
      struct completion_trap {
        ~completion_trap() { promise.set_value(buffer); }

        std::shared_ptr<Buffer> buffer = std::make_shared<Buffer>("");
        std::promise<std::shared_ptr<Buffer>> promise;
      } completion;
      auto completion_future = completion.promise.get_future().share();

      // launch parse task
      task_group_->Append([this, rh, whole, partial, completion_future, block_index] {
        std::shared_ptr<ResizableBuffer> storage;
        RETURN_NOT_OK(AllocateScalarStorage(&storage));
        BlockParser parser(pool_, parse_options_, storage);
        RETURN_NOT_OK(parser.Parse(whole));

        auto completion = completion_future.get();
        if (completion->size() != 0) {
          std::shared_ptr<Buffer> straddling;
          RETURN_NOT_OK(ConcatenateBuffers({partial, completion}, pool_, &straddling));
          RETURN_NOT_OK(parser.Parse(straddling));
        }

        std::shared_ptr<Array> parsed;
        RETURN_NOT_OK(parser.Finish(&parsed));
        builder_->Insert(block_index, field("", parsed->type()), parsed);
        return Status::OK();
      });

      RETURN_NOT_OK(readahead_.Read(&rh));
      if (rh.buffer) {
        // get the completion of a partial row from the previous block and submit to
        // just-lauched parse task
        // FIXME(bkietz) this will just error out if a row spans more than a pair of
        // blocks
        std::shared_ptr<Buffer> completion;
        RETURN_NOT_OK(
            chunker_->Process(partial, rh.buffer, &completion, &starts_with_whole));
      }
    }

    return Finish(out);
  }

  Status Finish(std::shared_ptr<Table>* out) {
    std::shared_ptr<ChunkedArray> array;
    RETURN_NOT_OK(builder_->Finish(&array));

    int num_fields = array->type()->num_children();
    int num_chunks = array->num_chunks();

    std::vector<std::shared_ptr<Column>> columns(num_fields);
    for (int i = 0; i < num_fields; ++i) {
      ArrayVector chunks(num_chunks);
      for (int chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
        chunks[chunk_index] =
            static_cast<const StructArray&>(*array->chunk(chunk_index)).field(i);
      }
      columns[i] = std::make_shared<Column>(array->type()->child(i), chunks);
    }

    *out = Table::Make(schema(array->type()->children()), columns, array->length());
    return Status::OK();
  }

  Status AllocateScalarStorage(std::shared_ptr<ResizableBuffer>* out) {
    auto size = read_options_.block_size * (kRowsPerBlockGuess + 1) / kRowsPerBlockGuess;
    return AllocateResizableBuffer(pool_, size, out);
  }

 private:
  MemoryPool* pool_;
  ReadOptions read_options_;
  ParseOptions parse_options_;
  ReadaheadSpooler readahead_;
  std::unique_ptr<Chunker> chunker_;
  std::shared_ptr<internal::TaskGroup> task_group_;
  std::unique_ptr<ChunkedArrayBuilder> builder_;
};

Status TableReader::Make(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                         const ReadOptions& read_options,
                         const ParseOptions& parse_options,
                         std::shared_ptr<TableReader>* out) {
  if (read_options.use_threads) {
    *out = std::make_shared<ThreadedTableReader>(pool, input, GetCpuThreadPool(),
                                                 read_options, parse_options);
  } else {
    *out = std::make_shared<SerialTableReader>(pool, input, read_options, parse_options);
  }
  return Status::OK();
}

Status ParseOne(ParseOptions options, std::shared_ptr<Buffer> json,
                std::shared_ptr<RecordBatch>* out) {
  std::shared_ptr<ResizableBuffer> storage;
  RETURN_NOT_OK(AllocateResizableBuffer(default_memory_pool(), json->size(), &storage));
  BlockParser parser(default_memory_pool(), options, storage);
  RETURN_NOT_OK(parser.Parse(json));
  std::shared_ptr<Array> parsed;
  RETURN_NOT_OK(parser.Finish(&parsed));

  auto type =
      options.explicit_schema ? struct_(options.explicit_schema->fields()) : struct_({});
  auto promotion_graph =
      options.unexpected_field_behavior == UnexpectedFieldBehavior::InferType
          ? GetPromotionGraph()
          : nullptr;
  std::unique_ptr<ChunkedArrayBuilder> builder;
  RETURN_NOT_OK(MakeChunkedArrayBuilder(internal::TaskGroup::MakeSerial(),
                                        default_memory_pool(), promotion_graph, type,
                                        &builder));

  builder->Insert(0, field("", type), parsed);
  std::shared_ptr<ChunkedArray> converted_chunked;
  RETURN_NOT_OK(builder->Finish(&converted_chunked));
  auto converted = static_cast<const StructArray*>(converted_chunked->chunk(0).get());

  std::vector<std::shared_ptr<Array>> columns(converted->num_fields());
  for (int i = 0; i < converted->num_fields(); ++i) {
    columns[i] = converted->field(i);
  }
  *out = RecordBatch::Make(schema(converted->type()->children()), converted->length(),
                           std::move(columns));
  return Status::OK();
}

}  // namespace json
}  // namespace arrow
