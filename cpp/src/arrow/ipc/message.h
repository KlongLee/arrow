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

// C++ object model and user API for interprocess schema messaging

#ifndef ARROW_IPC_MESSAGE_H
#define ARROW_IPC_MESSAGE_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Buffer;
class Field;

namespace io {

class InputStream;
class OutputStream;
class RandomAccessFile;

}  // namespace io

namespace ipc {

enum class MetadataVersion : char { V1, V2, V3 };

// ARROW-109: We set this number arbitrarily to help catch user mistakes. For
// deeply nested schemas, it is expected the user will indicate explicitly the
// maximum allowed recursion depth
constexpr int kMaxNestingDepth = 64;

// Read interface classes. We do not fully deserialize the flatbuffers so that
// individual fields metadata can be retrieved from very large schema without
//

class Message;

/// \brief An IPC message including metadata and body
class ARROW_EXPORT Message {
 public:
  enum Type { NONE, SCHEMA, DICTIONARY_BATCH, RECORD_BATCH, TENSOR };

  /// \brief Construct message, but do not validate
  ///
  /// Use at your own risk; Message::Open has more metadata validation
  Message(const std::shared_ptr<Buffer>& metadata, const std::shared_ptr<Buffer>& body);

  ~Message();

  /// \brief Create and validate a Message instance from two buffers
  ///
  /// \param[in] metadata a buffer containing the Flatbuffer metadata
  /// \param[in] body a buffer containing the message body, which may be nullptr
  /// \param[out] out the created message
  static Status Open(const std::shared_ptr<Buffer>& metadata,
                     const std::shared_ptr<Buffer>& body, std::unique_ptr<Message>* out);

  /// \brief Write length-prefixed metadata and body to output stream
  ///
  /// \param[in] file output stream to write to
  /// \param[out] output_length the number of bytes written
  /// \return Status
  bool Equals(const Message& other) const;

  /// \brief the Message metadata
  ///
  /// \return buffer
  std::shared_ptr<Buffer> metadata() const;

  /// \brief the Message body, if any
  ///
  /// \return buffer is nullptr if no body
  std::shared_ptr<Buffer> body() const;

  Type type() const;

  MetadataVersion metadata_version() const;

  const void* header() const;

  /// \brief Write length-prefixed metadata and body to output stream
  ///
  /// \param[in] file output stream to write to
  /// \param[out] output_length the number of bytes written
  /// \return Status
  Status SerializeTo(io::OutputStream* file, int64_t* output_length) const;

 private:
  // Hide serialization details from user API
  class MessageImpl;
  std::unique_ptr<MessageImpl> impl_;

  DISALLOW_COPY_AND_ASSIGN(Message);
};

ARROW_EXPORT std::string FormatMessageType(Message::Type type);

/// \brief Abstract interface for a sequence of messages
/// \since 0.5.0
class ARROW_EXPORT MessageReader {
 public:
  virtual ~MessageReader() = default;

  /// \brief Read next Message from the interface
  ///
  /// \param[out] message an arrow::ipc::Message instance
  /// \return Status
  virtual Status ReadNextMessage(std::unique_ptr<Message>* message) = 0;
};

/// \brief Implementation of MessageReader that reads from InputStream
/// \since 0.5.0
class ARROW_EXPORT InputStreamMessageReader : public MessageReader {
 public:
  explicit InputStreamMessageReader(io::InputStream* stream) : stream_(stream) {}

  explicit InputStreamMessageReader(const std::shared_ptr<io::InputStream>& owned_stream)
      : InputStreamMessageReader(owned_stream.get()) {
    owned_stream_ = owned_stream;
  }

  ~InputStreamMessageReader();

  Status ReadNextMessage(std::unique_ptr<Message>* message) override;

 private:
  io::InputStream* stream_;
  std::shared_ptr<io::InputStream> owned_stream_;
};

/// \brief Read encapulated RPC message from position in file
///
/// Read a length-prefixed message flatbuffer starting at the indicated file
/// offset. If the message has a body with non-zero length, it will also be
/// read
///
/// The metadata_length includes at least the length prefix and the flatbuffer
///
/// \param[in] offset the position in the file where the message starts. The
/// first 4 bytes after the offset are the message length
/// \param[in] metadata_length the total number of bytes to read from file
/// \param[in] file the seekable file interface to read from
/// \param[out] message the message read
/// \return Status success or failure
ARROW_EXPORT
Status ReadMessage(const int64_t offset, const int32_t metadata_length,
                   io::RandomAccessFile* file, std::unique_ptr<Message>* message);

/// \brief Read encapulated RPC message (metadata and body) from InputStream
///
/// Read length-prefixed message with as-yet unknown length. Returns nullptr if
/// there are not enough bytes available or the message length is 0 (e.g. EOS
/// in a stream)
ARROW_EXPORT
Status ReadMessage(io::InputStream* stream, std::unique_ptr<Message>* message);

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_MESSAGE_H
