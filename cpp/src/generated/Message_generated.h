// automatically generated by the FlatBuffers compiler, do not modify


#ifndef FLATBUFFERS_GENERATED_MESSAGE_ORG_APACHE_ARROW_FLATBUF_H_
#define FLATBUFFERS_GENERATED_MESSAGE_ORG_APACHE_ARROW_FLATBUF_H_

#include "flatbuffers/flatbuffers.h"

#include "Schema_generated.h"
#include "SparseTensor_generated.h"
#include "Tensor_generated.h"

namespace org {
namespace apache {
namespace arrow {
namespace flatbuf {

struct FieldNode;

struct BodyCompression;

struct RecordBatch;

struct DictionaryBatch;

struct Message;

enum class CompressionType : int8_t {
  LZ4_FRAME = 0,
  ZSTD = 1,
  MIN = LZ4_FRAME,
  MAX = ZSTD
};

inline const CompressionType (&EnumValuesCompressionType())[2] {
  static const CompressionType values[] = {
    CompressionType::LZ4_FRAME,
    CompressionType::ZSTD
  };
  return values;
}

inline const char * const *EnumNamesCompressionType() {
  static const char * const names[3] = {
    "LZ4_FRAME",
    "ZSTD",
    nullptr
  };
  return names;
}

inline const char *EnumNameCompressionType(CompressionType e) {
  if (flatbuffers::IsOutRange(e, CompressionType::LZ4_FRAME, CompressionType::ZSTD)) return "";
  const size_t index = static_cast<size_t>(e);
  return EnumNamesCompressionType()[index];
}

/// Provided for forward compatibility in case we need to support different
/// strategies for compressing the IPC message body (like whole-body
/// compression rather than buffer-level) in the future
enum class BodyCompressionMethod : int8_t {
  /// Each constituent buffer is first compressed with the indicated
  /// compressor, and then written with the uncompressed length in the first 8
  /// bytes as a 64-bit little-endian signed integer followed by the compressed
  /// buffer bytes (and then padding as required by the protocol). The
  /// uncompressed length may be set to -1 to indicate that the data that
  /// follows is not compressed, which can be useful for cases where
  /// compression does not yield appreciable savings.
  BUFFER = 0,
  MIN = BUFFER,
  MAX = BUFFER
};

inline const BodyCompressionMethod (&EnumValuesBodyCompressionMethod())[1] {
  static const BodyCompressionMethod values[] = {
    BodyCompressionMethod::BUFFER
  };
  return values;
}

inline const char * const *EnumNamesBodyCompressionMethod() {
  static const char * const names[2] = {
    "BUFFER",
    nullptr
  };
  return names;
}

inline const char *EnumNameBodyCompressionMethod(BodyCompressionMethod e) {
  if (flatbuffers::IsOutRange(e, BodyCompressionMethod::BUFFER, BodyCompressionMethod::BUFFER)) return "";
  const size_t index = static_cast<size_t>(e);
  return EnumNamesBodyCompressionMethod()[index];
}

/// ----------------------------------------------------------------------
/// The root Message type
/// This union enables us to easily send different message types without
/// redundant storage, and in the future we can easily add new message types.
///
/// Arrow implementations do not need to implement all of the message types,
/// which may include experimental metadata types. For maximum compatibility,
/// it is best to send data using RecordBatch
enum class MessageHeader : uint8_t {
  NONE = 0,
  Schema = 1,
  DictionaryBatch = 2,
  RecordBatch = 3,
  Tensor = 4,
  SparseTensor = 5,
  MIN = NONE,
  MAX = SparseTensor
};

inline const MessageHeader (&EnumValuesMessageHeader())[6] {
  static const MessageHeader values[] = {
    MessageHeader::NONE,
    MessageHeader::Schema,
    MessageHeader::DictionaryBatch,
    MessageHeader::RecordBatch,
    MessageHeader::Tensor,
    MessageHeader::SparseTensor
  };
  return values;
}

inline const char * const *EnumNamesMessageHeader() {
  static const char * const names[7] = {
    "NONE",
    "Schema",
    "DictionaryBatch",
    "RecordBatch",
    "Tensor",
    "SparseTensor",
    nullptr
  };
  return names;
}

inline const char *EnumNameMessageHeader(MessageHeader e) {
  if (flatbuffers::IsOutRange(e, MessageHeader::NONE, MessageHeader::SparseTensor)) return "";
  const size_t index = static_cast<size_t>(e);
  return EnumNamesMessageHeader()[index];
}

template<typename T> struct MessageHeaderTraits {
  static const MessageHeader enum_value = MessageHeader::NONE;
};

template<> struct MessageHeaderTraits<org::apache::arrow::flatbuf::Schema> {
  static const MessageHeader enum_value = MessageHeader::Schema;
};

template<> struct MessageHeaderTraits<org::apache::arrow::flatbuf::DictionaryBatch> {
  static const MessageHeader enum_value = MessageHeader::DictionaryBatch;
};

template<> struct MessageHeaderTraits<org::apache::arrow::flatbuf::RecordBatch> {
  static const MessageHeader enum_value = MessageHeader::RecordBatch;
};

template<> struct MessageHeaderTraits<org::apache::arrow::flatbuf::Tensor> {
  static const MessageHeader enum_value = MessageHeader::Tensor;
};

template<> struct MessageHeaderTraits<org::apache::arrow::flatbuf::SparseTensor> {
  static const MessageHeader enum_value = MessageHeader::SparseTensor;
};

bool VerifyMessageHeader(flatbuffers::Verifier &verifier, const void *obj, MessageHeader type);
bool VerifyMessageHeaderVector(flatbuffers::Verifier &verifier, const flatbuffers::Vector<flatbuffers::Offset<void>> *values, const flatbuffers::Vector<uint8_t> *types);

/// ----------------------------------------------------------------------
/// Data structures for describing a table row batch (a collection of
/// equal-length Arrow arrays)
/// Metadata about a field at some level of a nested type tree (but not
/// its children).
///
/// For example, a List<Int16> with values [[1, 2, 3], null, [4], [5, 6], null]
/// would have {length: 5, null_count: 2} for its List node, and {length: 6,
/// null_count: 0} for its Int16 node, as separate FieldNode structs
FLATBUFFERS_MANUALLY_ALIGNED_STRUCT(8) FieldNode FLATBUFFERS_FINAL_CLASS {
 private:
  int64_t length_;
  int64_t null_count_;

 public:
  FieldNode() {
    memset(static_cast<void *>(this), 0, sizeof(FieldNode));
  }
  FieldNode(int64_t _length, int64_t _null_count)
      : length_(flatbuffers::EndianScalar(_length)),
        null_count_(flatbuffers::EndianScalar(_null_count)) {
  }
  /// The number of value slots in the Arrow array at this level of a nested
  /// tree
  int64_t length() const {
    return flatbuffers::EndianScalar(length_);
  }
  /// The number of observed nulls. Fields with null_count == 0 may choose not
  /// to write their physical validity bitmap out as a materialized buffer,
  /// instead setting the length of the bitmap buffer to 0.
  int64_t null_count() const {
    return flatbuffers::EndianScalar(null_count_);
  }
};
FLATBUFFERS_STRUCT_END(FieldNode, 16);

/// Optional compression for the memory buffers constituting IPC message
/// bodies. Intended for use with RecordBatch but could be used for other
/// message types
struct BodyCompression FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_CODEC = 4,
    VT_METHOD = 6
  };
  /// Compressor library
  org::apache::arrow::flatbuf::CompressionType codec() const {
    return static_cast<org::apache::arrow::flatbuf::CompressionType>(GetField<int8_t>(VT_CODEC, 0));
  }
  /// Indicates the way the record batch body was compressed
  org::apache::arrow::flatbuf::BodyCompressionMethod method() const {
    return static_cast<org::apache::arrow::flatbuf::BodyCompressionMethod>(GetField<int8_t>(VT_METHOD, 0));
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<int8_t>(verifier, VT_CODEC) &&
           VerifyField<int8_t>(verifier, VT_METHOD) &&
           verifier.EndTable();
  }
};

struct BodyCompressionBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_codec(org::apache::arrow::flatbuf::CompressionType codec) {
    fbb_.AddElement<int8_t>(BodyCompression::VT_CODEC, static_cast<int8_t>(codec), 0);
  }
  void add_method(org::apache::arrow::flatbuf::BodyCompressionMethod method) {
    fbb_.AddElement<int8_t>(BodyCompression::VT_METHOD, static_cast<int8_t>(method), 0);
  }
  explicit BodyCompressionBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  BodyCompressionBuilder &operator=(const BodyCompressionBuilder &);
  flatbuffers::Offset<BodyCompression> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<BodyCompression>(end);
    return o;
  }
};

inline flatbuffers::Offset<BodyCompression> CreateBodyCompression(
    flatbuffers::FlatBufferBuilder &_fbb,
    org::apache::arrow::flatbuf::CompressionType codec = org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME,
    org::apache::arrow::flatbuf::BodyCompressionMethod method = org::apache::arrow::flatbuf::BodyCompressionMethod::BUFFER) {
  BodyCompressionBuilder builder_(_fbb);
  builder_.add_method(method);
  builder_.add_codec(codec);
  return builder_.Finish();
}

/// A data header describing the shared memory layout of a "record" or "row"
/// batch. Some systems call this a "row batch" internally and others a "record
/// batch".
struct RecordBatch FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_LENGTH = 4,
    VT_NODES = 6,
    VT_BUFFERS = 8,
    VT_COMPRESSION = 10
  };
  /// number of records / rows. The arrays in the batch should all have this
  /// length
  int64_t length() const {
    return GetField<int64_t>(VT_LENGTH, 0);
  }
  /// Nodes correspond to the pre-ordered flattened logical schema
  const flatbuffers::Vector<const org::apache::arrow::flatbuf::FieldNode *> *nodes() const {
    return GetPointer<const flatbuffers::Vector<const org::apache::arrow::flatbuf::FieldNode *> *>(VT_NODES);
  }
  /// Buffers correspond to the pre-ordered flattened buffer tree
  ///
  /// The number of buffers appended to this list depends on the schema. For
  /// example, most primitive arrays will have 2 buffers, 1 for the validity
  /// bitmap and 1 for the values. For struct arrays, there will only be a
  /// single buffer for the validity (nulls) bitmap
  const flatbuffers::Vector<const org::apache::arrow::flatbuf::Buffer *> *buffers() const {
    return GetPointer<const flatbuffers::Vector<const org::apache::arrow::flatbuf::Buffer *> *>(VT_BUFFERS);
  }
  /// Optional compression of the message body
  const org::apache::arrow::flatbuf::BodyCompression *compression() const {
    return GetPointer<const org::apache::arrow::flatbuf::BodyCompression *>(VT_COMPRESSION);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<int64_t>(verifier, VT_LENGTH) &&
           VerifyOffset(verifier, VT_NODES) &&
           verifier.VerifyVector(nodes()) &&
           VerifyOffset(verifier, VT_BUFFERS) &&
           verifier.VerifyVector(buffers()) &&
           VerifyOffset(verifier, VT_COMPRESSION) &&
           verifier.VerifyTable(compression()) &&
           verifier.EndTable();
  }
};

struct RecordBatchBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_length(int64_t length) {
    fbb_.AddElement<int64_t>(RecordBatch::VT_LENGTH, length, 0);
  }
  void add_nodes(flatbuffers::Offset<flatbuffers::Vector<const org::apache::arrow::flatbuf::FieldNode *>> nodes) {
    fbb_.AddOffset(RecordBatch::VT_NODES, nodes);
  }
  void add_buffers(flatbuffers::Offset<flatbuffers::Vector<const org::apache::arrow::flatbuf::Buffer *>> buffers) {
    fbb_.AddOffset(RecordBatch::VT_BUFFERS, buffers);
  }
  void add_compression(flatbuffers::Offset<org::apache::arrow::flatbuf::BodyCompression> compression) {
    fbb_.AddOffset(RecordBatch::VT_COMPRESSION, compression);
  }
  explicit RecordBatchBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  RecordBatchBuilder &operator=(const RecordBatchBuilder &);
  flatbuffers::Offset<RecordBatch> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<RecordBatch>(end);
    return o;
  }
};

inline flatbuffers::Offset<RecordBatch> CreateRecordBatch(
    flatbuffers::FlatBufferBuilder &_fbb,
    int64_t length = 0,
    flatbuffers::Offset<flatbuffers::Vector<const org::apache::arrow::flatbuf::FieldNode *>> nodes = 0,
    flatbuffers::Offset<flatbuffers::Vector<const org::apache::arrow::flatbuf::Buffer *>> buffers = 0,
    flatbuffers::Offset<org::apache::arrow::flatbuf::BodyCompression> compression = 0) {
  RecordBatchBuilder builder_(_fbb);
  builder_.add_length(length);
  builder_.add_compression(compression);
  builder_.add_buffers(buffers);
  builder_.add_nodes(nodes);
  return builder_.Finish();
}

inline flatbuffers::Offset<RecordBatch> CreateRecordBatchDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    int64_t length = 0,
    const std::vector<org::apache::arrow::flatbuf::FieldNode> *nodes = nullptr,
    const std::vector<org::apache::arrow::flatbuf::Buffer> *buffers = nullptr,
    flatbuffers::Offset<org::apache::arrow::flatbuf::BodyCompression> compression = 0) {
  auto nodes__ = nodes ? _fbb.CreateVectorOfStructs<org::apache::arrow::flatbuf::FieldNode>(*nodes) : 0;
  auto buffers__ = buffers ? _fbb.CreateVectorOfStructs<org::apache::arrow::flatbuf::Buffer>(*buffers) : 0;
  return org::apache::arrow::flatbuf::CreateRecordBatch(
      _fbb,
      length,
      nodes__,
      buffers__,
      compression);
}

/// For sending dictionary encoding information. Any Field can be
/// dictionary-encoded, but in this case none of its children may be
/// dictionary-encoded.
/// There is one vector / column per dictionary, but that vector / column
/// may be spread across multiple dictionary batches by using the isDelta
/// flag
struct DictionaryBatch FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ID = 4,
    VT_DATA = 6,
    VT_ISDELTA = 8
  };
  int64_t id() const {
    return GetField<int64_t>(VT_ID, 0);
  }
  const org::apache::arrow::flatbuf::RecordBatch *data() const {
    return GetPointer<const org::apache::arrow::flatbuf::RecordBatch *>(VT_DATA);
  }
  /// If isDelta is true the values in the dictionary are to be appended to a
  /// dictionary with the indicated id. If isDelta is false this dictionary
  /// should replace the existing dictionary.
  bool isDelta() const {
    return GetField<uint8_t>(VT_ISDELTA, 0) != 0;
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<int64_t>(verifier, VT_ID) &&
           VerifyOffset(verifier, VT_DATA) &&
           verifier.VerifyTable(data()) &&
           VerifyField<uint8_t>(verifier, VT_ISDELTA) &&
           verifier.EndTable();
  }
};

struct DictionaryBatchBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_id(int64_t id) {
    fbb_.AddElement<int64_t>(DictionaryBatch::VT_ID, id, 0);
  }
  void add_data(flatbuffers::Offset<org::apache::arrow::flatbuf::RecordBatch> data) {
    fbb_.AddOffset(DictionaryBatch::VT_DATA, data);
  }
  void add_isDelta(bool isDelta) {
    fbb_.AddElement<uint8_t>(DictionaryBatch::VT_ISDELTA, static_cast<uint8_t>(isDelta), 0);
  }
  explicit DictionaryBatchBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  DictionaryBatchBuilder &operator=(const DictionaryBatchBuilder &);
  flatbuffers::Offset<DictionaryBatch> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<DictionaryBatch>(end);
    return o;
  }
};

inline flatbuffers::Offset<DictionaryBatch> CreateDictionaryBatch(
    flatbuffers::FlatBufferBuilder &_fbb,
    int64_t id = 0,
    flatbuffers::Offset<org::apache::arrow::flatbuf::RecordBatch> data = 0,
    bool isDelta = false) {
  DictionaryBatchBuilder builder_(_fbb);
  builder_.add_id(id);
  builder_.add_data(data);
  builder_.add_isDelta(isDelta);
  return builder_.Finish();
}

struct Message FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_VERSION = 4,
    VT_HEADER_TYPE = 6,
    VT_HEADER = 8,
    VT_BODYLENGTH = 10,
    VT_CUSTOM_METADATA = 12
  };
  org::apache::arrow::flatbuf::MetadataVersion version() const {
    return static_cast<org::apache::arrow::flatbuf::MetadataVersion>(GetField<int16_t>(VT_VERSION, 0));
  }
  org::apache::arrow::flatbuf::MessageHeader header_type() const {
    return static_cast<org::apache::arrow::flatbuf::MessageHeader>(GetField<uint8_t>(VT_HEADER_TYPE, 0));
  }
  const void *header() const {
    return GetPointer<const void *>(VT_HEADER);
  }
  template<typename T> const T *header_as() const;
  const org::apache::arrow::flatbuf::Schema *header_as_Schema() const {
    return header_type() == org::apache::arrow::flatbuf::MessageHeader::Schema ? static_cast<const org::apache::arrow::flatbuf::Schema *>(header()) : nullptr;
  }
  const org::apache::arrow::flatbuf::DictionaryBatch *header_as_DictionaryBatch() const {
    return header_type() == org::apache::arrow::flatbuf::MessageHeader::DictionaryBatch ? static_cast<const org::apache::arrow::flatbuf::DictionaryBatch *>(header()) : nullptr;
  }
  const org::apache::arrow::flatbuf::RecordBatch *header_as_RecordBatch() const {
    return header_type() == org::apache::arrow::flatbuf::MessageHeader::RecordBatch ? static_cast<const org::apache::arrow::flatbuf::RecordBatch *>(header()) : nullptr;
  }
  const org::apache::arrow::flatbuf::Tensor *header_as_Tensor() const {
    return header_type() == org::apache::arrow::flatbuf::MessageHeader::Tensor ? static_cast<const org::apache::arrow::flatbuf::Tensor *>(header()) : nullptr;
  }
  const org::apache::arrow::flatbuf::SparseTensor *header_as_SparseTensor() const {
    return header_type() == org::apache::arrow::flatbuf::MessageHeader::SparseTensor ? static_cast<const org::apache::arrow::flatbuf::SparseTensor *>(header()) : nullptr;
  }
  int64_t bodyLength() const {
    return GetField<int64_t>(VT_BODYLENGTH, 0);
  }
  const flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>> *custom_metadata() const {
    return GetPointer<const flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>> *>(VT_CUSTOM_METADATA);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<int16_t>(verifier, VT_VERSION) &&
           VerifyField<uint8_t>(verifier, VT_HEADER_TYPE) &&
           VerifyOffset(verifier, VT_HEADER) &&
           VerifyMessageHeader(verifier, header(), header_type()) &&
           VerifyField<int64_t>(verifier, VT_BODYLENGTH) &&
           VerifyOffset(verifier, VT_CUSTOM_METADATA) &&
           verifier.VerifyVector(custom_metadata()) &&
           verifier.VerifyVectorOfTables(custom_metadata()) &&
           verifier.EndTable();
  }
};

template<> inline const org::apache::arrow::flatbuf::Schema *Message::header_as<org::apache::arrow::flatbuf::Schema>() const {
  return header_as_Schema();
}

template<> inline const org::apache::arrow::flatbuf::DictionaryBatch *Message::header_as<org::apache::arrow::flatbuf::DictionaryBatch>() const {
  return header_as_DictionaryBatch();
}

template<> inline const org::apache::arrow::flatbuf::RecordBatch *Message::header_as<org::apache::arrow::flatbuf::RecordBatch>() const {
  return header_as_RecordBatch();
}

template<> inline const org::apache::arrow::flatbuf::Tensor *Message::header_as<org::apache::arrow::flatbuf::Tensor>() const {
  return header_as_Tensor();
}

template<> inline const org::apache::arrow::flatbuf::SparseTensor *Message::header_as<org::apache::arrow::flatbuf::SparseTensor>() const {
  return header_as_SparseTensor();
}

struct MessageBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_version(org::apache::arrow::flatbuf::MetadataVersion version) {
    fbb_.AddElement<int16_t>(Message::VT_VERSION, static_cast<int16_t>(version), 0);
  }
  void add_header_type(org::apache::arrow::flatbuf::MessageHeader header_type) {
    fbb_.AddElement<uint8_t>(Message::VT_HEADER_TYPE, static_cast<uint8_t>(header_type), 0);
  }
  void add_header(flatbuffers::Offset<void> header) {
    fbb_.AddOffset(Message::VT_HEADER, header);
  }
  void add_bodyLength(int64_t bodyLength) {
    fbb_.AddElement<int64_t>(Message::VT_BODYLENGTH, bodyLength, 0);
  }
  void add_custom_metadata(flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>> custom_metadata) {
    fbb_.AddOffset(Message::VT_CUSTOM_METADATA, custom_metadata);
  }
  explicit MessageBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  MessageBuilder &operator=(const MessageBuilder &);
  flatbuffers::Offset<Message> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<Message>(end);
    return o;
  }
};

inline flatbuffers::Offset<Message> CreateMessage(
    flatbuffers::FlatBufferBuilder &_fbb,
    org::apache::arrow::flatbuf::MetadataVersion version = org::apache::arrow::flatbuf::MetadataVersion::V1,
    org::apache::arrow::flatbuf::MessageHeader header_type = org::apache::arrow::flatbuf::MessageHeader::NONE,
    flatbuffers::Offset<void> header = 0,
    int64_t bodyLength = 0,
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>> custom_metadata = 0) {
  MessageBuilder builder_(_fbb);
  builder_.add_bodyLength(bodyLength);
  builder_.add_custom_metadata(custom_metadata);
  builder_.add_header(header);
  builder_.add_version(version);
  builder_.add_header_type(header_type);
  return builder_.Finish();
}

inline flatbuffers::Offset<Message> CreateMessageDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    org::apache::arrow::flatbuf::MetadataVersion version = org::apache::arrow::flatbuf::MetadataVersion::V1,
    org::apache::arrow::flatbuf::MessageHeader header_type = org::apache::arrow::flatbuf::MessageHeader::NONE,
    flatbuffers::Offset<void> header = 0,
    int64_t bodyLength = 0,
    const std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>> *custom_metadata = nullptr) {
  auto custom_metadata__ = custom_metadata ? _fbb.CreateVector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>(*custom_metadata) : 0;
  return org::apache::arrow::flatbuf::CreateMessage(
      _fbb,
      version,
      header_type,
      header,
      bodyLength,
      custom_metadata__);
}

inline bool VerifyMessageHeader(flatbuffers::Verifier &verifier, const void *obj, MessageHeader type) {
  switch (type) {
    case MessageHeader::NONE: {
      return true;
    }
    case MessageHeader::Schema: {
      auto ptr = reinterpret_cast<const org::apache::arrow::flatbuf::Schema *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case MessageHeader::DictionaryBatch: {
      auto ptr = reinterpret_cast<const org::apache::arrow::flatbuf::DictionaryBatch *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case MessageHeader::RecordBatch: {
      auto ptr = reinterpret_cast<const org::apache::arrow::flatbuf::RecordBatch *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case MessageHeader::Tensor: {
      auto ptr = reinterpret_cast<const org::apache::arrow::flatbuf::Tensor *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case MessageHeader::SparseTensor: {
      auto ptr = reinterpret_cast<const org::apache::arrow::flatbuf::SparseTensor *>(obj);
      return verifier.VerifyTable(ptr);
    }
    default: return true;
  }
}

inline bool VerifyMessageHeaderVector(flatbuffers::Verifier &verifier, const flatbuffers::Vector<flatbuffers::Offset<void>> *values, const flatbuffers::Vector<uint8_t> *types) {
  if (!values || !types) return !values && !types;
  if (values->size() != types->size()) return false;
  for (flatbuffers::uoffset_t i = 0; i < values->size(); ++i) {
    if (!VerifyMessageHeader(
        verifier,  values->Get(i), types->GetEnum<MessageHeader>(i))) {
      return false;
    }
  }
  return true;
}

inline const org::apache::arrow::flatbuf::Message *GetMessage(const void *buf) {
  return flatbuffers::GetRoot<org::apache::arrow::flatbuf::Message>(buf);
}

inline const org::apache::arrow::flatbuf::Message *GetSizePrefixedMessage(const void *buf) {
  return flatbuffers::GetSizePrefixedRoot<org::apache::arrow::flatbuf::Message>(buf);
}

inline bool VerifyMessageBuffer(
    flatbuffers::Verifier &verifier) {
  return verifier.VerifyBuffer<org::apache::arrow::flatbuf::Message>(nullptr);
}

inline bool VerifySizePrefixedMessageBuffer(
    flatbuffers::Verifier &verifier) {
  return verifier.VerifySizePrefixedBuffer<org::apache::arrow::flatbuf::Message>(nullptr);
}

inline void FinishMessageBuffer(
    flatbuffers::FlatBufferBuilder &fbb,
    flatbuffers::Offset<org::apache::arrow::flatbuf::Message> root) {
  fbb.Finish(root);
}

inline void FinishSizePrefixedMessageBuffer(
    flatbuffers::FlatBufferBuilder &fbb,
    flatbuffers::Offset<org::apache::arrow::flatbuf::Message> root) {
  fbb.FinishSizePrefixed(root);
}

}  // namespace flatbuf
}  // namespace arrow
}  // namespace apache
}  // namespace org

#endif  // FLATBUFFERS_GENERATED_MESSAGE_ORG_APACHE_ARROW_FLATBUF_H_
