// automatically generated by the FlatBuffers compiler, do not modify


#ifndef FLATBUFFERS_GENERATED_COMMON_PLASMA_FLATBUF_H_
#define FLATBUFFERS_GENERATED_COMMON_PLASMA_FLATBUF_H_

#include "flatbuffers/flatbuffers.h"

namespace plasma {
namespace flatbuf {

struct ObjectInfo;
struct ObjectInfoBuilder;
struct ObjectInfoT;

struct ObjectInfoT : public flatbuffers::NativeTable {
  typedef ObjectInfo TableType;
  std::string object_id{};
  int64_t data_size = 0;
  int64_t metadata_size = 0;
  int32_t ref_count = 0;
  int64_t create_time = 0;
  int64_t construct_duration = 0;
  std::string digest{};
  bool is_deletion = false;
};

struct ObjectInfo FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef ObjectInfoT NativeTableType;
  typedef ObjectInfoBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_OBJECT_ID = 4,
    VT_DATA_SIZE = 6,
    VT_METADATA_SIZE = 8,
    VT_REF_COUNT = 10,
    VT_CREATE_TIME = 12,
    VT_CONSTRUCT_DURATION = 14,
    VT_DIGEST = 16,
    VT_IS_DELETION = 18
  };
  const flatbuffers::String *object_id() const {
    return GetPointer<const flatbuffers::String *>(VT_OBJECT_ID);
  }
  int64_t data_size() const {
    return GetField<int64_t>(VT_DATA_SIZE, 0);
  }
  int64_t metadata_size() const {
    return GetField<int64_t>(VT_METADATA_SIZE, 0);
  }
  int32_t ref_count() const {
    return GetField<int32_t>(VT_REF_COUNT, 0);
  }
  int64_t create_time() const {
    return GetField<int64_t>(VT_CREATE_TIME, 0);
  }
  int64_t construct_duration() const {
    return GetField<int64_t>(VT_CONSTRUCT_DURATION, 0);
  }
  const flatbuffers::String *digest() const {
    return GetPointer<const flatbuffers::String *>(VT_DIGEST);
  }
  bool is_deletion() const {
    return GetField<uint8_t>(VT_IS_DELETION, 0) != 0;
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffset(verifier, VT_OBJECT_ID) &&
           verifier.VerifyString(object_id()) &&
           VerifyField<int64_t>(verifier, VT_DATA_SIZE) &&
           VerifyField<int64_t>(verifier, VT_METADATA_SIZE) &&
           VerifyField<int32_t>(verifier, VT_REF_COUNT) &&
           VerifyField<int64_t>(verifier, VT_CREATE_TIME) &&
           VerifyField<int64_t>(verifier, VT_CONSTRUCT_DURATION) &&
           VerifyOffset(verifier, VT_DIGEST) &&
           verifier.VerifyString(digest()) &&
           VerifyField<uint8_t>(verifier, VT_IS_DELETION) &&
           verifier.EndTable();
  }
  ObjectInfoT *UnPack(const flatbuffers::resolver_function_t *_resolver = nullptr) const;
  void UnPackTo(ObjectInfoT *_o, const flatbuffers::resolver_function_t *_resolver = nullptr) const;
  static flatbuffers::Offset<ObjectInfo> Pack(flatbuffers::FlatBufferBuilder &_fbb, const ObjectInfoT* _o, const flatbuffers::rehasher_function_t *_rehasher = nullptr);
};

struct ObjectInfoBuilder {
  typedef ObjectInfo Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_object_id(flatbuffers::Offset<flatbuffers::String> object_id) {
    fbb_.AddOffset(ObjectInfo::VT_OBJECT_ID, object_id);
  }
  void add_data_size(int64_t data_size) {
    fbb_.AddElement<int64_t>(ObjectInfo::VT_DATA_SIZE, data_size, 0);
  }
  void add_metadata_size(int64_t metadata_size) {
    fbb_.AddElement<int64_t>(ObjectInfo::VT_METADATA_SIZE, metadata_size, 0);
  }
  void add_ref_count(int32_t ref_count) {
    fbb_.AddElement<int32_t>(ObjectInfo::VT_REF_COUNT, ref_count, 0);
  }
  void add_create_time(int64_t create_time) {
    fbb_.AddElement<int64_t>(ObjectInfo::VT_CREATE_TIME, create_time, 0);
  }
  void add_construct_duration(int64_t construct_duration) {
    fbb_.AddElement<int64_t>(ObjectInfo::VT_CONSTRUCT_DURATION, construct_duration, 0);
  }
  void add_digest(flatbuffers::Offset<flatbuffers::String> digest) {
    fbb_.AddOffset(ObjectInfo::VT_DIGEST, digest);
  }
  void add_is_deletion(bool is_deletion) {
    fbb_.AddElement<uint8_t>(ObjectInfo::VT_IS_DELETION, static_cast<uint8_t>(is_deletion), 0);
  }
  explicit ObjectInfoBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  flatbuffers::Offset<ObjectInfo> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<ObjectInfo>(end);
    return o;
  }
};

inline flatbuffers::Offset<ObjectInfo> CreateObjectInfo(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::String> object_id = 0,
    int64_t data_size = 0,
    int64_t metadata_size = 0,
    int32_t ref_count = 0,
    int64_t create_time = 0,
    int64_t construct_duration = 0,
    flatbuffers::Offset<flatbuffers::String> digest = 0,
    bool is_deletion = false) {
  ObjectInfoBuilder builder_(_fbb);
  builder_.add_construct_duration(construct_duration);
  builder_.add_create_time(create_time);
  builder_.add_metadata_size(metadata_size);
  builder_.add_data_size(data_size);
  builder_.add_digest(digest);
  builder_.add_ref_count(ref_count);
  builder_.add_object_id(object_id);
  builder_.add_is_deletion(is_deletion);
  return builder_.Finish();
}

inline flatbuffers::Offset<ObjectInfo> CreateObjectInfoDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const char *object_id = nullptr,
    int64_t data_size = 0,
    int64_t metadata_size = 0,
    int32_t ref_count = 0,
    int64_t create_time = 0,
    int64_t construct_duration = 0,
    const char *digest = nullptr,
    bool is_deletion = false) {
  auto object_id__ = object_id ? _fbb.CreateString(object_id) : 0;
  auto digest__ = digest ? _fbb.CreateString(digest) : 0;
  return plasma::flatbuf::CreateObjectInfo(
      _fbb,
      object_id__,
      data_size,
      metadata_size,
      ref_count,
      create_time,
      construct_duration,
      digest__,
      is_deletion);
}

flatbuffers::Offset<ObjectInfo> CreateObjectInfo(flatbuffers::FlatBufferBuilder &_fbb, const ObjectInfoT *_o, const flatbuffers::rehasher_function_t *_rehasher = nullptr);

inline ObjectInfoT *ObjectInfo::UnPack(const flatbuffers::resolver_function_t *_resolver) const {
  auto _o = std::unique_ptr<ObjectInfoT>(new ObjectInfoT());
  UnPackTo(_o.get(), _resolver);
  return _o.release();
}

inline void ObjectInfo::UnPackTo(ObjectInfoT *_o, const flatbuffers::resolver_function_t *_resolver) const {
  (void)_o;
  (void)_resolver;
  { auto _e = object_id(); if (_e) _o->object_id = _e->str(); }
  { auto _e = data_size(); _o->data_size = _e; }
  { auto _e = metadata_size(); _o->metadata_size = _e; }
  { auto _e = ref_count(); _o->ref_count = _e; }
  { auto _e = create_time(); _o->create_time = _e; }
  { auto _e = construct_duration(); _o->construct_duration = _e; }
  { auto _e = digest(); if (_e) _o->digest = _e->str(); }
  { auto _e = is_deletion(); _o->is_deletion = _e; }
}

inline flatbuffers::Offset<ObjectInfo> ObjectInfo::Pack(flatbuffers::FlatBufferBuilder &_fbb, const ObjectInfoT* _o, const flatbuffers::rehasher_function_t *_rehasher) {
  return CreateObjectInfo(_fbb, _o, _rehasher);
}

inline flatbuffers::Offset<ObjectInfo> CreateObjectInfo(flatbuffers::FlatBufferBuilder &_fbb, const ObjectInfoT *_o, const flatbuffers::rehasher_function_t *_rehasher) {
  (void)_rehasher;
  (void)_o;
  struct _VectorArgs { flatbuffers::FlatBufferBuilder *__fbb; const ObjectInfoT* __o; const flatbuffers::rehasher_function_t *__rehasher; } _va = { &_fbb, _o, _rehasher}; (void)_va;
  auto _object_id = _o->object_id.empty() ? 0 : _fbb.CreateString(_o->object_id);
  auto _data_size = _o->data_size;
  auto _metadata_size = _o->metadata_size;
  auto _ref_count = _o->ref_count;
  auto _create_time = _o->create_time;
  auto _construct_duration = _o->construct_duration;
  auto _digest = _o->digest.empty() ? 0 : _fbb.CreateString(_o->digest);
  auto _is_deletion = _o->is_deletion;
  return plasma::flatbuf::CreateObjectInfo(
      _fbb,
      _object_id,
      _data_size,
      _metadata_size,
      _ref_count,
      _create_time,
      _construct_duration,
      _digest,
      _is_deletion);
}

}  // namespace flatbuf
}  // namespace plasma

#endif  // FLATBUFFERS_GENERATED_COMMON_PLASMA_FLATBUF_H_
