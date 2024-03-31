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

// Private header, not to be exported

#pragma once

#include <_types/_uint16_t.h>
#include <_types/_uint64_t.h>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array/builder_binary.h"
#include "arrow/buffer_builder.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_builders.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"

#define XXH_INLINE_ALL

#include "arrow/vendored/xxhash.h"  // IWYU pragma: keep

namespace arrow {
namespace internal {

// XXX would it help to have a 32-bit hash value on large datasets?
typedef uint64_t hash_t;

// Notes about the choice of a hash function.
// - XXH3 is extremely fast on most data sizes, from small to huge;
//   faster even than HW CRC-based hashing schemes
// - our custom hash function for tiny values (< 16 bytes) is still
//   significantly faster (~30%), at least on this machine and compiler

template <uint64_t AlgNum>
inline hash_t ComputeStringHash(const void* data, int64_t length);

/// \brief A hash function for bitmaps that can handle offsets and lengths in
/// terms of number of bits. The hash only depends on the bits actually hashed.
///
/// It's the caller's responsibility to ensure that bits_offset + num_bits are
/// readable from the bitmap.
///
/// \pre bits_offset >= 0
/// \pre num_bits >= 0
/// \pre (bits_offset + num_bits + 7) / 8 <= readable length in bytes from bitmap
///
/// \param bitmap The pointer to the bitmap.
/// \param seed The seed for the hash function (useful when chaining hash functions).
/// \param bits_offset The offset in bits relative to the start of the bitmap.
/// \param num_bits The number of bits after the offset to be hashed.
ARROW_EXPORT hash_t ComputeBitmapHash(const uint8_t* bitmap, hash_t seed,
                                      int64_t bits_offset, int64_t num_bits);

template <typename Scalar, uint64_t AlgNum>
struct ScalarHelperBase {
  static bool CompareScalars(Scalar u, Scalar v) { return u == v; }

  static hash_t ComputeHash(const Scalar& value) {
    // Generic hash computation for scalars.  Simply apply the string hash
    // to the bit representation of the value.

    // XXX in the case of FP values, we'd like equal values to have the same hash,
    // even if they have different bit representations...
    return ComputeStringHash<AlgNum>(&value, sizeof(value));
  }
};

template <typename Scalar, uint64_t AlgNum = 0, typename Enable = void>
struct ScalarHelper : public ScalarHelperBase<Scalar, AlgNum> {};

template <typename Scalar, uint64_t AlgNum>
struct ScalarHelper<Scalar, AlgNum, enable_if_t<std::is_integral<Scalar>::value>>
    : public ScalarHelperBase<Scalar, AlgNum> {
  // ScalarHelper specialization for integers

  static hash_t ComputeHash(const Scalar& value) {
    // Faster hash computation for integers.

    // Two of xxhash's prime multipliers (which are chosen for their
    // bit dispersion properties)
    static constexpr uint64_t multipliers[] = {11400714785074694791ULL,
                                               14029467366897019727ULL};

    // Multiplying by the prime number mixes the low bits into the high bits,
    // then byte-swapping (which is a single CPU instruction) allows the
    // combined high and low bits to participate in the initial hash table index.
    auto h = static_cast<hash_t>(value);
    return bit_util::ByteSwap(multipliers[AlgNum] * h);
  }
};

template <typename Scalar, uint64_t AlgNum>
struct ScalarHelper<Scalar, AlgNum,
                    enable_if_t<std::is_same<std::string_view, Scalar>::value>>
    : public ScalarHelperBase<Scalar, AlgNum> {
  // ScalarHelper specialization for std::string_view

  static hash_t ComputeHash(std::string_view value) {
    return ComputeStringHash<AlgNum>(value.data(), static_cast<int64_t>(value.size()));
  }
};

template <typename Scalar, uint64_t AlgNum>
struct ScalarHelper<Scalar, AlgNum, enable_if_t<std::is_floating_point<Scalar>::value>>
    : public ScalarHelperBase<Scalar, AlgNum> {
  // ScalarHelper specialization for reals

  static bool CompareScalars(Scalar u, Scalar v) {
    if (std::isnan(u)) {
      // XXX should we do a bit-precise comparison?
      return std::isnan(v);
    }
    return u == v;
  }
};

template <uint64_t AlgNum = 0>
hash_t ComputeStringHash(const void* data, int64_t length) {
  if (ARROW_PREDICT_TRUE(length <= 16)) {
    // Specialize for small hash strings, as they are quite common as
    // hash table keys.  Even XXH3 isn't quite as fast.
    auto p = reinterpret_cast<const uint8_t*>(data);
    auto n = static_cast<uint32_t>(length);
    if (n <= 8) {
      if (n <= 3) {
        if (n == 0) {
          return 1U;
        }
        uint32_t x = (n << 24) ^ (p[0] << 16) ^ (p[n / 2] << 8) ^ p[n - 1];
        return ScalarHelper<uint32_t, AlgNum>::ComputeHash(x);
      }
      // 4 <= length <= 8
      // We can read the string as two overlapping 32-bit ints, apply
      // different hash functions to each of them in parallel, then XOR
      // the results
      uint32_t x, y;
      hash_t hx, hy;
      x = util::SafeLoadAs<uint32_t>(p + n - 4);
      y = util::SafeLoadAs<uint32_t>(p);
      hx = ScalarHelper<uint32_t, AlgNum>::ComputeHash(x);
      hy = ScalarHelper<uint32_t, AlgNum ^ 1>::ComputeHash(y);
      return n ^ hx ^ hy;
    }
    // 8 <= length <= 16
    // Apply the same principle as above
    uint64_t x, y;
    hash_t hx, hy;
    x = util::SafeLoadAs<uint64_t>(p + n - 8);
    y = util::SafeLoadAs<uint64_t>(p);
    hx = ScalarHelper<uint64_t, AlgNum>::ComputeHash(x);
    hy = ScalarHelper<uint64_t, AlgNum ^ 1>::ComputeHash(y);
    return n ^ hx ^ hy;
  }

#if XXH3_SECRET_SIZE_MIN != 136
#error XXH3_SECRET_SIZE_MIN changed, please fix kXxh3Secrets
#endif

  // XXH3_64bits_withSeed generates a secret based on the seed, which is too slow.
  // Instead, we use hard-coded random secrets.  To maximize cache efficiency,
  // they reuse the same memory area.
  static constexpr unsigned char kXxh3Secrets[XXH3_SECRET_SIZE_MIN + 1] = {
      0xe7, 0x8b, 0x13, 0xf9, 0xfc, 0xb5, 0x8e, 0xef, 0x81, 0x48, 0x2c, 0xbf, 0xf9, 0x9f,
      0xc1, 0x1e, 0x43, 0x6d, 0xbf, 0xa6, 0x6d, 0xb5, 0x72, 0xbc, 0x97, 0xd8, 0x61, 0x24,
      0x0f, 0x12, 0xe3, 0x05, 0x21, 0xf7, 0x5c, 0x66, 0x67, 0xa5, 0x65, 0x03, 0x96, 0x26,
      0x69, 0xd8, 0x29, 0x20, 0xf8, 0xc7, 0xb0, 0x3d, 0xdd, 0x7d, 0x18, 0xa0, 0x60, 0x75,
      0x92, 0xa4, 0xce, 0xba, 0xc0, 0x77, 0xf4, 0xac, 0xb7, 0x03, 0x53, 0xf0, 0x98, 0xce,
      0xe6, 0x2b, 0x20, 0xc7, 0x82, 0x91, 0xab, 0xbf, 0x68, 0x5c, 0x62, 0x4d, 0x33, 0xa3,
      0xe1, 0xb3, 0xff, 0x97, 0x54, 0x4c, 0x44, 0x34, 0xb5, 0xb9, 0x32, 0x4c, 0x75, 0x42,
      0x89, 0x53, 0x94, 0xd4, 0x9f, 0x2b, 0x76, 0x4d, 0x4e, 0xe6, 0xfa, 0x15, 0x3e, 0xc1,
      0xdb, 0x71, 0x4b, 0x2c, 0x94, 0xf5, 0xfc, 0x8c, 0x89, 0x4b, 0xfb, 0xc1, 0x82, 0xa5,
      0x6a, 0x53, 0xf9, 0x4a, 0xba, 0xce, 0x1f, 0xc0, 0x97, 0x1a, 0x87};

  static_assert(AlgNum < 2, "AlgNum too large");
  static constexpr auto secret = kXxh3Secrets + AlgNum;
  return XXH3_64bits_withSecret(data, static_cast<size_t>(length), secret,
                                XXH3_SECRET_SIZE_MIN);
}

// XXX add a HashEq<ArrowType> struct with both hash and compare functions?

// ----------------------------------------------------------------------
// An open-addressing insert-only hash table (no deletes)

template <typename Payload>
class HashTable {
 public:
  static constexpr hash_t kSentinel = 0ULL;
  static constexpr int64_t kLoadFactor = 2UL;

  struct Entry {
    hash_t h;
    Payload payload;

    // An entry is valid if the hash is different from the sentinel value
    operator bool() const { return h != kSentinel; }
  };

  HashTable(MemoryPool* pool, uint64_t capacity) : entries_builder_(pool) {
    DCHECK_NE(pool, nullptr);
    // Minimum of 32 elements
    capacity = std::max<uint64_t>(capacity, 32UL);
    capacity_ = bit_util::NextPower2(capacity);
    capacity_mask_ = capacity_ - 1;
    size_ = 0;

    DCHECK_OK(UpsizeBuffer(capacity_));
  }

  // Lookup with non-linear probing
  // cmp_func should have signature bool(const Payload*).
  // Return a (Entry*, found) pair.
  template <typename CmpFunc>
  std::pair<Entry*, bool> Lookup(hash_t h, CmpFunc&& cmp_func) {
    auto p = Lookup<DoCompare, CmpFunc>(h, entries_, capacity_mask_,
                                        std::forward<CmpFunc>(cmp_func));
    return {&entries_[p.first], p.second};
  }

  template <typename CmpFunc>
  std::pair<const Entry*, bool> Lookup(hash_t h, CmpFunc&& cmp_func) const {
    auto p = Lookup<DoCompare, CmpFunc>(h, entries_, capacity_mask_,
                                        std::forward<CmpFunc>(cmp_func));
    return {&entries_[p.first], p.second};
  }

  Status Insert(Entry* entry, hash_t h, const Payload& payload) {
    // Ensure entry is empty before inserting
    assert(!*entry);
    entry->h = FixHash(h);
    entry->payload = payload;
    ++size_;

    if (ARROW_PREDICT_FALSE(NeedUpsizing())) {
      // Resize less frequently since it is expensive
      return Upsize(capacity_ * kLoadFactor * 2);
    }
    return Status::OK();
  }

  uint64_t size() const { return size_; }

  // Visit all non-empty entries in the table
  // The visit_func should have signature void(const Entry*)
  template <typename VisitFunc>
  void VisitEntries(VisitFunc&& visit_func) const {
    for (uint64_t i = 0; i < capacity_; i++) {
      const auto& entry = entries_[i];
      if (entry) {
        visit_func(&entry);
      }
    }
  }

 protected:
  // NoCompare is for when the value is known not to exist in the table
  enum CompareKind { DoCompare, NoCompare };

  // The workhorse lookup function
  template <CompareKind CKind, typename CmpFunc>
  std::pair<uint64_t, bool> Lookup(hash_t h, const Entry* entries, uint64_t size_mask,
                                   CmpFunc&& cmp_func) const {
    static constexpr uint8_t perturb_shift = 5;

    uint64_t index, perturb;
    const Entry* entry;

    h = FixHash(h);
    index = h & size_mask;
    perturb = (h >> perturb_shift) + 1U;

    while (true) {
      entry = &entries[index];
      if (CompareEntry<CKind, CmpFunc>(h, entry, std::forward<CmpFunc>(cmp_func))) {
        // Found
        return {index, true};
      }
      if (entry->h == kSentinel) {
        // Empty slot
        return {index, false};
      }

      // Perturbation logic inspired from CPython's set / dict object.
      // The goal is that all 64 bits of the unmasked hash value eventually
      // participate in the probing sequence, to minimize clustering.
      index = (index + perturb) & size_mask;
      perturb = (perturb >> perturb_shift) + 1U;
    }
  }

  template <CompareKind CKind, typename CmpFunc>
  bool CompareEntry(hash_t h, const Entry* entry, CmpFunc&& cmp_func) const {
    if (CKind == NoCompare) {
      return false;
    } else {
      return entry->h == h && cmp_func(&entry->payload);
    }
  }

  bool NeedUpsizing() const {
    // Keep the load factor <= 1/2
    return size_ * kLoadFactor >= capacity_;
  }

  Status UpsizeBuffer(uint64_t capacity) {
    RETURN_NOT_OK(entries_builder_.Resize(capacity));
    entries_ = entries_builder_.mutable_data();
    memset(static_cast<void*>(entries_), 0, capacity * sizeof(Entry));

    return Status::OK();
  }

  Status Upsize(uint64_t new_capacity) {
    assert(new_capacity > capacity_);
    uint64_t new_mask = new_capacity - 1;
    assert((new_capacity & new_mask) == 0);  // it's a power of two

    // Stash old entries and seal builder, effectively resetting the Buffer
    const Entry* old_entries = entries_;
    ARROW_ASSIGN_OR_RAISE(auto previous, entries_builder_.FinishWithLength(capacity_));
    // Allocate new buffer
    RETURN_NOT_OK(UpsizeBuffer(new_capacity));

    for (uint64_t i = 0; i < capacity_; i++) {
      const auto& entry = old_entries[i];
      if (entry) {
        // Dummy compare function will not be called
        auto p = Lookup<NoCompare>(entry.h, entries_, new_mask,
                                   [](const Payload*) { return false; });
        // Lookup<NoCompare> (and CompareEntry<NoCompare>) ensure that an
        // empty slots is always returned
        assert(!p.second);
        entries_[p.first] = entry;
      }
    }
    capacity_ = new_capacity;
    capacity_mask_ = new_mask;

    return Status::OK();
  }

  hash_t FixHash(hash_t h) const { return (h == kSentinel) ? 42U : h; }

  // The number of slots available in the hash table array.
  uint64_t capacity_;
  uint64_t capacity_mask_;
  // The number of used slots in the hash table array.
  uint64_t size_;

  Entry* entries_;
  TypedBufferBuilder<Entry> entries_builder_;
};

// SwissHashTable is a hash table adapated from the "SwissTable" family of hash tables
// from Abseil (https://abseil.io/blog/20180927-swisstables) (no deletes)
template <typename Payload>
class SwissHashTable {
 public:
  static constexpr hash_t kSentinel = 0ULL;
  static constexpr uint64_t kGroupSize = 8;
  static constexpr uint64_t kMaxAvgGroupLoad = 7;
  static_assert(kMaxAvgGroupLoad < kGroupSize);

  static constexpr uint64_t kH1Mask = 0xffffffffffffff80ULL;
  static constexpr uint64_t kH2Mask = 0x000000000000007fULL;
  static constexpr uint8_t kEmptyControlByte = 0b10000000;

  static constexpr uint64_t kLoBits = 0x0101010101010101;
  static constexpr uint64_t kHiBits = 0x8080808080808080;

  using H1 = uint64_t;
  using H2 = uint8_t;

  struct Entry {
    hash_t h;
    Payload payload;
    uint64_t entry_index;

    // An entry is valid if the hash is different from the sentinel value
    explicit operator bool() const { return h != kSentinel; }
  };

  // TODO(SGZW): support kGroupSize = 16 by simd
  struct Group {
    Entry entries[kGroupSize];
  };

  // metadata is the h2 metadata array for a group.
  // find operations first probe the controls bytes
  // to filter candidates before matching keys
  struct GroupMeta {
    uint8_t control_bytes[kGroupSize];
  };

  SwissHashTable(MemoryPool* pool, uint64_t capacity)
      : group_builder_(pool), group_meta_builder_(pool) {
    DCHECK_NE(pool, nullptr);
    groups_count_ = NumGroups(capacity);
    limit_ = groups_count_ * kMaxAvgGroupLoad;
    size_ = 0;
    DCHECK_OK(UpsizeBuffer(groups_count_));
  }

  // Lookup with non-linear probing
  // cmp_func should have signature bool(const Payload*).
  // Return a (Entry*, found) pair.
  template <typename CmpFunc>
  std::pair<Entry*, bool> Lookup(hash_t h, CmpFunc&& cmp_func) {
    return DoLookup<DoCompare, CmpFunc>(h);
  }

  template <typename CmpFunc>
  std::pair<const Entry*, bool> Lookup(hash_t h, CmpFunc&& cmp_func) const {
    return DoLookup<DoCompare, CmpFunc>(h);
  }

  Status Insert(Entry* entry, hash_t h, const Payload& payload) {
    return DoInsert(entry, h, payload);
  }

  uint64_t size() const { return size_; }

  // Visit all non-empty entries in the table
  // The visit_func should have signature void(const Entry*)
  template <typename VisitFunc>
  void VisitEntries(VisitFunc&& visit_func) const {
    for (uint64_t i = 0; i < groups_count_; i++) {
      const auto& group = groups_[i];
      for (uint16_t j = 0; j < kGroupSize; j++) {
        const auto& entry = group.entries[j];
        if (entry) {
          visit_func(&entry);
        }
      }
    }
  }

 protected:
  // NoCompare is for when the value is known not to exist in the table
  enum CompareKind { DoCompare, NoCompare };

  Status DoInsert(Entry* entry, hash_t h, Payload payload) {
    // Ensure entry is empty before inserting
    assert(!*entry);
    assert(entry->entry_index != 0);
    entry->h = h;
    entry->payload = std::move(payload);
    ++size_;

    auto p = UnPackEntryIndex(entry->index);
    auto group_index = p.first;
    auto group_internal_index = p.second;

    // update meta
    group_metas_[group_index].control_bytes[group_internal_index] = h & kH2Mask;

    if (ARROW_PREDICT_FALSE(NeedUpsizing())) {
      // Resize less frequently since it is expensive
      return Upsize();
    }
    return Status::OK();
  }

  // The workhorse lookup function
  template <CompareKind CKind, typename CmpFunc>
  std::pair<const Entry*, bool> DoLookup(hash_t h, CmpFunc&& cmp_func) const {
    auto hash_pair = SplitHash(h);
    auto h1 = hash_pair.first;
    auto h2 = hash_pair.second;

    auto group_index = ProbeStart(h1);
    while (true) {
      const auto& group = groups_[group_index];
      // probe
      auto match_value = GroupMetaMatchH2(group_metas_[group_index], h2);
      while (match_value != 0) {
        auto group_internal_index = NextMatchGroupInternalIndex(&match_value);
        auto* entry = &group.entries[group_internal_index];
        if (CompareEntry<CKind, CmpFunc>(h, entry, std::forward<CmpFunc>(cmp_func))) {
          // Found
          entry->index = PackEntryIndex(group_index, group_internal_index);
          return {entry, true};
        }
      }

      // stop probing if we see an empty slot
      auto match_empty_value = GroupMetaMatchEmpty(group_metas_[group_index]);
      if (match_empty_value != 0) {
        auto group_internal_index = NextMatchGroupInternalIndex(&match_empty_value);
        auto* entry = &group.entries[group_internal_index];
        // Not Found
        entry->index = PackEntryIndex(group_index, group_internal_index);
        return {entry, false};
      }

      // next group
      ++group_index;
      if (ARROW_PREDICT_FALSE(group_index >= groups_count_)) {
        group_index = 0;
      }
    }
  }

  template <CompareKind CKind, typename CmpFunc>
  bool CompareEntry(hash_t h, const Entry* entry, CmpFunc&& cmp_func) const {
    if (CKind == NoCompare) {
      return false;
    } else {
      return entry->h == h && cmp_func(&entry->payload);
    }
  }

  bool NeedUpsizing() const {
    // Keep the load factor(size_ / (groups_count_ * kGroupSize)) <= kMaxAvgGroupLoad /
    // kGroupSize
    return size_ >= limit_;
  }

  Status UpsizeBuffer(uint64_t groups_count) {
    RETURN_NOT_OK(group_builder_.Resize(groups_count));
    RETURN_NOT_OK(group_meta_builder_.Resize(groups_count));
    groups_ = group_builder_.mutable_data();
    group_metas_ = group_meta_builder_.mutable_data();
    memset(static_cast<void*>(groups_), 0, groups_count * sizeof(Entry));
    memset(static_cast<void*>(group_metas_), kEmptyControlByte,
           groups_count * sizeof(Entry));
    return Status::OK();
  }

  Status Upsize() {
    auto old_groups_count = groups_count_;
    auto old_size = size_;

    // Stash old entries and seal builder, effectively resetting the Buffer
    const Group* old_groups = groups_;
    ARROW_ASSIGN_OR_RAISE(auto previous_groups,
                          group_builder_.FinishWithLength(old_groups_count));
    ARROW_ASSIGN_OR_RAISE(auto previous_metas,
                          group_meta_builder_.FinishWithLength(old_groups_count));

    groups_count_ = old_groups_count << 1;
    limit_ = groups_count_ * kMaxAvgGroupLoad;
    size_ = 0;
    RETURN_NOT_OK(UpsizeBuffer(groups_count_));

    uint64_t reinsert_count = 0;
    for (uint64_t i = 0; i < old_groups; i++) {
      auto& old_group = old_groups[i];
      for (uint16_t j = 0; j < kGroupSize; j++) {
        auto* old_entry = *old_group.entries[j];
        if (old_entry) {
          ++reinsert_count;
          auto p = DoLookup<CompareKind::NoCompare>(old_entry->h,
                                                    [](const Payload*) { return false; });
          assert(!p.second);
          RETURN_NOT_OK(DoInsert(p.first, old_entry->h, std::move(old_entry->payload)));
        }
      }
    }
    assert(reinsert_count == old_size);
    return Status::OK();
  }

  // numGroups returns the minimum number of groups needed to store |n| elems.
  uint64_t NumGroups(uint64_t n) const noexcept {
    auto groups = (n + kMaxAvgGroupLoad - 1) / kMaxAvgGroupLoad;
    if (groups == 0) {
      groups = 1;
    }
    return groups;
  }

  std::pair<H1, H2> SplitHash(uint64_t hash_value) const noexcept {
    return {(hash_value & kH1Mask) >> 7, hash_value & kH2Mask};
  }

  uint64_t ProbeStart(H1 h1) const noexcept { return h1 % groups_count_; }

  uint64_t GroupMetaMatchH2(const GroupMeta& meta, H2 h2) const noexcept {
    auto u64_meta = *static_cast<uint64_t*>(meta.control_bits);
    return arrow::bit_util::HasZeroByte(u64_meta ^ (kLoBits * h2));
  }

  uint64_t GroupMetaMatchEmpty(const GroupMeta& meta) {
    auto u64_meta = *static_cast<uint64_t*>(meta.control_bits);
    return arrow::bit_util::HasZeroByte(u64_meta ^ kHiBits);
  }

  uint64_t NextMatchGroupInternalIndex(uint64_t* match_value) {
    uint64_t s = arrow::bit_util::CountTrailingZeros(*match_value);
    *match_value &= ~(1 << s);  // clear bit |s|
    return s >> 3;              // div by 8
  }

  uint64_t PackEntryIndex(uint64_t group_index, uint64_t group_internal_index) {
    return (group_index << 8) | group_internal_index;
  }

  std::pair<uint64_t, uint64_t> UnPackEntryIndex(uint64_t entry_index) {
    return {entry_index >> 8, entry_index & 0xff};
  }

  // The number of groups available in the hash table array.
  uint64_t groups_count_;
  // The number of used slots in the hash table array.
  uint64_t size_;
  // The number of max used slots in the hash table array.
  uint64_t limit_;

  Group* groups_;
  TypedBufferBuilder<Group> group_builder_;

  GroupMeta* group_metas_;
  TypedBufferBuilder<GroupMeta> group_meta_builder_;
};

// XXX typedef memo_index_t int32_t ?

constexpr int32_t kKeyNotFound = -1;

// ----------------------------------------------------------------------
// A base class for memoization table.

class MemoTable {
 public:
  virtual ~MemoTable() = default;

  virtual int32_t size() const = 0;
};

// ----------------------------------------------------------------------
// A memoization table for memory-cheap scalar values.

// The memoization table remembers and allows to look up the insertion
// index for each key.

template <typename Scalar, template <class> class HashTableTemplateType = HashTable>
class ScalarMemoTable : public MemoTable {
 public:
  explicit ScalarMemoTable(MemoryPool* pool, int64_t entries = 0)
      : hash_table_(pool, static_cast<uint64_t>(entries)) {}

  int32_t Get(const Scalar& value) const {
    auto cmp_func = [value](const Payload* payload) -> bool {
      return ScalarHelper<Scalar, 0>::CompareScalars(payload->value, value);
    };
    hash_t h = ComputeHash(value);
    auto p = hash_table_.Lookup(h, cmp_func);
    if (p.second) {
      return p.first->payload.memo_index;
    } else {
      return kKeyNotFound;
    }
  }

  template <typename Func1, typename Func2>
  Status GetOrInsert(const Scalar& value, Func1&& on_found, Func2&& on_not_found,
                     int32_t* out_memo_index) {
    auto cmp_func = [value](const Payload* payload) -> bool {
      return ScalarHelper<Scalar, 0>::CompareScalars(value, payload->value);
    };
    hash_t h = ComputeHash(value);
    auto p = hash_table_.Lookup(h, cmp_func);
    int32_t memo_index;
    if (p.second) {
      memo_index = p.first->payload.memo_index;
      on_found(memo_index);
    } else {
      memo_index = size();
      RETURN_NOT_OK(hash_table_.Insert(p.first, h, {value, memo_index}));
      on_not_found(memo_index);
    }
    *out_memo_index = memo_index;
    return Status::OK();
  }

  Status GetOrInsert(const Scalar& value, int32_t* out_memo_index) {
    return GetOrInsert(
        value, [](int32_t i) {}, [](int32_t i) {}, out_memo_index);
  }

  int32_t GetNull() const { return null_index_; }

  template <typename Func1, typename Func2>
  int32_t GetOrInsertNull(Func1&& on_found, Func2&& on_not_found) {
    int32_t memo_index = GetNull();
    if (memo_index != kKeyNotFound) {
      on_found(memo_index);
    } else {
      null_index_ = memo_index = size();
      on_not_found(memo_index);
    }
    return memo_index;
  }

  int32_t GetOrInsertNull() {
    return GetOrInsertNull([](int32_t i) {}, [](int32_t i) {});
  }

  // The number of entries in the memo table +1 if null was added.
  // (which is also 1 + the largest memo index)
  int32_t size() const override {
    return static_cast<int32_t>(hash_table_.size()) + (GetNull() != kKeyNotFound);
  }

  // Copy values starting from index `start` into `out_data`
  void CopyValues(int32_t start, Scalar* out_data) const {
    hash_table_.VisitEntries([=](const HashTableEntry* entry) {
      int32_t index = entry->payload.memo_index - start;
      if (index >= 0) {
        out_data[index] = entry->payload.value;
      }
    });
    // Zero-initialize the null entry
    if (null_index_ != kKeyNotFound) {
      int32_t index = null_index_ - start;
      if (index >= 0) {
        out_data[index] = Scalar{};
      }
    }
  }

  void CopyValues(Scalar* out_data) const { CopyValues(0, out_data); }

 protected:
  struct Payload {
    Scalar value;
    int32_t memo_index;
  };

  using HashTableType = HashTableTemplateType<Payload>;
  using HashTableEntry = typename HashTableType::Entry;
  HashTableType hash_table_;
  int32_t null_index_ = kKeyNotFound;

  hash_t ComputeHash(const Scalar& value) const {
    return ScalarHelper<Scalar, 0>::ComputeHash(value);
  }

 public:
  // defined here so that `HashTableType` is visible
  // Merge entries from `other_table` into `this->hash_table_`.
  Status MergeTable(const ScalarMemoTable& other_table) {
    const HashTableType& other_hashtable = other_table.hash_table_;

    other_hashtable.VisitEntries([this](const HashTableEntry* other_entry) {
      int32_t unused;
      DCHECK_OK(this->GetOrInsert(other_entry->payload.value, &unused));
    });
    // TODO: ARROW-17074 - implement proper error handling
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// A memoization table for small scalar values, using direct indexing

template <typename Scalar, typename Enable = void>
struct SmallScalarTraits {};

template <>
struct SmallScalarTraits<bool> {
  static constexpr int32_t cardinality = 2;

  static uint32_t AsIndex(bool value) { return value ? 1 : 0; }
};

template <typename Scalar>
struct SmallScalarTraits<Scalar, enable_if_t<std::is_integral<Scalar>::value>> {
  using Unsigned = typename std::make_unsigned<Scalar>::type;

  static constexpr int32_t cardinality = 1U + std::numeric_limits<Unsigned>::max();

  static uint32_t AsIndex(Scalar value) { return static_cast<Unsigned>(value); }
};

template <typename Scalar, template <class> class HashTableTemplateType = HashTable>
class SmallScalarMemoTable : public MemoTable {
 public:
  explicit SmallScalarMemoTable(MemoryPool* pool, int64_t entries = 0) {
    std::fill(value_to_index_, value_to_index_ + cardinality + 1, kKeyNotFound);
    index_to_value_.reserve(cardinality);
  }

  int32_t Get(const Scalar value) const {
    auto value_index = AsIndex(value);
    return value_to_index_[value_index];
  }

  template <typename Func1, typename Func2>
  Status GetOrInsert(const Scalar value, Func1&& on_found, Func2&& on_not_found,
                     int32_t* out_memo_index) {
    auto value_index = AsIndex(value);
    auto memo_index = value_to_index_[value_index];
    if (memo_index == kKeyNotFound) {
      memo_index = static_cast<int32_t>(index_to_value_.size());
      index_to_value_.push_back(value);
      value_to_index_[value_index] = memo_index;
      DCHECK_LT(memo_index, cardinality + 1);
      on_not_found(memo_index);
    } else {
      on_found(memo_index);
    }
    *out_memo_index = memo_index;
    return Status::OK();
  }

  Status GetOrInsert(const Scalar value, int32_t* out_memo_index) {
    return GetOrInsert(
        value, [](int32_t i) {}, [](int32_t i) {}, out_memo_index);
  }

  int32_t GetNull() const { return value_to_index_[cardinality]; }

  template <typename Func1, typename Func2>
  int32_t GetOrInsertNull(Func1&& on_found, Func2&& on_not_found) {
    auto memo_index = GetNull();
    if (memo_index == kKeyNotFound) {
      memo_index = value_to_index_[cardinality] = size();
      index_to_value_.push_back(0);
      on_not_found(memo_index);
    } else {
      on_found(memo_index);
    }
    return memo_index;
  }

  int32_t GetOrInsertNull() {
    return GetOrInsertNull([](int32_t i) {}, [](int32_t i) {});
  }

  // The number of entries in the memo table
  // (which is also 1 + the largest memo index)
  int32_t size() const override { return static_cast<int32_t>(index_to_value_.size()); }

  // Merge entries from `other_table` into `this`.
  Status MergeTable(const SmallScalarMemoTable& other_table) {
    for (const Scalar& other_val : other_table.index_to_value_) {
      int32_t unused;
      RETURN_NOT_OK(this->GetOrInsert(other_val, &unused));
    }
    return Status::OK();
  }

  // Copy values starting from index `start` into `out_data`
  void CopyValues(int32_t start, Scalar* out_data) const {
    DCHECK_GE(start, 0);
    DCHECK_LE(static_cast<size_t>(start), index_to_value_.size());
    int64_t offset = start * static_cast<int32_t>(sizeof(Scalar));
    memcpy(out_data, index_to_value_.data() + offset, (size() - start) * sizeof(Scalar));
  }

  void CopyValues(Scalar* out_data) const { CopyValues(0, out_data); }

  const std::vector<Scalar>& values() const { return index_to_value_; }

 protected:
  static constexpr auto cardinality = SmallScalarTraits<Scalar>::cardinality;
  static_assert(cardinality <= 256, "cardinality too large for direct-addressed table");

  uint32_t AsIndex(Scalar value) const {
    return SmallScalarTraits<Scalar>::AsIndex(value);
  }

  // The last index is reserved for the null element.
  int32_t value_to_index_[cardinality + 1];
  std::vector<Scalar> index_to_value_;
};

// ----------------------------------------------------------------------
// A memoization table for variable-sized binary data.

template <typename BinaryBuilderT>
class BinaryMemoTable : public MemoTable {
 public:
  using builder_offset_type = typename BinaryBuilderT::offset_type;
  explicit BinaryMemoTable(MemoryPool* pool, int64_t entries = 0,
                           int64_t values_size = -1)
      : hash_table_(pool, static_cast<uint64_t>(entries)), binary_builder_(pool) {
    const int64_t data_size = (values_size < 0) ? entries * 4 : values_size;
    DCHECK_OK(binary_builder_.Resize(entries));
    DCHECK_OK(binary_builder_.ReserveData(data_size));
  }

  int32_t Get(const void* data, builder_offset_type length) const {
    hash_t h = ComputeStringHash<0>(data, length);
    auto p = Lookup(h, data, length);
    if (p.second) {
      return p.first->payload.memo_index;
    } else {
      return kKeyNotFound;
    }
  }

  int32_t Get(std::string_view value) const {
    return Get(value.data(), static_cast<builder_offset_type>(value.length()));
  }

  template <typename Func1, typename Func2>
  Status GetOrInsert(const void* data, builder_offset_type length, Func1&& on_found,
                     Func2&& on_not_found, int32_t* out_memo_index) {
    hash_t h = ComputeStringHash<0>(data, length);
    auto p = Lookup(h, data, length);
    int32_t memo_index;
    if (p.second) {
      memo_index = p.first->payload.memo_index;
      on_found(memo_index);
    } else {
      memo_index = size();
      // Insert string value
      RETURN_NOT_OK(binary_builder_.Append(static_cast<const char*>(data), length));
      // Insert hash entry
      RETURN_NOT_OK(
          hash_table_.Insert(const_cast<HashTableEntry*>(p.first), h, {memo_index}));

      on_not_found(memo_index);
    }
    *out_memo_index = memo_index;
    return Status::OK();
  }

  template <typename Func1, typename Func2>
  Status GetOrInsert(std::string_view value, Func1&& on_found, Func2&& on_not_found,
                     int32_t* out_memo_index) {
    return GetOrInsert(value.data(), static_cast<builder_offset_type>(value.length()),
                       std::forward<Func1>(on_found), std::forward<Func2>(on_not_found),
                       out_memo_index);
  }

  Status GetOrInsert(const void* data, builder_offset_type length,
                     int32_t* out_memo_index) {
    return GetOrInsert(
        data, length, [](int32_t i) {}, [](int32_t i) {}, out_memo_index);
  }

  Status GetOrInsert(std::string_view value, int32_t* out_memo_index) {
    return GetOrInsert(value.data(), static_cast<builder_offset_type>(value.length()),
                       out_memo_index);
  }

  int32_t GetNull() const { return null_index_; }

  template <typename Func1, typename Func2>
  int32_t GetOrInsertNull(Func1&& on_found, Func2&& on_not_found) {
    int32_t memo_index = GetNull();
    if (memo_index == kKeyNotFound) {
      memo_index = null_index_ = size();
      DCHECK_OK(binary_builder_.AppendNull());
      on_not_found(memo_index);
    } else {
      on_found(memo_index);
    }
    return memo_index;
  }

  int32_t GetOrInsertNull() {
    return GetOrInsertNull([](int32_t i) {}, [](int32_t i) {});
  }

  // The number of entries in the memo table
  // (which is also 1 + the largest memo index)
  int32_t size() const override {
    return static_cast<int32_t>(hash_table_.size() + (GetNull() != kKeyNotFound));
  }

  int64_t values_size() const { return binary_builder_.value_data_length(); }

  // Copy (n + 1) offsets starting from index `start` into `out_data`
  template <class Offset>
  void CopyOffsets(int32_t start, Offset* out_data) const {
    DCHECK_LE(start, size());

    const builder_offset_type* offsets = binary_builder_.offsets_data();
    const builder_offset_type delta =
        start < binary_builder_.length() ? offsets[start] : 0;
    for (int32_t i = start; i < size(); ++i) {
      const builder_offset_type adjusted_offset = offsets[i] - delta;
      Offset cast_offset = static_cast<Offset>(adjusted_offset);
      assert(static_cast<builder_offset_type>(cast_offset) ==
             adjusted_offset);  // avoid truncation
      *out_data++ = cast_offset;
    }

    // Copy last value since BinaryBuilder only materializes it on in Finish()
    *out_data = static_cast<Offset>(binary_builder_.value_data_length() - delta);
  }

  template <class Offset>
  void CopyOffsets(Offset* out_data) const {
    CopyOffsets(0, out_data);
  }

  // Copy values starting from index `start` into `out_data`
  void CopyValues(int32_t start, uint8_t* out_data) const {
    CopyValues(start, -1, out_data);
  }

  // Same as above, but check output size in debug mode
  void CopyValues(int32_t start, int64_t out_size, uint8_t* out_data) const {
    DCHECK_LE(start, size());

    // The absolute byte offset of `start` value in the binary buffer.
    const builder_offset_type offset = binary_builder_.offset(start);
    const auto length = binary_builder_.value_data_length() - static_cast<size_t>(offset);

    if (out_size != -1) {
      assert(static_cast<int64_t>(length) <= out_size);
    }

    auto view = binary_builder_.GetView(start);
    memcpy(out_data, view.data(), length);
  }

  void CopyValues(uint8_t* out_data) const { CopyValues(0, -1, out_data); }

  void CopyValues(int64_t out_size, uint8_t* out_data) const {
    CopyValues(0, out_size, out_data);
  }

  void CopyFixedWidthValues(int32_t start, int32_t width_size, int64_t out_size,
                            uint8_t* out_data) const {
    // This method exists to cope with the fact that the BinaryMemoTable does
    // not know the fixed width when inserting the null value. The data
    // buffer hold a zero length string for the null value (if found).
    //
    // Thus, the method will properly inject an empty value of the proper width
    // in the output buffer.
    //
    if (start >= size()) {
      return;
    }

    int32_t null_index = GetNull();
    if (null_index < start) {
      // Nothing to skip, proceed as usual.
      CopyValues(start, out_size, out_data);
      return;
    }

    builder_offset_type left_offset = binary_builder_.offset(start);

    // Ensure that the data length is exactly missing width_size bytes to fit
    // in the expected output (n_values * width_size).
#ifndef NDEBUG
    int64_t data_length = values_size() - static_cast<size_t>(left_offset);
    assert(data_length + width_size == out_size);
    ARROW_UNUSED(data_length);
#endif

    auto in_data = binary_builder_.value_data() + left_offset;
    // The null use 0-length in the data, slice the data in 2 and skip by
    // width_size in out_data. [part_1][width_size][part_2]
    auto null_data_offset = binary_builder_.offset(null_index);
    auto left_size = null_data_offset - left_offset;
    if (left_size > 0) {
      memcpy(out_data, in_data + left_offset, left_size);
    }
    // Zero-initialize the null entry
    memset(out_data + left_size, 0, width_size);

    auto right_size = values_size() - static_cast<size_t>(null_data_offset);
    if (right_size > 0) {
      // skip the null fixed size value.
      auto out_offset = left_size + width_size;
      assert(out_data + out_offset + right_size == out_data + out_size);
      memcpy(out_data + out_offset, in_data + null_data_offset, right_size);
    }
  }

  // Visit the stored values in insertion order.
  // The visitor function should have the signature `void(std::string_view)`
  // or `void(const std::string_view&)`.
  template <typename VisitFunc>
  void VisitValues(int32_t start, VisitFunc&& visit) const {
    for (int32_t i = start; i < size(); ++i) {
      visit(binary_builder_.GetView(i));
    }
  }

 protected:
  struct Payload {
    int32_t memo_index;
  };

  using HashTableType = HashTable<Payload>;
  using HashTableEntry = typename HashTable<Payload>::Entry;
  HashTableType hash_table_;
  BinaryBuilderT binary_builder_;

  int32_t null_index_ = kKeyNotFound;

  std::pair<const HashTableEntry*, bool> Lookup(hash_t h, const void* data,
                                                builder_offset_type length) const {
    auto cmp_func = [&](const Payload* payload) {
      std::string_view lhs = binary_builder_.GetView(payload->memo_index);
      std::string_view rhs(static_cast<const char*>(data), length);
      return lhs == rhs;
    };
    return hash_table_.Lookup(h, cmp_func);
  }

 public:
  Status MergeTable(const BinaryMemoTable& other_table) {
    other_table.VisitValues(0, [this](std::string_view other_value) {
      int32_t unused;
      DCHECK_OK(this->GetOrInsert(other_value, &unused));
    });
    return Status::OK();
  }
};

template <typename T, typename Enable = void>
struct HashTraits {};

template <>
struct HashTraits<BooleanType> {
  using MemoTableType = SmallScalarMemoTable<bool>;
};

template <typename T>
struct HashTraits<T, enable_if_8bit_int<T>> {
  using c_type = typename T::c_type;
  using MemoTableType = SmallScalarMemoTable<typename T::c_type>;
};

template <typename T>
struct HashTraits<T, enable_if_t<has_c_type<T>::value && !is_8bit_int<T>::value>> {
  using c_type = typename T::c_type;
  using MemoTableType = ScalarMemoTable<c_type, HashTable>;
};

template <typename T>
struct HashTraits<T, enable_if_t<has_string_view<T>::value &&
                                 !std::is_base_of<LargeBinaryType, T>::value>> {
  using MemoTableType = BinaryMemoTable<BinaryBuilder>;
};

template <typename T>
struct HashTraits<T, enable_if_decimal<T>> {
  using MemoTableType = BinaryMemoTable<BinaryBuilder>;
};

template <typename T>
struct HashTraits<T, enable_if_t<std::is_base_of<LargeBinaryType, T>::value>> {
  using MemoTableType = BinaryMemoTable<LargeBinaryBuilder>;
};

template <typename MemoTableType>
static inline Status ComputeNullBitmap(MemoryPool* pool, const MemoTableType& memo_table,
                                       int64_t start_offset, int64_t* null_count,
                                       std::shared_ptr<Buffer>* null_bitmap) {
  int64_t dict_length = static_cast<int64_t>(memo_table.size()) - start_offset;
  int64_t null_index = memo_table.GetNull();

  *null_count = 0;
  *null_bitmap = nullptr;

  if (null_index != kKeyNotFound && null_index >= start_offset) {
    null_index -= start_offset;
    *null_count = 1;
    ARROW_ASSIGN_OR_RAISE(*null_bitmap,
                          internal::BitmapAllButOne(pool, dict_length, null_index));
  }

  return Status::OK();
}

struct StringViewHash {
  // std::hash compatible hasher for use with std::unordered_*
  // (the std::hash specialization provided by nonstd constructs std::string
  // temporaries then invokes std::hash<std::string> against those)
  hash_t operator()(std::string_view value) const {
    return ComputeStringHash<0>(value.data(), static_cast<int64_t>(value.size()));
  }
};

}  // namespace internal
}  // namespace arrow
