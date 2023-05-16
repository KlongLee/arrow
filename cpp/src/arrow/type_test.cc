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

// Unit tests for DataType (and subclasses), Field, and Schema

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <gmock/gmock.h>

#include "arrow/array.h"
#include "arrow/memory_pool.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"

namespace arrow {

using testing::ElementsAre;

using internal::checked_cast;
using internal::checked_pointer_cast;

TEST(TestTypeId, AllTypeIds) {
  const auto all_ids = AllTypeIds();
  ASSERT_EQ(static_cast<int>(all_ids.size()), Type::MAX_ID);
}

template <typename ReprFunc>
void CheckTypeIdReprs(ReprFunc&& repr_func, bool expect_uppercase) {
  std::unordered_set<std::string> unique_reprs;
  const auto all_ids = AllTypeIds();
  for (const auto id : all_ids) {
    std::string repr = repr_func(id);
    ASSERT_TRUE(std::all_of(repr.begin(), repr.end(),
                            [=](const char c) {
                              return c == '_' || std::isdigit(c) ||
                                     (expect_uppercase ? std::isupper(c)
                                                       : std::islower(c));
                            }))
        << "Invalid type id repr: '" << repr << "'";
    unique_reprs.insert(std::move(repr));
  }
  // No duplicates
  ASSERT_EQ(unique_reprs.size(), all_ids.size());
}

TEST(TestTypeId, ToString) {
  // Should be all uppercase strings (corresponding to the enum member names)
  CheckTypeIdReprs([](Type::type id) { return internal::ToString(id); },
                   /* expect_uppercase=*/true);
}

TEST(TestTypeId, ToTypeName) {
  // Should be all lowercase strings (corresponding to TypeClass::type_name())
  CheckTypeIdReprs([](Type::type id) { return internal::ToTypeName(id); },
                   /* expect_uppercase=*/false);
}

TEST(TestField, Basics) {
  Field f0("f0", int32());
  Field f0_nn("f0", int32(), false);

  ASSERT_EQ(f0.name(), "f0");
  ASSERT_EQ(f0.type()->ToString(), int32()->ToString());

  ASSERT_TRUE(f0.nullable());
  ASSERT_FALSE(f0_nn.nullable());
}

TEST(TestField, ToString) {
  auto metadata = key_value_metadata({"foo", "bar"}, {"bizz", "buzz"});
  auto f0 = field("f0", int32(), false, metadata);

  std::string result = f0->ToString(/*print_metadata=*/true);
  std::string expected = R"(f0: int32 not null
-- metadata --
foo: bizz
bar: buzz)";
  ASSERT_EQ(expected, result);

  result = f0->ToString();
  expected = "f0: int32 not null";
  ASSERT_EQ(expected, result);
}

TEST(TestField, Equals) {
  auto meta1 = key_value_metadata({{"a", "1"}, {"b", "2"}});
  // Different from meta1
  auto meta2 = key_value_metadata({{"a", "1"}, {"b", "3"}});
  // Equal to meta1, though in different order
  auto meta3 = key_value_metadata({{"b", "2"}, {"a", "1"}});

  Field f0("f0", int32());
  Field f0_nn("f0", int32(), false);
  Field f0_other("f0", int32());
  Field f0_with_meta1("f0", int32(), true, meta1);
  Field f0_with_meta2("f0", int32(), true, meta2);
  Field f0_with_meta3("f0", int32(), true, meta3);

  AssertFieldEqual(f0, f0_other);
  AssertFieldNotEqual(f0, f0_nn);
  AssertFieldNotEqual(f0, f0_with_meta1, /*check_metadata=*/true);
  AssertFieldNotEqual(f0_with_meta1, f0_with_meta2, /*check_metadata=*/true);
  AssertFieldEqual(f0_with_meta1, f0_with_meta3, /*check_metadata=*/true);

  AssertFieldEqual(f0, f0_with_meta1);
  AssertFieldEqual(f0, f0_with_meta2);
  AssertFieldEqual(f0_with_meta1, f0_with_meta2);

  // operator==(), where check_metadata == false
  ASSERT_EQ(f0, f0_other);
  ASSERT_NE(f0, f0_nn);
  ASSERT_EQ(f0, f0_with_meta1);
  ASSERT_EQ(f0_with_meta1, f0_with_meta2);
}

#define ASSERT_COMPATIBLE_IMPL(NAME, TYPE, PLURAL)                        \
  void Assert##NAME##Compatible(const TYPE& left, const TYPE& right) {    \
    ASSERT_TRUE(left.IsCompatibleWith(right))                             \
        << PLURAL << left.ToString() << "' and '" << right.ToString()     \
        << "' should be compatible";                                      \
  }                                                                       \
                                                                          \
  void Assert##NAME##Compatible(const std::shared_ptr<TYPE>& left,        \
                                const std::shared_ptr<TYPE>& right) {     \
    ASSERT_NE(left, nullptr);                                             \
    ASSERT_NE(right, nullptr);                                            \
    Assert##NAME##Compatible(*left, *right);                              \
  }                                                                       \
                                                                          \
  void Assert##NAME##NotCompatible(const TYPE& left, const TYPE& right) { \
    ASSERT_FALSE(left.IsCompatibleWith(right))                            \
        << PLURAL << left.ToString() << "' and '" << right.ToString()     \
        << "' should not be compatible";                                  \
  }                                                                       \
                                                                          \
  void Assert##NAME##NotCompatible(const std::shared_ptr<TYPE>& left,     \
                                   const std::shared_ptr<TYPE>& right) {  \
    ASSERT_NE(left, nullptr);                                             \
    ASSERT_NE(right, nullptr);                                            \
    Assert##NAME##NotCompatible(*left, *right);                           \
  }

ASSERT_COMPATIBLE_IMPL(Field, Field, "fields")
#undef ASSERT_COMPATIBLE_IMPL

TEST(TestField, IsCompatibleWith) {
  auto meta1 = key_value_metadata({{"a", "1"}, {"b", "2"}});
  // Different from meta1
  auto meta2 = key_value_metadata({{"a", "1"}, {"b", "3"}});
  // Equal to meta1, though in different order
  auto meta3 = key_value_metadata({{"b", "2"}, {"a", "1"}});

  Field f0("f0", int32());
  Field f0_nn("f0", int32(), false);
  Field f0_nt("f0", null());
  Field f0_other("f0", int32());
  Field f0_with_meta1("f0", int32(), true, meta1);
  Field f0_with_meta2("f0", int32(), true, meta2);
  Field f0_with_meta3("f0", int32(), true, meta3);
  Field other("other", int64());

  AssertFieldCompatible(f0, f0_other);
  AssertFieldCompatible(f0, f0_with_meta1);
  AssertFieldCompatible(f0, f0_nn);
  AssertFieldCompatible(f0, f0_nt);
  AssertFieldCompatible(f0_nt, f0_with_meta1);
  AssertFieldCompatible(f0_with_meta1, f0_with_meta2);
  AssertFieldCompatible(f0_with_meta1, f0_with_meta3);
  AssertFieldNotCompatible(f0, other);
}

TEST(TestField, TestMetadataConstruction) {
  auto metadata = key_value_metadata({"foo", "bar"}, {"bizz", "buzz"});
  auto metadata2 = metadata->Copy();
  auto f0 = field("f0", int32(), true, metadata);
  auto f1 = field("f0", int32(), true, metadata2);
  ASSERT_TRUE(metadata->Equals(*f0->metadata()));
  AssertFieldEqual(f0, f1);
}

TEST(TestField, TestWithMetadata) {
  auto metadata = key_value_metadata({"foo", "bar"}, {"bizz", "buzz"});
  auto f0 = field("f0", int32());
  auto f1 = field("f0", int32(), true, metadata);
  std::shared_ptr<Field> f2 = f0->WithMetadata(metadata);

  AssertFieldEqual(f0, f2);
  AssertFieldNotEqual(f0, f2, /*check_metadata=*/true);

  AssertFieldEqual(f1, f2);
  AssertFieldEqual(f1, f2, /*check_metadata=*/true);

  // Ensure pointer equality for zero-copy
  ASSERT_EQ(metadata.get(), f1->metadata().get());
}

TEST(TestField, TestWithMergedMetadata) {
  auto metadata = key_value_metadata({"foo", "bar"}, {"bizz", "buzz"});
  auto f0 = field("f0", int32(), true, metadata);
  auto f1 = field("f0", int32());

  auto metadata2 = key_value_metadata({"bar", "baz"}, {"bozz", "bazz"});

  auto f2 = f0->WithMergedMetadata(metadata2);
  auto expected = field("f0", int32(), true, metadata->Merge(*metadata2));
  AssertFieldEqual(expected, f2);

  auto f3 = f1->WithMergedMetadata(metadata2);
  expected = field("f0", int32(), true, metadata2);
  AssertFieldEqual(expected, f3);
}

TEST(TestField, TestRemoveMetadata) {
  auto metadata = key_value_metadata({"foo", "bar"}, {"bizz", "buzz"});
  auto f0 = field("f0", int32());
  auto f1 = field("f0", int32(), true, metadata);
  std::shared_ptr<Field> f2 = f1->RemoveMetadata();
  ASSERT_EQ(f2->metadata(), nullptr);
}

TEST(TestField, TestEmptyMetadata) {
  // Empty metadata should be equivalent to no metadata at all
  auto metadata1 = key_value_metadata({});
  auto metadata2 = key_value_metadata({"foo"}, {"foo value"});

  auto f0 = field("f0", int32());
  auto f1 = field("f0", int32(), true, metadata1);
  auto f2 = field("f0", int32(), true, metadata2);

  AssertFieldEqual(f0, f1);
  AssertFieldEqual(f0, f2);
  AssertFieldEqual(f0, f1, /*check_metadata =*/true);
  AssertFieldNotEqual(f0, f2, /*check_metadata =*/true);
}

TEST(TestField, TestFlatten) {
  auto metadata = key_value_metadata({"foo", "bar"}, {"bizz", "buzz"});
  auto f0 = field("f0", int32(), true /* nullable */, metadata);
  auto vec = f0->Flatten();
  ASSERT_EQ(vec.size(), 1);
  AssertFieldEqual(vec[0], f0);

  auto f1 = field("f1", float64(), false /* nullable */);
  auto ff = field("nest", struct_({f0, f1}));
  vec = ff->Flatten();
  ASSERT_EQ(vec.size(), 2);
  auto expected0 = field("nest.f0", int32(), true /* nullable */, metadata);
  // nullable parent implies nullable flattened child
  auto expected1 = field("nest.f1", float64(), true /* nullable */);
  AssertFieldEqual(vec[0], expected0);
  AssertFieldEqual(vec[1], expected1);

  ff = field("nest", struct_({f0, f1}), false /* nullable */);
  vec = ff->Flatten();
  ASSERT_EQ(vec.size(), 2);
  expected0 = field("nest.f0", int32(), true /* nullable */, metadata);
  expected1 = field("nest.f1", float64(), false /* nullable */);
  AssertFieldEqual(vec[0], expected0);
  AssertFieldEqual(vec[1], expected1);
}

TEST(TestField, TestReplacement) {
  auto metadata = key_value_metadata({"foo", "bar"}, {"bizz", "buzz"});
  auto f0 = field("f0", int32(), true, metadata);
  auto fzero = f0->WithType(utf8());
  auto f1 = f0->WithName("f1");

  AssertFieldNotEqual(f0, fzero);
  AssertFieldNotCompatible(f0, fzero);
  AssertFieldNotEqual(fzero, f1);
  AssertFieldNotCompatible(fzero, f1);
  AssertFieldNotEqual(f1, f0);
  AssertFieldNotCompatible(f1, f0);

  ASSERT_EQ(fzero->name(), "f0");
  AssertTypeEqual(fzero->type(), utf8());
  ASSERT_TRUE(fzero->metadata()->Equals(*metadata));

  ASSERT_EQ(f1->name(), "f1");
  AssertTypeEqual(f1->type(), int32());
  ASSERT_TRUE(f1->metadata()->Equals(*metadata));
}

TEST(TestField, TestMerge) {
  auto metadata1 = key_value_metadata({"foo"}, {"v"});
  auto metadata2 = key_value_metadata({"bar"}, {"v"});
  {
    // different name.
    ASSERT_RAISES(Invalid, field("f0", int32())->MergeWith(field("f1", int32())));
  }
  {
    // Same type.
    auto f1 = field("f", int32())->WithMetadata(metadata1);
    auto f2 = field("f", int32())->WithMetadata(metadata2);
    std::shared_ptr<Field> result;
    ASSERT_OK_AND_ASSIGN(result, f1->MergeWith(f2));
    ASSERT_TRUE(result->Equals(f1));
    ASSERT_OK_AND_ASSIGN(result, f2->MergeWith(f1));
    ASSERT_TRUE(result->Equals(f2));
  }
  {
    // promote_nullability == false
    auto f = field("f", int32());
    auto null_field = field("f", null());
    Field::MergeOptions options;
    options.promote_nullability = false;
    ASSERT_RAISES(Invalid, f->MergeWith(null_field, options));
    ASSERT_RAISES(Invalid, null_field->MergeWith(f, options));

    // Also rejects fields with different nullability.
    ASSERT_RAISES(Invalid,
                  f->WithNullable(true)->MergeWith(f->WithNullable(false), options));
  }
  {
    // promote_nullability == true; merge with a null field.
    Field::MergeOptions options;
    options.promote_nullability = true;
    auto f = field("f", int32())->WithNullable(false)->WithMetadata(metadata1);
    auto null_field = field("f", null())->WithMetadata(metadata2);

    std::shared_ptr<Field> result;
    ASSERT_OK_AND_ASSIGN(result, f->MergeWith(null_field, options));
    ASSERT_TRUE(result->Equals(f->WithNullable(true)->WithMetadata(metadata1)));
    ASSERT_OK_AND_ASSIGN(result, null_field->MergeWith(f, options));
    ASSERT_TRUE(result->Equals(f->WithNullable(true)->WithMetadata(metadata2)));
  }
  {
    // promote_nullability == true; merge a nullable field and a in-nullable field.
    Field::MergeOptions options;
    options.promote_nullability = true;
    auto f1 = field("f", int32())->WithNullable(false);
    auto f2 = field("f", int32())->WithNullable(true);
    std::shared_ptr<Field> result;
    ASSERT_OK_AND_ASSIGN(result, f1->MergeWith(f2, options));
    ASSERT_TRUE(result->Equals(f1->WithNullable(true)));
    ASSERT_OK_AND_ASSIGN(result, f2->MergeWith(f1, options));
    ASSERT_TRUE(result->Equals(f2));
  }
}

struct FieldPathTestCase {
  struct OutputValues {
    explicit OutputValues(std::vector<int> indices = {})
        : path(FieldPath(std::move(indices))) {}

    template <typename T>
    const auto& Get() const {
      if constexpr (std::is_same_v<T, Field>) {
        return field;
      } else if constexpr (std::is_same_v<T, Array>) {
        return array;
      } else if constexpr (std::is_same_v<T, ArrayData>) {
        return array->data();
      } else if constexpr (std::is_same_v<T, ChunkedArray>) {
        return chunked_array;
      }
    }

    FieldPath path;
    std::shared_ptr<Field> field;
    std::shared_ptr<Array> array;
    std::shared_ptr<ChunkedArray> chunked_array;
  };

  static constexpr int kNumColumns = 2;
  static constexpr int kNumRows = 100;
  static constexpr int kRandomSeed = 0xbeef;

  // Input for the FieldPath::Get functions in multiple forms
  std::shared_ptr<Schema> schema;
  std::shared_ptr<DataType> type;
  std::shared_ptr<Array> array;
  std::shared_ptr<RecordBatch> record_batch;
  std::shared_ptr<ChunkedArray> chunked_array;
  std::shared_ptr<Table> table;

  template <typename T>
  const auto& GetInput() const {
    if constexpr (std::is_same_v<T, Schema>) {
      return schema;
    } else if constexpr (std::is_same_v<T, DataType>) {
      return type;
    } else if constexpr (std::is_same_v<T, Array>) {
      return array;
    } else if constexpr (std::is_same_v<T, ArrayData>) {
      return array->data();
    } else if constexpr (std::is_same_v<T, RecordBatch>) {
      return record_batch;
    } else if constexpr (std::is_same_v<T, ChunkedArray>) {
      return chunked_array;
    } else if constexpr (std::is_same_v<T, Table>) {
      return table;
    }
  }

  // Number of chunks for each column in the input Table
  const std::array<int, kNumColumns> num_column_chunks = {15, 20};
  // Number of chunks in the input ChunkedArray
  const int num_chunks = 15;

  // Expected outputs for each child;
  OutputValues v0{{0}}, v1{{1}};
  OutputValues v1_0{{1, 0}}, v1_1{{1, 1}};
  OutputValues v1_1_0{{1, 1, 0}}, v1_1_1{{1, 1, 1}};
  // Expected outputs for nested children with null flattening applied
  OutputValues v1_0_flat{{1, 0}}, v1_1_flat{{1, 1}};
  OutputValues v1_1_0_flat{{1, 1, 0}}, v1_1_1_flat{{1, 1, 1}};

  static const FieldPathTestCase* Instance() {
    static const auto maybe_instance = Make();
    return &maybe_instance.ValueOrDie();
  }

  static Result<FieldPathTestCase> Make() {
    // Generate test input based on a single schema. First by creating a StructArray,
    // then deriving the other input types (ChunkedArray, RecordBatch, Table, etc) from
    // it. We also compute the expected outputs for each child individually (for each
    // output type).
    FieldPathTestCase out;
    random::RandomArrayGenerator gen(kRandomSeed);

    // Define child fields and input schema
    out.v1_1_1.field = field("b", boolean());
    out.v1_1_0.field = field("f", float32());
    out.v1_1.field = field("s1", struct_({out.v1_1_0.field, out.v1_1_1.field}));
    out.v1_0.field = field("i", int32());
    out.v1.field = field("s0", struct_({out.v1_0.field, out.v1_1.field}));
    out.v0.field = field("u", utf8());
    out.schema = arrow::schema({out.v0.field, out.v1.field});
    out.type = struct_(out.schema->fields());

    // Create null bitmaps for the struct fields independent of its childrens'
    // bitmaps. For FieldPath::GetFlattened, parent/child bitmaps should be combined
    // - for FieldPath::Get, higher-level nulls are ignored.
    auto bitmap1_1 = gen.NullBitmap(kNumRows, 0.15);
    auto bitmap1 = gen.NullBitmap(kNumRows, 0.30);

    // Generate raw leaf arrays
    out.v1_1_1.array = gen.ArrayOf(out.v1_1_1.field->type(), kNumRows);
    out.v1_1_0.array = gen.ArrayOf(out.v1_1_0.field->type(), kNumRows);
    out.v1_0.array = gen.ArrayOf(out.v1_0.field->type(), kNumRows);
    out.v0.array = gen.ArrayOf(out.v0.field->type(), kNumRows);
    // Make struct fields from leaf arrays (we use the custom bitmaps here)
    ARROW_ASSIGN_OR_RAISE(
        out.v1_1.array,
        StructArray::Make({out.v1_1_0.array, out.v1_1_1.array},
                          {out.v1_1_0.field, out.v1_1_1.field}, bitmap1_1));
    ARROW_ASSIGN_OR_RAISE(out.v1.array,
                          StructArray::Make({out.v1_0.array, out.v1_1.array},
                                            {out.v1_0.field, out.v1_1.field}, bitmap1));

    // Not used to create the test input, but pre-compute flattened versions of nested
    // arrays for comparisons in the GetFlattened tests.
    ARROW_ASSIGN_OR_RAISE(
        out.v1_0_flat.array,
        checked_pointer_cast<StructArray>(out.v1.array)->GetFlattenedField(0));
    ARROW_ASSIGN_OR_RAISE(
        out.v1_1_flat.array,
        checked_pointer_cast<StructArray>(out.v1.array)->GetFlattenedField(1));
    ARROW_ASSIGN_OR_RAISE(
        out.v1_1_0_flat.array,
        checked_pointer_cast<StructArray>(out.v1_1_flat.array)->GetFlattenedField(0));
    ARROW_ASSIGN_OR_RAISE(
        out.v1_1_1_flat.array,
        checked_pointer_cast<StructArray>(out.v1_1_flat.array)->GetFlattenedField(1));
    // Sanity check
    ARROW_CHECK(!out.v1_0_flat.array->Equals(out.v1_0.array));
    ARROW_CHECK(!out.v1_1_flat.array->Equals(out.v1_1.array));
    ARROW_CHECK(!out.v1_1_0_flat.array->Equals(out.v1_1_0.array));
    ARROW_CHECK(!out.v1_1_1_flat.array->Equals(out.v1_1_1.array));

    // Finalize the input Array
    ARROW_ASSIGN_OR_RAISE(out.array, StructArray::Make({out.v0.array, out.v1.array},
                                                       {out.v0.field, out.v1.field}));
    ARROW_RETURN_NOT_OK(out.array->ValidateFull());
    // Finalize the input RecordBatch
    ARROW_ASSIGN_OR_RAISE(out.record_batch, RecordBatch::FromStructArray(out.array));
    ARROW_RETURN_NOT_OK(out.record_batch->ValidateFull());
    // Finalize the input ChunkedArray
    out.chunked_array = SliceToChunkedArray(*out.array, out.num_chunks);
    ARROW_RETURN_NOT_OK(out.chunked_array->ValidateFull());

    // For each expected child array, create a chunked equivalent (we use a different
    // chunk layout for each top-level column to make the Table test more interesting)
    for (OutputValues* v :
         {&out.v0, &out.v1, &out.v1_0, &out.v1_1, &out.v1_1_0, &out.v1_1_1,
          &out.v1_0_flat, &out.v1_1_flat, &out.v1_1_0_flat, &out.v1_1_1_flat}) {
      v->chunked_array =
          SliceToChunkedArray(*v->array, out.num_column_chunks[v->path[0]]);
    }
    // Finalize the input Table
    out.table =
        Table::Make(out.schema, {out.v0.chunked_array, out.v1.chunked_array}, kNumRows);
    ARROW_RETURN_NOT_OK(out.table->ValidateFull());

    return std::move(out);
  }

  static std::shared_ptr<ChunkedArray> SliceToChunkedArray(const Array& array,
                                                           int num_chunks) {
    ARROW_CHECK(num_chunks > 0 && array.length() >= num_chunks);
    ArrayVector chunks;
    chunks.reserve(num_chunks);
    for (int64_t inc = array.length() / num_chunks, beg = 0,
                 end = inc + array.length() % num_chunks;
         end <= array.length(); beg = end, end += inc) {
      chunks.push_back(array.SliceSafe(beg, end - beg).ValueOrDie());
    }
    ARROW_CHECK_EQ(static_cast<int>(chunks.size()), num_chunks);
    return ChunkedArray::Make(std::move(chunks)).ValueOrDie();
  }
};

class FieldPathTestFixture : public ::testing::Test {
 public:
  FieldPathTestFixture() : case_(FieldPathTestCase::Instance()) {}

 protected:
  template <typename T>
  using OutputType = typename FieldRef::GetType<T>::element_type;

  template <bool Flattened, typename T>
  static auto DoGet(const T& root, const FieldPath& path, MemoryPool* pool = nullptr) {
    if constexpr (Flattened) {
      return path.GetFlattened(root, pool);
    } else {
      return path.Get(root);
    }
  }

  template <typename I>
  void AssertOutputsEqual(const std::shared_ptr<Field>& expected,
                          const std::shared_ptr<Field>& actual) const {
    AssertFieldEqual(*expected, *actual);
  }
  template <typename I>
  void AssertOutputsEqual(const std::shared_ptr<Array>& expected,
                          const std::shared_ptr<Array>& actual) const {
    AssertArraysEqual(*expected, *actual);
  }
  template <typename I>
  void AssertOutputsEqual(const std::shared_ptr<ChunkedArray>& expected,
                          const std::shared_ptr<ChunkedArray>& actual) const {
    // We only do this dance due to the way the test inputs/outputs are generated.
    // Basically, the "expected" output ChunkedArrays don't have an equal num_chunks since
    // they're reused to create the input Table (which has a distinct chunking per
    // column). However, if the input was the ChunkedArray, the returned outputs should
    // always have the same num_chunks as the input.
    if constexpr (std::is_same_v<I, ChunkedArray>) {
      EXPECT_EQ(case_->chunked_array->num_chunks(), actual->num_chunks());
    } else {
      EXPECT_EQ(expected->num_chunks(), actual->num_chunks());
    }
    AssertChunkedEquivalent(*expected, *actual);
  }

  const FieldPathTestCase* case_;
};

class TestFieldPath : public FieldPathTestFixture {
 protected:
  template <typename I, bool Flattened = false>
  void TestGetWithInvalidIndex() const {
    const auto& input = case_->GetInput<I>();
    for (const auto& path :
         {FieldPath({2, 1, 0}), FieldPath({1, 2, 0}), FieldPath{1, 1, 2}}) {
      EXPECT_RAISES_WITH_MESSAGE_THAT(IndexError,
                                      ::testing::HasSubstr("index out of range"),
                                      DoGet<Flattened>(*input, path));
    }
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("empty indices cannot be traversed"),
        DoGet<Flattened>(*input, FieldPath()));
  }

  template <typename I, typename O = OutputType<I>>
  void TestIndexErrorMessage() const {
    auto result = FieldPath({1, 1, 2}).Get(*case_->GetInput<I>());
    std::string substr = "index out of range. indices=[ 1 1 >2< ] ";
    if constexpr (std::is_same_v<O, Field>) {
      substr += "fields: { f: float, b: bool, }";
    } else {
      substr += "column types: { float, bool, }";
    }
    EXPECT_RAISES_WITH_MESSAGE_THAT(IndexError, ::testing::HasSubstr(substr), result);
  }

  template <typename I, bool Flattened = false>
  void TestGetWithNonStructArray() const {
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        NotImplemented, ::testing::HasSubstr("Get child data of non-struct array"),
        DoGet<Flattened>(*case_->v1_1_0.Get<I>(), FieldPath({1, 1, 0})));
  }

  template <typename I, typename O = OutputType<I>>
  void TestGet() const {
    const auto& input = case_->GetInput<I>();
    ASSERT_OK_AND_ASSIGN(auto v0, FieldPath({0}).Get(*input));
    ASSERT_OK_AND_ASSIGN(auto v1, FieldPath({1}).Get(*input));
    ASSERT_OK_AND_ASSIGN(auto v1_0, FieldPath({1, 0}).Get(*input));
    ASSERT_OK_AND_ASSIGN(auto v1_1, FieldPath({1, 1}).Get(*input));
    ASSERT_OK_AND_ASSIGN(auto v1_1_0, FieldPath({1, 1, 0}).Get(*input));
    ASSERT_OK_AND_ASSIGN(auto v1_1_1, FieldPath({1, 1, 1}).Get(*input));

    AssertOutputsEqual<I>(case_->v0.Get<O>(), v0);
    AssertOutputsEqual<I>(case_->v1.Get<O>(), v1);
    AssertOutputsEqual<I>(case_->v1_0.Get<O>(), v1_0);
    AssertOutputsEqual<I>(case_->v1_1.Get<O>(), v1_1);
    AssertOutputsEqual<I>(case_->v1_1_0.Get<O>(), v1_1_0);
    AssertOutputsEqual<I>(case_->v1_1_1.Get<O>(), v1_1_1);
  }

  template <typename I, typename O = OutputType<I>>
  void TestGetFlattened() const {
    const auto& input = case_->GetInput<I>();
    ASSERT_OK_AND_ASSIGN(auto v0, FieldPath({0}).GetFlattened(*input));
    ASSERT_OK_AND_ASSIGN(auto v1, FieldPath({1}).GetFlattened(*input));
    ASSERT_OK_AND_ASSIGN(auto v1_0, FieldPath({1, 0}).GetFlattened(*input));
    ASSERT_OK_AND_ASSIGN(auto v1_1, FieldPath({1, 1}).GetFlattened(*input));
    ASSERT_OK_AND_ASSIGN(auto v1_1_0, FieldPath({1, 1, 0}).GetFlattened(*input));
    ASSERT_OK_AND_ASSIGN(auto v1_1_1, FieldPath({1, 1, 1}).GetFlattened(*input));

    AssertOutputsEqual<I>(case_->v0.Get<O>(), v0);
    AssertOutputsEqual<I>(case_->v1.Get<O>(), v1);
    AssertOutputsEqual<I>(case_->v1_0_flat.Get<O>(), v1_0);
    AssertOutputsEqual<I>(case_->v1_1_flat.Get<O>(), v1_1);
    AssertOutputsEqual<I>(case_->v1_1_0_flat.Get<O>(), v1_1_0);
    AssertOutputsEqual<I>(case_->v1_1_1_flat.Get<O>(), v1_1_1);
  }
};

TEST_F(TestFieldPath, GetWithInvalidIndex) {
  TestGetWithInvalidIndex<Schema>();
  TestGetWithInvalidIndex<DataType>();
  TestGetWithInvalidIndex<Array>();
  TestGetWithInvalidIndex<ArrayData>();
  TestGetWithInvalidIndex<ChunkedArray>();
  TestGetWithInvalidIndex<RecordBatch>();
  TestGetWithInvalidIndex<Table>();
  // With flattening
  TestGetWithInvalidIndex<Array, true>();
  TestGetWithInvalidIndex<ArrayData, true>();
  TestGetWithInvalidIndex<ChunkedArray, true>();
  TestGetWithInvalidIndex<RecordBatch, true>();
  TestGetWithInvalidIndex<Table, true>();
}

TEST_F(TestFieldPath, IndexErrorMessage) {
  TestIndexErrorMessage<Schema>();
  TestIndexErrorMessage<DataType>();
  TestIndexErrorMessage<Array>();
  TestIndexErrorMessage<ArrayData>();
  TestIndexErrorMessage<ChunkedArray>();
  TestIndexErrorMessage<RecordBatch>();
  TestIndexErrorMessage<Table>();
}

TEST_F(TestFieldPath, GetWithNonStructArray) {
  TestGetWithNonStructArray<Array>();
  TestGetWithNonStructArray<ArrayData>();
  TestGetWithNonStructArray<ChunkedArray>();
  // With flattening
  TestGetWithNonStructArray<Array, true>();
  TestGetWithNonStructArray<ArrayData, true>();
  TestGetWithNonStructArray<ChunkedArray, true>();
}

TEST_F(TestFieldPath, GetFromSchema) { TestGet<Schema>(); }
TEST_F(TestFieldPath, GetFromDataType) { TestGet<DataType>(); }

TEST_F(TestFieldPath, GetFromArray) { TestGet<Array>(); }
TEST_F(TestFieldPath, GetFromChunkedArray) { TestGet<ChunkedArray>(); }
TEST_F(TestFieldPath, GetFromRecordBatch) { TestGet<RecordBatch>(); }
TEST_F(TestFieldPath, GetFromTable) { TestGet<Table>(); }

TEST_F(TestFieldPath, GetFlattenedFromArray) { TestGetFlattened<Array>(); }
TEST_F(TestFieldPath, GetFlattenedFromChunkedArray) { TestGetFlattened<ChunkedArray>(); }
TEST_F(TestFieldPath, GetFlattenedFromRecordBatch) { TestGetFlattened<RecordBatch>(); }
TEST_F(TestFieldPath, GetFlattenedFromTable) { TestGetFlattened<Table>(); }

TEST_F(TestFieldPath, Basics) {
  auto f0 = field("alpha", int32());
  auto f1 = field("beta", int32());
  auto f2 = field("alpha", int32());
  auto f3 = field("beta", int32());
  Schema s({f0, f1, f2, f3});

  // retrieving a field with single-element FieldPath is equivalent to Schema::field
  for (int index = 0; index < s.num_fields(); ++index) {
    ASSERT_OK_AND_EQ(s.field(index), FieldPath({index}).Get(s));
  }
}

TEST_F(TestFieldPath, GetFromEmptyChunked) {
  FieldVector fields = {
      field("i", int32()),
      field("s", struct_({field("b", boolean()), field("f", float32())}))};
  std::shared_ptr<ChunkedArray> child;

  // Empty ChunkedArray with no chunks
  ChunkedArray chunked_array({}, struct_(fields));
  ASSERT_OK(chunked_array.ValidateFull());
  ASSERT_EQ(chunked_array.num_chunks(), 0);
  ASSERT_OK_AND_ASSIGN(child, FieldPath({1, 1}).Get(chunked_array));
  AssertTypeEqual(float32(), child->type());
  ASSERT_EQ(child->length(), 0);

  // Empty Table with no column chunks
  ChunkedArrayVector table_columns;
  for (const auto& f : fields) {
    table_columns.push_back(std::make_shared<ChunkedArray>(ArrayVector{}, f->type()));
  }
  auto table = Table::Make(schema(fields), table_columns, 0);
  ASSERT_OK(table->ValidateFull());
  for (const auto& column : table->columns()) {
    ASSERT_EQ(column->num_chunks(), 0);
  }
  ASSERT_OK_AND_ASSIGN(child, FieldPath({1, 1}).Get(*table));
  AssertTypeEqual(float32(), child->type());
  ASSERT_EQ(child->length(), 0);
}

TEST(TestFieldRef, Basics) {
  auto f0 = field("alpha", int32());
  auto f1 = field("beta", int32());
  auto f2 = field("alpha", int32());
  auto f3 = field("beta", int32());
  Schema s({f0, f1, f2, f3});

  // lookup by index returns Indices{index}
  for (int index = 0; index < s.num_fields(); ++index) {
    EXPECT_THAT(FieldRef(index).FindAll(s), ElementsAre(FieldPath{index}));
  }
  // out of range index results in a failure to match
  EXPECT_THAT(FieldRef(s.num_fields() * 2).FindAll(s), ElementsAre());

  // lookup by name returns the Indices of both matching fields
  EXPECT_THAT(FieldRef("alpha").FindAll(s), ElementsAre(FieldPath{0}, FieldPath{2}));
  EXPECT_THAT(FieldRef("beta").FindAll(s), ElementsAre(FieldPath{1}, FieldPath{3}));
}

TEST(TestFieldRef, FindAllForTable) {
  constexpr int kNumRows = 100;
  auto f0 = field("alpha", int32());
  auto f1 = field("beta", int32());
  auto f2 = field("alpha", int32());
  auto f3 = field("beta", int32());
  auto schema = arrow::schema({f0, f1, f2, f3});

  arrow::random::RandomArrayGenerator gen_{42};
  auto a0 = gen_.ArrayOf(int32(), kNumRows);
  auto a1 = gen_.ArrayOf(int32(), kNumRows);
  auto a2 = gen_.ArrayOf(int32(), kNumRows);
  auto a3 = gen_.ArrayOf(int32(), kNumRows);

  auto table_ptr = Table::Make(schema, {a0, a1, a2, a3});
  ASSERT_OK(table_ptr->ValidateFull());

  // lookup by index returns Indices{index}
  auto schema_num_fields = table_ptr->schema()->num_fields();
  for (int index = 0; index < schema_num_fields; ++index) {
    EXPECT_THAT(FieldRef(index).FindAll(*table_ptr), ElementsAre(FieldPath{index}));
  }
  // out of range index results in a failure to match
  EXPECT_THAT(FieldRef(schema_num_fields * 2).FindAll(*table_ptr), ElementsAre());

  //// lookup by name returns the Indices of both matching fields
  EXPECT_THAT(FieldRef("alpha").FindAll(*table_ptr),
              ElementsAre(FieldPath{0}, FieldPath{2}));
  EXPECT_THAT(FieldRef("beta").FindAll(*table_ptr),
              ElementsAre(FieldPath{1}, FieldPath{3}));
}

TEST(TestFieldRef, FindAllForRecordBatch) {
  constexpr int kNumRows = 100;
  auto f0 = field("alpha", int32());
  auto f1 = field("beta", int32());
  auto f2 = field("alpha", int32());
  auto f3 = field("beta", int32());
  auto schema = arrow::schema({f0, f1, f2, f3});

  arrow::random::RandomArrayGenerator gen_{42};
  auto a0 = gen_.ArrayOf(int32(), kNumRows);
  auto a1 = gen_.ArrayOf(int32(), kNumRows);
  auto a2 = gen_.ArrayOf(int32(), kNumRows);
  auto a3 = gen_.ArrayOf(int32(), kNumRows);

  auto record_batch_ptr = RecordBatch::Make(schema, kNumRows, {a0, a1, a2, a3});
  ASSERT_OK(record_batch_ptr->ValidateFull());

  // lookup by index returns Indices{index}
  auto schema_num_fields = record_batch_ptr->schema()->num_fields();
  for (int index = 0; index < schema_num_fields; ++index) {
    EXPECT_THAT(FieldRef(index).FindAll(*record_batch_ptr),
                ElementsAre(FieldPath{index}));
  }
  // out of range index results in a failure to match
  EXPECT_THAT(FieldRef(schema_num_fields * 2).FindAll(*record_batch_ptr), ElementsAre());

  //// lookup by name returns the Indices of both matching fields
  EXPECT_THAT(FieldRef("alpha").FindAll(*record_batch_ptr),
              ElementsAre(FieldPath{0}, FieldPath{2}));
  EXPECT_THAT(FieldRef("beta").FindAll(*record_batch_ptr),
              ElementsAre(FieldPath{1}, FieldPath{3}));
}

TEST(TestFieldRef, FromDotPath) {
  ASSERT_OK_AND_EQ(FieldRef("alpha"), FieldRef::FromDotPath(R"(.alpha)"));

  ASSERT_OK_AND_EQ(FieldRef("", ""), FieldRef::FromDotPath(R"(..)"));

  ASSERT_OK_AND_EQ(FieldRef(2), FieldRef::FromDotPath(R"([2])"));

  ASSERT_OK_AND_EQ(FieldRef("beta", 3), FieldRef::FromDotPath(R"(.beta[3])"));

  ASSERT_OK_AND_EQ(FieldRef(5, "gamma", "delta", 7),
                   FieldRef::FromDotPath(R"([5].gamma.delta[7])"));

  ASSERT_OK_AND_EQ(FieldRef("hello world"), FieldRef::FromDotPath(R"(.hello world)"));

  ASSERT_OK_AND_EQ(FieldRef(R"([y]\tho.\)"), FieldRef::FromDotPath(R"(.\[y\]\\tho\.\)"));

  ASSERT_OK_AND_EQ(FieldRef(), FieldRef::FromDotPath(R"()"));

  ASSERT_RAISES(Invalid, FieldRef::FromDotPath(R"(alpha)"));
  ASSERT_RAISES(Invalid, FieldRef::FromDotPath(R"([134234)"));
  ASSERT_RAISES(Invalid, FieldRef::FromDotPath(R"([1stuf])"));
}

TEST(TestFieldRef, DotPathRoundTrip) {
  auto check_roundtrip = [](const FieldRef& ref) {
    auto dot_path = ref.ToDotPath();
    ASSERT_OK_AND_EQ(ref, FieldRef::FromDotPath(dot_path));
  };

  check_roundtrip(FieldRef());
  check_roundtrip(FieldRef("foo"));
  check_roundtrip(FieldRef("foo", 1, "bar", 2, 3));
  check_roundtrip(FieldRef(1, 2, 3));
  check_roundtrip(FieldRef("foo", 1, FieldRef("bar", 2, 3), FieldRef()));
}

TEST(TestFieldRef, Nested) {
  auto f0 = field("alpha", int32());
  auto f1_0 = field("alpha", int32());
  auto f1 = field("beta", struct_({f1_0}));
  auto f2_0 = field("alpha", int32());
  auto f2_1_0 = field("alpha", int32());
  auto f2_1_1 = field("alpha", int32());
  auto f2_1 = field("gamma", struct_({f2_1_0, f2_1_1}));
  auto f2 = field("beta", struct_({f2_0, f2_1}));
  Schema s({f0, f1, f2});

  EXPECT_THAT(FieldRef("beta", "alpha").FindAll(s),
              ElementsAre(FieldPath{1, 0}, FieldPath{2, 0}));
  EXPECT_THAT(FieldRef("beta", "gamma", "alpha").FindAll(s),
              ElementsAre(FieldPath{2, 1, 0}, FieldPath{2, 1, 1}));
}

TEST(TestFieldRef, Flatten) {
  FieldRef ref;

  auto assert_name = [](const FieldRef& ref, const std::string& expected) {
    ASSERT_TRUE(ref.IsName());
    ASSERT_EQ(*ref.name(), expected);
  };

  auto assert_path = [](const FieldRef& ref, const std::vector<int>& expected) {
    ASSERT_TRUE(ref.IsFieldPath());
    ASSERT_EQ(ref.field_path()->indices(), expected);
  };

  auto assert_nested = [](const FieldRef& ref, const std::vector<FieldRef>& expected) {
    ASSERT_TRUE(ref.IsNested());
    ASSERT_EQ(*ref.nested_refs(), expected);
  };

  assert_path(FieldRef(), {});
  assert_path(FieldRef(1, 2, 3), {1, 2, 3});
  // If all leaves are field paths, they are fully flattened
  assert_path(FieldRef(1, FieldRef(2, 3)), {1, 2, 3});
  assert_path(FieldRef(1, FieldRef(2, 3), FieldRef(), FieldRef(FieldRef(4), FieldRef(5))),
              {1, 2, 3, 4, 5});
  assert_path(FieldRef(FieldRef(), FieldRef(FieldRef(), FieldRef())), {});

  assert_name(FieldRef("foo"), "foo");

  // Nested empty field refs are optimized away
  assert_nested(FieldRef("foo", 1, FieldRef(), FieldRef(FieldRef(), "bar")),
                {FieldRef("foo"), FieldRef(1), FieldRef("bar")});
  // For now, subsequences of indices are not concatenated
  assert_nested(FieldRef("foo", FieldRef("bar"), FieldRef(1, 2), FieldRef(3)),
                {FieldRef("foo"), FieldRef("bar"), FieldRef(1, 2), FieldRef(3)});
}

using TestSchema = ::testing::Test;

TEST_F(TestSchema, Basics) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f1_optional = field("f1", uint8());

  auto f2 = field("f2", utf8());

  auto schema = ::arrow::schema({f0, f1, f2});

  ASSERT_EQ(3, schema->num_fields());
  AssertFieldEqual(*f0, *schema->field(0));
  AssertFieldEqual(*f1, *schema->field(1));
  AssertFieldEqual(*f2, *schema->field(2));

  auto schema2 = ::arrow::schema({f0, f1, f2});

  std::vector<std::shared_ptr<Field>> fields3 = {f0, f1_optional, f2};
  auto schema3 = std::make_shared<Schema>(fields3);
  AssertSchemaEqual(schema, schema2);
  AssertSchemaNotEqual(schema, schema3);
  ASSERT_EQ(*schema, *schema2);
  ASSERT_NE(*schema, *schema3);

  ASSERT_EQ(schema->fingerprint(), schema2->fingerprint());
  ASSERT_NE(schema->fingerprint(), schema3->fingerprint());

  auto schema4 = ::arrow::schema({f0}, Endianness::Little);
  auto schema5 = ::arrow::schema({f0}, Endianness::Little);
  auto schema6 = ::arrow::schema({f0}, Endianness::Big);
  auto schema7 = ::arrow::schema({f0});

  AssertSchemaEqual(schema4, schema5);
  AssertSchemaNotEqual(schema4, schema6);
#if ARROW_LITTLE_ENDIAN
  AssertSchemaEqual(schema4, schema7);
  AssertSchemaNotEqual(schema6, schema7);
#else
  AssertSchemaNotEqual(schema4, schema6);
  AssertSchemaEqual(schema6, schema7);
#endif

  ASSERT_EQ(schema4->fingerprint(), schema5->fingerprint());
  ASSERT_NE(schema4->fingerprint(), schema6->fingerprint());
#if ARROW_LITTLE_ENDIAN
  ASSERT_EQ(schema4->fingerprint(), schema7->fingerprint());
  ASSERT_NE(schema6->fingerprint(), schema7->fingerprint());
#else
  ASSERT_NE(schema4->fingerprint(), schema7->fingerprint());
  ASSERT_EQ(schema6->fingerprint(), schema7->fingerprint());
#endif
}

TEST_F(TestSchema, ToString) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  auto metadata = key_value_metadata({"foo"}, {"bar"});
  auto schema = ::arrow::schema({f0, f1, f2, f3}, metadata);

  std::string result = schema->ToString();
  std::string expected = R"(f0: int32
f1: uint8 not null
f2: string
f3: list<item: int16>)";

  ASSERT_EQ(expected, result);

  result = schema->ToString(/*print_metadata=*/true);
  std::string expected_with_metadata = expected + R"(
-- metadata --
foo: bar)";

  ASSERT_EQ(expected_with_metadata, result);

  // With swapped endianness
#if ARROW_LITTLE_ENDIAN
  schema = schema->WithEndianness(Endianness::Big);
  expected = R"(f0: int32
f1: uint8 not null
f2: string
f3: list<item: int16>
-- endianness: big --)";
#else
  schema = schema->WithEndianness(Endianness::Little);
  expected = R"(f0: int32
f1: uint8 not null
f2: string
f3: list<item: int16>
-- endianness: little --)";
#endif

  result = schema->ToString();
  ASSERT_EQ(expected, result);

  result = schema->ToString(/*print_metadata=*/true);
  expected_with_metadata = expected + R"(
-- metadata --
foo: bar)";

  ASSERT_EQ(expected_with_metadata, result);
}

TEST_F(TestSchema, GetFieldByName) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  auto schema = ::arrow::schema({f0, f1, f2, f3});

  std::shared_ptr<Field> result;

  result = schema->GetFieldByName("f1");
  AssertFieldEqual(f1, result);

  result = schema->GetFieldByName("f3");
  AssertFieldEqual(f3, result);

  result = schema->GetFieldByName("not-found");
  ASSERT_EQ(result, nullptr);
}

TEST_F(TestSchema, GetFieldIndex) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  auto schema = ::arrow::schema({f0, f1, f2, f3});

  ASSERT_EQ(0, schema->GetFieldIndex(f0->name()));
  ASSERT_EQ(1, schema->GetFieldIndex(f1->name()));
  ASSERT_EQ(2, schema->GetFieldIndex(f2->name()));
  ASSERT_EQ(3, schema->GetFieldIndex(f3->name()));
  ASSERT_EQ(-1, schema->GetFieldIndex("not-found"));
}

TEST_F(TestSchema, GetFieldDuplicates) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f1", list(int16()));

  auto schema = ::arrow::schema({f0, f1, f2, f3});

  ASSERT_EQ(0, schema->GetFieldIndex(f0->name()));
  ASSERT_EQ(-1, schema->GetFieldIndex(f1->name()));  // duplicate
  ASSERT_EQ(2, schema->GetFieldIndex(f2->name()));
  ASSERT_EQ(-1, schema->GetFieldIndex("not-found"));
  ASSERT_EQ(std::vector<int>{0}, schema->GetAllFieldIndices(f0->name()));
  ASSERT_EQ(std::vector<int>({1, 3}), schema->GetAllFieldIndices(f1->name()));

  ASSERT_TRUE(::arrow::schema({f0, f1, f2})->HasDistinctFieldNames());
  ASSERT_FALSE(schema->HasDistinctFieldNames());

  std::vector<std::shared_ptr<Field>> results;

  results = schema->GetAllFieldsByName(f0->name());
  ASSERT_EQ(results.size(), 1);
  AssertFieldEqual(results[0], f0);

  results = schema->GetAllFieldsByName(f1->name());
  ASSERT_EQ(results.size(), 2);
  if (results[0]->type()->id() == Type::UINT8) {
    AssertFieldEqual(results[0], f1);
    AssertFieldEqual(results[1], f3);
  } else {
    AssertFieldEqual(results[0], f3);
    AssertFieldEqual(results[1], f1);
  }

  results = schema->GetAllFieldsByName("not-found");
  ASSERT_EQ(results.size(), 0);
}

TEST_F(TestSchema, CanReferenceFieldsByNames) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f1", list(int16()));

  auto schema = ::arrow::schema({f0, f1, f2, f3});

  ASSERT_OK(schema->CanReferenceFieldsByNames({"f0", "f2"}));
  ASSERT_OK(schema->CanReferenceFieldsByNames({"f2", "f0"}));

  // Not found
  ASSERT_RAISES(Invalid, schema->CanReferenceFieldsByNames({"nope"}));
  ASSERT_RAISES(Invalid, schema->CanReferenceFieldsByNames({"f0", "nope"}));
  // Duplicates
  ASSERT_RAISES(Invalid, schema->CanReferenceFieldsByNames({"f1"}));
  ASSERT_RAISES(Invalid, schema->CanReferenceFieldsByNames({"f0", "f1"}));
  // Both
  ASSERT_RAISES(Invalid, schema->CanReferenceFieldsByNames({"f0", "f1", "nope"}));
}

TEST_F(TestSchema, TestMetadataConstruction) {
  auto metadata0 = key_value_metadata({{"foo", "bar"}, {"bizz", "buzz"}});
  auto metadata1 = key_value_metadata({{"foo", "baz"}});

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8(), true);
  auto f3 = field("f2", utf8(), true, metadata1->Copy());

  auto schema0 = ::arrow::schema({f0, f1, f2}, metadata0);
  auto schema1 = ::arrow::schema({f0, f1, f2}, metadata1);
  auto schema2 = ::arrow::schema({f0, f1, f2}, metadata0->Copy());
  auto schema3 = ::arrow::schema({f0, f1, f3}, metadata0->Copy());

  ASSERT_TRUE(metadata0->Equals(*schema0->metadata()));
  ASSERT_TRUE(metadata1->Equals(*schema1->metadata()));
  ASSERT_TRUE(metadata0->Equals(*schema2->metadata()));
  AssertSchemaEqual(schema0, schema2);

  AssertSchemaEqual(schema0, schema1);
  AssertSchemaNotEqual(schema0, schema1, /*check_metadata=*/true);

  AssertSchemaEqual(schema2, schema1);
  AssertSchemaNotEqual(schema2, schema1, /*check_metadata=*/true);

  // Field has different metatadata
  AssertSchemaEqual(schema2, schema3);
  AssertSchemaNotEqual(schema2, schema3, /*check_metadata=*/true);

  ASSERT_EQ(schema0->fingerprint(), schema1->fingerprint());
  ASSERT_EQ(schema0->fingerprint(), schema2->fingerprint());
  ASSERT_EQ(schema0->fingerprint(), schema3->fingerprint());
  ASSERT_NE(schema0->metadata_fingerprint(), schema1->metadata_fingerprint());
  ASSERT_EQ(schema0->metadata_fingerprint(), schema2->metadata_fingerprint());
  ASSERT_NE(schema0->metadata_fingerprint(), schema3->metadata_fingerprint());
}

TEST_F(TestSchema, TestNestedMetadataComparison) {
  auto item0 = field("item", int32(), true);
  auto item1 = field("item", int32(), true, key_value_metadata({{"foo", "baz"}}));

  Schema schema0({field("f", list(item0))});
  Schema schema1({field("f", list(item1))});

  ASSERT_EQ(schema0.fingerprint(), schema1.fingerprint());
  ASSERT_NE(schema0.metadata_fingerprint(), schema1.metadata_fingerprint());

  AssertSchemaEqual(schema0, schema1);
  AssertSchemaNotEqual(schema0, schema1, /* check_metadata = */ true);
}

TEST_F(TestSchema, TestDeeplyNestedMetadataComparison) {
  auto item0 = field("item", int32(), true);
  auto item1 = field("item", int32(), true, key_value_metadata({{"foo", "baz"}}));

  Schema schema0(
      {field("f", list(list(sparse_union({field("struct", struct_({item0}))}))))});
  Schema schema1(
      {field("f", list(list(sparse_union({field("struct", struct_({item1}))}))))});

  ASSERT_EQ(schema0.fingerprint(), schema1.fingerprint());
  ASSERT_NE(schema0.metadata_fingerprint(), schema1.metadata_fingerprint());

  AssertSchemaEqual(schema0, schema1);
  AssertSchemaNotEqual(schema0, schema1, /* check_metadata = */ true);
}

TEST_F(TestSchema, TestFieldsDifferOnlyInMetadata) {
  auto f0 = field("f", utf8(), true, nullptr);
  auto f1 = field("f", utf8(), true, key_value_metadata({{"foo", "baz"}}));

  Schema schema0({f0, f1});
  Schema schema1({f1, f0});

  AssertSchemaEqual(schema0, schema1);
  AssertSchemaNotEqual(schema0, schema1, /* check_metadata = */ true);

  ASSERT_EQ(schema0.fingerprint(), schema1.fingerprint());
  ASSERT_NE(schema0.metadata_fingerprint(), schema1.metadata_fingerprint());
}

TEST_F(TestSchema, TestEmptyMetadata) {
  // Empty metadata should be equivalent to no metadata at all
  auto f1 = field("f1", int32());
  auto metadata1 = key_value_metadata({});
  auto metadata2 = key_value_metadata({"foo"}, {"foo value"});

  auto schema1 = ::arrow::schema({f1});
  auto schema2 = ::arrow::schema({f1}, metadata1);
  auto schema3 = ::arrow::schema({f1}, metadata2);

  AssertSchemaEqual(schema1, schema2);
  AssertSchemaNotEqual(schema1, schema3, /*check_metadata=*/true);

  ASSERT_EQ(schema1->fingerprint(), schema2->fingerprint());
  ASSERT_EQ(schema1->fingerprint(), schema3->fingerprint());
  ASSERT_EQ(schema1->metadata_fingerprint(), schema2->metadata_fingerprint());
  ASSERT_NE(schema1->metadata_fingerprint(), schema3->metadata_fingerprint());
}

TEST_F(TestSchema, TestWithMetadata) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto metadata = key_value_metadata({"foo", "bar"}, {"bizz", "buzz"});
  auto schema = std::make_shared<Schema>(fields);
  std::shared_ptr<Schema> new_schema = schema->WithMetadata(metadata);
  ASSERT_TRUE(metadata->Equals(*new_schema->metadata()));

  // Not copied
  ASSERT_TRUE(metadata.get() == new_schema->metadata().get());
}

TEST_F(TestSchema, TestRemoveMetadata) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = std::make_shared<Schema>(fields);
  std::shared_ptr<Schema> new_schema = schema->RemoveMetadata();
  ASSERT_TRUE(new_schema->metadata() == nullptr);
}

void AssertSchemaBuilderYield(const SchemaBuilder& builder,
                              const std::shared_ptr<Schema>& expected) {
  ASSERT_OK_AND_ASSIGN(auto schema, builder.Finish());
  AssertSchemaEqual(schema, expected);
}

TEST(TestSchemaBuilder, DefaultBehavior) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());

  SchemaBuilder builder;
  ASSERT_OK(builder.AddField(f0));
  ASSERT_OK(builder.AddField(f1));
  ASSERT_OK(builder.AddField(f2));
  AssertSchemaBuilderYield(builder, schema({f0, f1, f2}));

  builder.Reset();
  ASSERT_OK(builder.AddFields({f0, f1, f2->WithNullable(false)}));
  AssertSchemaBuilderYield(builder, schema({f0, f1, f2->WithNullable(false)}));

  builder.Reset();
  ASSERT_OK(builder.AddSchema(schema({f2, f0})));
  AssertSchemaBuilderYield(builder, schema({f2, f0}));

  builder.Reset();
  ASSERT_OK(builder.AddSchemas({schema({f1, f2}), schema({f2, f0})}));
  AssertSchemaBuilderYield(builder, schema({f1, f2, f2, f0}));
}

TEST(TestSchemaBuilder, WithMetadata) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto metadata = key_value_metadata({{"foo", "bar"}});

  SchemaBuilder builder;
  ASSERT_OK(builder.AddMetadata(*metadata));
  ASSERT_OK_AND_ASSIGN(auto schema, builder.Finish());
  AssertSchemaEqual(schema, ::arrow::schema({})->WithMetadata(metadata));

  ASSERT_OK(builder.AddField(f0));
  ASSERT_OK_AND_ASSIGN(schema, builder.Finish());
  AssertSchemaEqual(schema, ::arrow::schema({f0})->WithMetadata(metadata));

  SchemaBuilder other_builder{::arrow::schema({})->WithMetadata(metadata)};
  ASSERT_OK(other_builder.AddField(f1));
  ASSERT_OK_AND_ASSIGN(schema, other_builder.Finish());
  AssertSchemaEqual(schema, ::arrow::schema({f1})->WithMetadata(metadata));

  other_builder.Reset();
  ASSERT_OK(other_builder.AddField(f1->WithMetadata(metadata)));
  ASSERT_OK_AND_ASSIGN(schema, other_builder.Finish());
  AssertSchemaEqual(schema, ::arrow::schema({f1->WithMetadata(metadata)}));
}

TEST(TestSchemaBuilder, IncrementalConstruction) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());

  SchemaBuilder builder;
  std::shared_ptr<Schema> actual;

  ASSERT_OK_AND_ASSIGN(actual, builder.Finish());
  AssertSchemaEqual(actual, ::arrow::schema({}));

  ASSERT_OK(builder.AddField(f0));
  ASSERT_OK_AND_ASSIGN(actual, builder.Finish());
  AssertSchemaEqual(actual, ::arrow::schema({f0}));

  ASSERT_OK(builder.AddField(f1));
  ASSERT_OK_AND_ASSIGN(actual, builder.Finish());
  AssertSchemaEqual(actual, ::arrow::schema({f0, f1}));

  ASSERT_OK(builder.AddField(f2));
  AssertSchemaBuilderYield(builder, schema({f0, f1, f2}));
}

TEST(TestSchemaBuilder, PolicyIgnore) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f0_req = field("f0", utf8(), false);

  SchemaBuilder builder{SchemaBuilder::CONFLICT_IGNORE};

  ASSERT_OK(builder.AddFields({f0, f1}));
  AssertSchemaBuilderYield(builder, schema({f0, f1}));

  ASSERT_OK(builder.AddField(f0_req));
  AssertSchemaBuilderYield(builder, schema({f0, f1}));

  ASSERT_OK(builder.AddField(f0));
  AssertSchemaBuilderYield(builder, schema({f0, f1}));
}

TEST(TestSchemaBuilder, PolicyReplace) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f0_req = field("f0", utf8(), false);

  SchemaBuilder builder{SchemaBuilder::CONFLICT_REPLACE};

  ASSERT_OK(builder.AddFields({f0, f1}));
  AssertSchemaBuilderYield(builder, schema({f0, f1}));

  ASSERT_OK(builder.AddField(f0_req));
  AssertSchemaBuilderYield(builder, schema({f0_req, f1}));

  ASSERT_OK(builder.AddField(f0));
  AssertSchemaBuilderYield(builder, schema({f0, f1}));
}

TEST(TestSchemaBuilder, PolicyMerge) {
  auto f0 = field("f0", int32(), true);
  auto f1 = field("f1", uint8());
  // Same as f0, but not required.
  auto f0_opt = field("f0", int32());
  // Another type, can't merge
  auto f0_other = field("f0", utf8(), false);

  SchemaBuilder builder{SchemaBuilder::CONFLICT_MERGE};

  ASSERT_OK(builder.AddFields({f0, f1}));
  AssertSchemaBuilderYield(builder, schema({f0, f1}));

  ASSERT_OK(builder.AddField(f0_opt));
  AssertSchemaBuilderYield(builder, schema({f0_opt, f1}));

  // Unsupported merge with a different type
  ASSERT_RAISES(Invalid, builder.AddField(f0_other));
  // Builder should still contain state
  AssertSchemaBuilderYield(builder, schema({f0, f1}));

  builder.Reset();
  // Create a schema with duplicate fields
  builder.SetPolicy(SchemaBuilder::CONFLICT_APPEND);
  ASSERT_OK(builder.AddFields({f0, f0}));

  builder.SetPolicy(SchemaBuilder::CONFLICT_MERGE);
  // Even if the field is compatible, it can't know with which field to merge.
  ASSERT_RAISES(Invalid, builder.AddField(f0_opt));

  AssertSchemaBuilderYield(builder, schema({f0, f0}));
}

TEST(TestSchemaBuilder, PolicyError) {
  auto f0 = field("f0", int32(), true);
  auto f1 = field("f1", uint8());
  // Same as f0, but not required.
  auto f0_opt = field("f0", int32());
  // Another type, can't merge
  auto f0_other = field("f0", utf8(), false);

  SchemaBuilder builder{SchemaBuilder::CONFLICT_ERROR};

  ASSERT_OK(builder.AddFields({f0, f1}));
  AssertSchemaBuilderYield(builder, schema({f0, f1}));

  ASSERT_RAISES(Invalid, builder.AddField(f0));
  ASSERT_RAISES(Invalid, builder.AddField(f0_opt));
  ASSERT_RAISES(Invalid, builder.AddField(f0_other));
  AssertSchemaBuilderYield(builder, schema({f0, f1}));
}

TEST(TestSchemaBuilder, Merge) {
  auto f0 = field("f0", int32(), true);
  auto f1 = field("f1", uint8());
  // Same as f0, but not required.
  auto f0_opt = field("f0", int32());
  // Another type, can't merge
  auto f0_other = field("f0", utf8(), false);

  auto s1 = schema({f0, f1});
  auto s2 = schema({f1, f0});
  auto s3 = schema({f0_opt});
  auto broken = schema({f0_other});

  ASSERT_OK_AND_ASSIGN(auto schema, SchemaBuilder::Merge({s1, s2, s3}));
  ASSERT_OK(SchemaBuilder::AreCompatible({s1, s2, s3}));
  AssertSchemaEqual(schema, ::arrow::schema({f0_opt, f1}));

  ASSERT_OK_AND_ASSIGN(schema, SchemaBuilder::Merge({s2, s3, s1}));
  AssertSchemaEqual(schema, ::arrow::schema({f1, f0_opt}));

  ASSERT_RAISES(Invalid, SchemaBuilder::Merge({s3, broken}));
  ASSERT_RAISES(Invalid, SchemaBuilder::AreCompatible({s3, broken}));
}

class TestUnifySchemas : public TestSchema {
 protected:
  void AssertSchemaEqualsUnorderedFields(const Schema& lhs, const Schema& rhs) {
    if (lhs.metadata()) {
      ASSERT_NE(nullptr, rhs.metadata());
      ASSERT_TRUE(lhs.metadata()->Equals(*rhs.metadata()));
    } else {
      ASSERT_EQ(nullptr, rhs.metadata());
    }
    ASSERT_EQ(lhs.num_fields(), rhs.num_fields());
    for (int i = 0; i < lhs.num_fields(); ++i) {
      auto lhs_field = lhs.field(i);
      auto rhs_field = rhs.GetFieldByName(lhs_field->name());
      ASSERT_NE(nullptr, rhs_field);
      ASSERT_TRUE(lhs_field->Equals(rhs_field, true))
          << lhs_field->ToString() << " vs " << rhs_field->ToString();
    }
  }
};

TEST_F(TestUnifySchemas, EmptyInput) { ASSERT_RAISES(Invalid, UnifySchemas({})); }

TEST_F(TestUnifySchemas, IdenticalSchemas) {
  auto int32_field = field("int32_field", int32());
  auto uint8_field = field("uint8_field", uint8(), false);
  auto utf8_field = field("utf8_field", utf8());
  std::vector<std::string> keys{"foo"};
  std::vector<std::string> vals{"bar"};
  auto metadata = std::make_shared<KeyValueMetadata>(keys, vals);

  auto schema1 = schema({int32_field, uint8_field, utf8_field});
  auto schema2 = schema({int32_field, uint8_field, utf8_field->WithMetadata(metadata)})
                     ->WithMetadata(metadata);

  ASSERT_OK_AND_ASSIGN(auto result, UnifySchemas({schema1, schema2}));
  // Using Schema::Equals to make sure the ordering of fields is not changed.
  ASSERT_TRUE(result->Equals(*schema1, /*check_metadata=*/true));

  ASSERT_OK_AND_ASSIGN(result, UnifySchemas({schema2, schema1}));
  // Using Schema::Equals to make sure the ordering of fields is not changed.
  ASSERT_TRUE(result->Equals(*schema2, /*check_metadata=*/true));
}

TEST_F(TestUnifySchemas, FieldOrderingSameAsTheFirstSchema) {
  auto int32_field = field("int32_field", int32());
  auto uint8_field = field("uint8_field", uint8(), false);
  auto utf8_field = field("utf8_field", utf8());
  auto binary_field = field("binary_field", binary());

  auto schema1 = schema({int32_field, uint8_field, utf8_field});
  // schema2 only differs from schema1 in field ordering.
  auto schema2 = schema({uint8_field, int32_field, utf8_field});
  auto schema3 = schema({binary_field});

  ASSERT_OK_AND_ASSIGN(auto result, UnifySchemas({schema1, schema2, schema3}));

  ASSERT_EQ(4, result->num_fields());
  ASSERT_TRUE(int32_field->Equals(result->field(0)));
  ASSERT_TRUE(uint8_field->Equals(result->field(1)));
  ASSERT_TRUE(utf8_field->Equals(result->field(2)));
  ASSERT_TRUE(binary_field->Equals(result->field(3)));
}

TEST_F(TestUnifySchemas, MissingField) {
  auto int32_field = field("int32_field", int32());
  auto uint8_field = field("uint8_field", uint8(), false);
  auto utf8_field = field("utf8_field", utf8());
  auto metadata1 = key_value_metadata({"foo"}, {"bar"});
  auto metadata2 = key_value_metadata({"q"}, {"42"});

  auto schema1 = schema({int32_field, uint8_field})->WithMetadata(metadata1);
  auto schema2 = schema({uint8_field, utf8_field->WithMetadata(metadata2)});
  auto schema3 = schema({int32_field->WithMetadata(metadata1), uint8_field, utf8_field});

  ASSERT_OK_AND_ASSIGN(auto result, UnifySchemas({schema1, schema2}));
  AssertSchemaEqualsUnorderedFields(
      *result, *schema({int32_field, uint8_field, utf8_field->WithMetadata(metadata2)})
                    ->WithMetadata(metadata1));
}

TEST_F(TestUnifySchemas, PromoteNullTypeField) {
  auto metadata = key_value_metadata({"foo"}, {"bar"});
  auto null_field = field("f", null());
  auto int32_field = field("f", int32(), /*nullable=*/false);

  auto schema1 = schema({null_field->WithMetadata(metadata)});
  auto schema2 = schema({int32_field});

  ASSERT_OK_AND_ASSIGN(auto result, UnifySchemas({schema1, schema2}));
  AssertSchemaEqualsUnorderedFields(
      *result, *schema({int32_field->WithMetadata(metadata)->WithNullable(true)}));

  ASSERT_OK_AND_ASSIGN(result, UnifySchemas({schema2, schema1}));
  AssertSchemaEqualsUnorderedFields(*result, *schema({int32_field->WithNullable(true)}));
}

TEST_F(TestUnifySchemas, MoreSchemas) {
  auto int32_field = field("int32_field", int32());
  auto uint8_field = field("uint8_field", uint8(), false);
  auto utf8_field = field("utf8_field", utf8());

  ASSERT_OK_AND_ASSIGN(
      auto result,
      UnifySchemas({schema({int32_field}), schema({uint8_field}), schema({utf8_field})}));
  AssertSchemaEqualsUnorderedFields(
      *result, *schema({int32_field->WithNullable(true), uint8_field->WithNullable(false),
                        utf8_field->WithNullable(true)}));
}

TEST_F(TestUnifySchemas, IncompatibleTypes) {
  auto int32_field = field("f", int32());
  auto uint8_field = field("f", uint8(), false);

  auto schema1 = schema({int32_field});
  auto schema2 = schema({uint8_field});

  ASSERT_RAISES(Invalid, UnifySchemas({schema1, schema2}));
}

TEST_F(TestUnifySchemas, DuplicateFieldNames) {
  auto int32_field = field("int32_field", int32());
  auto utf8_field = field("utf8_field", utf8());

  auto schema1 = schema({int32_field, utf8_field});
  auto schema2 = schema({int32_field, int32_field, utf8_field});

  ASSERT_RAISES(Invalid, UnifySchemas({schema1, schema2}));
}

#define PRIMITIVE_TEST(KLASS, CTYPE, ENUM, NAME)                              \
  TEST(TypesTest, ARROW_CONCAT(TestPrimitive_, ENUM)) {                       \
    KLASS tp;                                                                 \
                                                                              \
    ASSERT_EQ(tp.id(), Type::ENUM);                                           \
    ASSERT_EQ(tp.ToString(), std::string(NAME));                              \
                                                                              \
    using CType = TypeTraits<KLASS>::CType;                                   \
    static_assert(std::is_same<CType, CTYPE>::value, "Not the same c-type!"); \
                                                                              \
    using DerivedArrowType = CTypeTraits<CTYPE>::ArrowType;                   \
    static_assert(std::is_same<DerivedArrowType, KLASS>::value,               \
                  "Not the same arrow-type!");                                \
  }

PRIMITIVE_TEST(Int8Type, int8_t, INT8, "int8");
PRIMITIVE_TEST(Int16Type, int16_t, INT16, "int16");
PRIMITIVE_TEST(Int32Type, int32_t, INT32, "int32");
PRIMITIVE_TEST(Int64Type, int64_t, INT64, "int64");
PRIMITIVE_TEST(UInt8Type, uint8_t, UINT8, "uint8");
PRIMITIVE_TEST(UInt16Type, uint16_t, UINT16, "uint16");
PRIMITIVE_TEST(UInt32Type, uint32_t, UINT32, "uint32");
PRIMITIVE_TEST(UInt64Type, uint64_t, UINT64, "uint64");

PRIMITIVE_TEST(FloatType, float, FLOAT, "float");
PRIMITIVE_TEST(DoubleType, double, DOUBLE, "double");

PRIMITIVE_TEST(BooleanType, bool, BOOL, "bool");

TEST(TestBinaryType, ToString) {
  BinaryType t1;
  BinaryType e1;
  StringType t2;
  AssertTypeEqual(t1, e1);
  AssertTypeNotEqual(t1, t2);
  ASSERT_EQ(t1.id(), Type::BINARY);
  ASSERT_EQ(t1.ToString(), std::string("binary"));
}

TEST(TestStringType, ToString) {
  StringType str;
  ASSERT_EQ(str.id(), Type::STRING);
  ASSERT_EQ(str.ToString(), std::string("string"));
}

TEST(TestLargeBinaryTypes, ToString) {
  BinaryType bt1;
  LargeBinaryType t1;
  LargeBinaryType e1;
  LargeStringType t2;
  AssertTypeEqual(t1, e1);
  AssertTypeNotEqual(t1, t2);
  AssertTypeNotEqual(t1, bt1);
  ASSERT_EQ(t1.id(), Type::LARGE_BINARY);
  ASSERT_EQ(t1.ToString(), std::string("large_binary"));
  ASSERT_EQ(t2.id(), Type::LARGE_STRING);
  ASSERT_EQ(t2.ToString(), std::string("large_string"));
}

TEST(TestFixedSizeBinaryType, ToString) {
  auto t = fixed_size_binary(10);
  ASSERT_EQ(t->id(), Type::FIXED_SIZE_BINARY);
  ASSERT_EQ("fixed_size_binary[10]", t->ToString());
}

TEST(TestFixedSizeBinaryType, Equals) {
  auto t1 = fixed_size_binary(10);
  auto t2 = fixed_size_binary(10);
  auto t3 = fixed_size_binary(3);

  AssertTypeEqual(*t1, *t2);
  AssertTypeNotEqual(*t1, *t3);
}

TEST(TestListType, Basics) {
  std::shared_ptr<DataType> vt = std::make_shared<UInt8Type>();

  ListType list_type(vt);
  ASSERT_EQ(list_type.id(), Type::LIST);

  ASSERT_EQ("list", list_type.name());
  ASSERT_EQ("list<item: uint8>", list_type.ToString());

  ASSERT_EQ(list_type.value_type()->id(), vt->id());
  ASSERT_EQ(list_type.value_type()->id(), vt->id());

  std::shared_ptr<DataType> st = std::make_shared<StringType>();
  std::shared_ptr<DataType> lt = std::make_shared<ListType>(st);
  ASSERT_EQ("list<item: string>", lt->ToString());

  ListType lt2(lt);
  ASSERT_EQ("list<item: list<item: string>>", lt2.ToString());
}

TEST(TestLargeListType, Basics) {
  std::shared_ptr<DataType> vt = std::make_shared<UInt8Type>();

  LargeListType list_type(vt);
  ASSERT_EQ(list_type.id(), Type::LARGE_LIST);

  ASSERT_EQ("large_list", list_type.name());
  ASSERT_EQ("large_list<item: uint8>", list_type.ToString());

  ASSERT_EQ(list_type.value_type()->id(), vt->id());
  ASSERT_EQ(list_type.value_type()->id(), vt->id());

  std::shared_ptr<DataType> st = std::make_shared<StringType>();
  std::shared_ptr<DataType> lt = std::make_shared<LargeListType>(st);
  ASSERT_EQ("large_list<item: string>", lt->ToString());

  LargeListType lt2(lt);
  ASSERT_EQ("large_list<item: large_list<item: string>>", lt2.ToString());
}

TEST(TestMapType, Basics) {
  auto md = key_value_metadata({"foo"}, {"foo value"});

  std::shared_ptr<DataType> kt = std::make_shared<StringType>();
  std::shared_ptr<DataType> it = std::make_shared<UInt8Type>();

  MapType map_type(kt, it);
  ASSERT_EQ(map_type.id(), Type::MAP);

  ASSERT_EQ("map", map_type.name());
  ASSERT_EQ("map<string, uint8>", map_type.ToString());

  ASSERT_EQ(map_type.key_type()->id(), kt->id());
  ASSERT_EQ(map_type.item_type()->id(), it->id());
  ASSERT_EQ(map_type.value_type()->id(), Type::STRUCT);

  std::shared_ptr<DataType> mt = std::make_shared<MapType>(it, kt);
  ASSERT_EQ("map<uint8, string>", mt->ToString());

  MapType mt2(kt, mt, /*keys_sorted=*/true);
  ASSERT_EQ("map<string, map<uint8, string>, keys_sorted>", mt2.ToString());
  AssertTypeNotEqual(map_type, mt2);
  MapType mt3(kt, mt);
  ASSERT_EQ("map<string, map<uint8, string>>", mt3.ToString());
  AssertTypeNotEqual(mt2, mt3);
  MapType mt4(kt, mt);
  AssertTypeEqual(mt3, mt4);

  // Field names are indifferent when comparing map types
  ASSERT_OK_AND_ASSIGN(
      auto mt5,
      MapType::Make(field(
          "some_entries",
          struct_({field("some_key", kt, false), field("some_value", mt)}), false)));
  AssertTypeEqual(mt3, *mt5);
  // ...unless we explicitly ask about them.
  ASSERT_FALSE(mt3.Equals(mt5, /*check_metadata=*/true));

  // nullability of value type matters in comparisons
  MapType map_type_non_nullable(kt, field("value", it, /*nullable=*/false));
  AssertTypeNotEqual(map_type, map_type_non_nullable);
}

TEST(TestMapType, Metadata) {
  auto md1 = key_value_metadata({"foo", "bar"}, {"foo value", "bar value"});
  auto md2 = key_value_metadata({"foo", "bar"}, {"foo value", "bar value"});
  auto md3 = key_value_metadata({"foo"}, {"foo value"});

  auto t1 = map(utf8(), field("value", int32(), md1));
  auto t2 = map(utf8(), field("value", int32(), md2));
  auto t3 = map(utf8(), field("value", int32(), md3));
  auto t4 =
      std::make_shared<MapType>(field("key", utf8(), md1), field("value", int32(), md2));
  ASSERT_OK_AND_ASSIGN(auto t5,
                       MapType::Make(field("some_entries",
                                           struct_({field("some_key", utf8(), false),
                                                    field("some_value", int32(), md2)}),
                                           false, md2)));

  AssertTypeEqual(*t1, *t2);
  AssertTypeEqual(*t1, *t2, /*check_metadata=*/true);

  AssertTypeEqual(*t1, *t3);
  AssertTypeNotEqual(*t1, *t3, /*check_metadata=*/true);

  AssertTypeEqual(*t1, *t4);
  AssertTypeNotEqual(*t1, *t4, /*check_metadata=*/true);

  AssertTypeEqual(*t1, *t5);
  AssertTypeNotEqual(*t1, *t5, /*check_metadata=*/true);
}

TEST(TestFixedSizeListType, Basics) {
  std::shared_ptr<DataType> vt = std::make_shared<UInt8Type>();

  FixedSizeListType fixed_size_list_type(vt, 4);
  ASSERT_EQ(fixed_size_list_type.id(), Type::FIXED_SIZE_LIST);

  ASSERT_EQ(4, fixed_size_list_type.list_size());
  ASSERT_EQ("fixed_size_list", fixed_size_list_type.name());
  ASSERT_EQ("fixed_size_list<item: uint8>[4]", fixed_size_list_type.ToString());

  ASSERT_EQ(fixed_size_list_type.value_type()->id(), vt->id());
  ASSERT_EQ(fixed_size_list_type.value_type()->id(), vt->id());

  std::shared_ptr<DataType> st = std::make_shared<StringType>();
  std::shared_ptr<DataType> lt = std::make_shared<FixedSizeListType>(st, 3);
  ASSERT_EQ("fixed_size_list<item: string>[3]", lt->ToString());

  FixedSizeListType lt2(lt, 7);
  ASSERT_EQ("fixed_size_list<item: fixed_size_list<item: string>[3]>[7]", lt2.ToString());
}

TEST(TestFixedSizeListType, Equals) {
  auto t1 = fixed_size_list(int8(), 3);
  auto t2 = fixed_size_list(int8(), 3);
  auto t3 = fixed_size_list(int8(), 4);
  auto t4 = fixed_size_list(int16(), 4);
  auto t5 = fixed_size_list(list(int16()), 4);
  auto t6 = fixed_size_list(list(int16()), 4);
  auto t7 = fixed_size_list(list(int32()), 4);

  AssertTypeEqual(t1, t2);
  AssertTypeNotEqual(t2, t3);
  AssertTypeNotEqual(t3, t4);
  AssertTypeNotEqual(t4, t5);
  AssertTypeEqual(t5, t6);
  AssertTypeNotEqual(t6, t7);
}

TEST(TestDateTypes, Attrs) {
  auto t1 = date32();
  auto t2 = date64();

  ASSERT_EQ("date32[day]", t1->ToString());
  ASSERT_EQ("date64[ms]", t2->ToString());

  ASSERT_EQ(32, checked_cast<const FixedWidthType&>(*t1).bit_width());
  ASSERT_EQ(64, checked_cast<const FixedWidthType&>(*t2).bit_width());
}

TEST(TestTimeType, Equals) {
  Time32Type t0;
  Time32Type t1(TimeUnit::SECOND);
  Time32Type t2(TimeUnit::MILLI);
  Time64Type t3(TimeUnit::MICRO);
  Time64Type t4(TimeUnit::NANO);
  Time64Type t5(TimeUnit::MICRO);

  ASSERT_EQ(32, t0.bit_width());
  ASSERT_EQ(64, t3.bit_width());

  AssertTypeEqual(t0, t2);
  AssertTypeEqual(t1, t1);
  AssertTypeNotEqual(t1, t3);
  AssertTypeNotEqual(t3, t4);
  AssertTypeEqual(t3, t5);
}

TEST(TestTimeType, ToString) {
  auto t1 = time32(TimeUnit::MILLI);
  auto t2 = time64(TimeUnit::NANO);
  auto t3 = time32(TimeUnit::SECOND);
  auto t4 = time64(TimeUnit::MICRO);

  ASSERT_EQ("time32[ms]", t1->ToString());
  ASSERT_EQ("time64[ns]", t2->ToString());
  ASSERT_EQ("time32[s]", t3->ToString());
  ASSERT_EQ("time64[us]", t4->ToString());
}

TEST(TestMonthIntervalType, Equals) {
  MonthIntervalType t1;
  MonthIntervalType t2;
  DayTimeIntervalType t3;

  AssertTypeEqual(t1, t2);
  AssertTypeNotEqual(t1, t3);
}

TEST(TestMonthIntervalType, ToString) {
  auto t1 = month_interval();

  ASSERT_EQ("month_interval", t1->ToString());
}

TEST(TestDayTimeIntervalType, Equals) {
  DayTimeIntervalType t1;
  DayTimeIntervalType t2;
  MonthIntervalType t3;

  AssertTypeEqual(t1, t2);
  AssertTypeNotEqual(t1, t3);
}

TEST(TestDayTimeIntervalType, ToString) {
  auto t1 = day_time_interval();

  ASSERT_EQ("day_time_interval", t1->ToString());
}

TEST(TestMonthDayNanoIntervalType, Equals) {
  MonthDayNanoIntervalType t1;
  MonthDayNanoIntervalType t2;
  MonthIntervalType t3;
  DayTimeIntervalType t4;

  AssertTypeEqual(t1, t2);
  AssertTypeNotEqual(t1, t3);
  AssertTypeNotEqual(t1, t4);
}

TEST(TestMonthDayNanoIntervalType, ToString) {
  auto t1 = month_day_nano_interval();

  ASSERT_EQ("month_day_nano_interval", t1->ToString());
}

TEST(TestDurationType, Equals) {
  DurationType t1;
  DurationType t2;
  DurationType t3(TimeUnit::NANO);
  DurationType t4(TimeUnit::NANO);

  AssertTypeEqual(t1, t2);
  AssertTypeNotEqual(t1, t3);
  AssertTypeEqual(t3, t4);
}

TEST(TestDurationType, ToString) {
  auto t1 = duration(TimeUnit::MILLI);
  auto t2 = duration(TimeUnit::NANO);
  auto t3 = duration(TimeUnit::SECOND);
  auto t4 = duration(TimeUnit::MICRO);

  ASSERT_EQ("duration[ms]", t1->ToString());
  ASSERT_EQ("duration[ns]", t2->ToString());
  ASSERT_EQ("duration[s]", t3->ToString());
  ASSERT_EQ("duration[us]", t4->ToString());
}

TEST(TestTimestampType, Equals) {
  TimestampType t1;
  TimestampType t2;
  TimestampType t3(TimeUnit::NANO);
  TimestampType t4(TimeUnit::NANO);

  DurationType dt1;
  DurationType dt2(TimeUnit::NANO);

  AssertTypeEqual(t1, t2);
  AssertTypeNotEqual(t1, t3);
  AssertTypeEqual(t3, t4);

  AssertTypeNotEqual(t1, dt1);
  AssertTypeNotEqual(t3, dt2);
}

TEST(TestTimestampType, ToString) {
  auto t1 = timestamp(TimeUnit::MILLI);
  auto t2 = timestamp(TimeUnit::NANO, "US/Eastern");
  auto t3 = timestamp(TimeUnit::SECOND);
  auto t4 = timestamp(TimeUnit::MICRO);

  ASSERT_EQ("timestamp[ms]", t1->ToString());
  ASSERT_EQ("timestamp[ns, tz=US/Eastern]", t2->ToString());
  ASSERT_EQ("timestamp[s]", t3->ToString());
  ASSERT_EQ("timestamp[us]", t4->ToString());
}

TEST(TestListType, Equals) {
  auto t1 = list(utf8());
  auto t2 = list(utf8());
  auto t3 = list(binary());
  auto t4 = list(field("item", utf8(), /*nullable=*/false));
  auto tl1 = large_list(binary());
  auto tl2 = large_list(binary());
  auto tl3 = large_list(float64());

  AssertTypeEqual(*t1, *t2);
  AssertTypeNotEqual(*t1, *t3);
  AssertTypeNotEqual(*t1, *t4);
  AssertTypeNotEqual(*t3, *tl1);
  AssertTypeEqual(*tl1, *tl2);
  AssertTypeNotEqual(*tl2, *tl3);

  std::shared_ptr<DataType> vt = std::make_shared<UInt8Type>();
  std::shared_ptr<Field> inner_field = std::make_shared<Field>("non_default_name", vt);

  ListType list_type(vt);
  ListType list_type_named(inner_field);

  AssertTypeEqual(list_type, list_type_named);
  ASSERT_FALSE(list_type.Equals(list_type_named, /*check_metadata=*/true));
}

TEST(TestListType, Metadata) {
  auto md1 = key_value_metadata({"foo", "bar"}, {"foo value", "bar value"});
  auto md2 = key_value_metadata({"foo", "bar"}, {"foo value", "bar value"});
  auto md3 = key_value_metadata({"foo"}, {"foo value"});

  auto f1 = field("item", utf8(), /*nullable =*/true, md1);
  auto f2 = field("item", utf8(), /*nullable =*/true, md2);
  auto f3 = field("item", utf8(), /*nullable =*/true, md3);
  auto f4 = field("item", utf8());
  auto f5 = field("item", utf8(), /*nullable =*/false, md1);

  auto t1 = list(f1);
  auto t2 = list(f2);
  auto t3 = list(f3);
  auto t4 = list(f4);
  auto t5 = list(f5);

  AssertTypeEqual(*t1, *t2);
  AssertTypeEqual(*t1, *t2, /*check_metadata =*/false);

  AssertTypeEqual(*t1, *t3);
  AssertTypeNotEqual(*t1, *t3, /*check_metadata =*/true);

  AssertTypeEqual(*t1, *t4);
  AssertTypeNotEqual(*t1, *t4, /*check_metadata =*/true);

  AssertTypeNotEqual(*t1, *t5);
  AssertTypeNotEqual(*t1, *t5, /*check_metadata =*/true);
}

TEST(TestNestedType, Equals) {
  auto create_struct = [](std::string inner_name,
                          std::string struct_name) -> std::shared_ptr<Field> {
    auto f_type = field(inner_name, int32());
    std::vector<std::shared_ptr<Field>> fields = {f_type};
    auto s_type = std::make_shared<StructType>(fields);
    return field(struct_name, s_type);
  };

  auto create_union = [](std::string inner_name,
                         std::string union_name) -> std::shared_ptr<Field> {
    auto f_type = field(inner_name, int32());
    std::vector<std::shared_ptr<Field>> fields = {f_type};
    std::vector<int8_t> codes = {42};
    return field(union_name, sparse_union(fields, codes));
  };

  auto s0 = create_struct("f0", "s0");
  auto s0_other = create_struct("f0", "s0");
  auto s0_bad = create_struct("f1", "s0");
  auto s1 = create_struct("f1", "s1");

  AssertFieldEqual(*s0, *s0_other);
  AssertFieldNotEqual(*s0, *s1);
  AssertFieldNotEqual(*s0, *s0_bad);

  auto u0 = create_union("f0", "u0");
  auto u0_other = create_union("f0", "u0");
  auto u0_bad = create_union("f1", "u0");
  auto u1 = create_union("f1", "u1");

  AssertFieldEqual(*u0, *u0_other);
  AssertFieldNotEqual(*u0, *u1);
  AssertFieldNotEqual(*u0, *u0_bad);
}

TEST(TestStructType, Basics) {
  auto f0_type = int32();
  auto f0 = field("f0", f0_type);

  auto f1_type = utf8();
  auto f1 = field("f1", f1_type);

  auto f2_type = uint8();
  auto f2 = field("f2", f2_type);

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};

  StructType struct_type(fields);

  ASSERT_TRUE(struct_type.field(0)->Equals(f0));
  ASSERT_TRUE(struct_type.field(1)->Equals(f1));
  ASSERT_TRUE(struct_type.field(2)->Equals(f2));

  ASSERT_EQ(struct_type.ToString(), "struct<f0: int32, f1: string, f2: uint8>");

  // TODO(wesm): out of bounds for field(...)
}

TEST(TestStructType, GetFieldByName) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  StructType struct_type({f0, f1, f2, f3});
  std::shared_ptr<Field> result;

  result = struct_type.GetFieldByName("f1");
  ASSERT_EQ(f1, result);

  result = struct_type.GetFieldByName("f3");
  ASSERT_EQ(f3, result);

  result = struct_type.GetFieldByName("not-found");
  ASSERT_EQ(result, nullptr);
}

TEST(TestStructType, GetFieldIndex) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  StructType struct_type({f0, f1, f2, f3});

  ASSERT_EQ(0, struct_type.GetFieldIndex(f0->name()));
  ASSERT_EQ(1, struct_type.GetFieldIndex(f1->name()));
  ASSERT_EQ(2, struct_type.GetFieldIndex(f2->name()));
  ASSERT_EQ(3, struct_type.GetFieldIndex(f3->name()));
  ASSERT_EQ(-1, struct_type.GetFieldIndex("not-found"));
}

TEST(TestStructType, GetFieldDuplicates) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", int64());
  auto f2 = field("f1", utf8());
  StructType struct_type({f0, f1, f2});

  ASSERT_EQ(0, struct_type.GetFieldIndex("f0"));
  ASSERT_EQ(-1, struct_type.GetFieldIndex("f1"));
  ASSERT_EQ(std::vector<int>{0}, struct_type.GetAllFieldIndices(f0->name()));
  ASSERT_EQ(std::vector<int>({1, 2}), struct_type.GetAllFieldIndices(f1->name()));

  std::vector<std::shared_ptr<Field>> results;

  results = struct_type.GetAllFieldsByName(f0->name());
  ASSERT_EQ(results.size(), 1);
  ASSERT_TRUE(results[0]->Equals(f0));

  results = struct_type.GetAllFieldsByName(f1->name());
  ASSERT_EQ(results.size(), 2);
  if (results[0]->type()->id() == Type::INT64) {
    ASSERT_TRUE(results[0]->Equals(f1));
    ASSERT_TRUE(results[1]->Equals(f2));
  } else {
    ASSERT_TRUE(results[0]->Equals(f2));
    ASSERT_TRUE(results[1]->Equals(f1));
  }

  results = struct_type.GetAllFieldsByName("not-found");
  ASSERT_EQ(results.size(), 0);
}

TEST(TestStructType, TestFieldsDifferOnlyInMetadata) {
  auto f0 = field("f", utf8(), true, nullptr);
  auto f1 = field("f", utf8(), true, key_value_metadata({{"foo", "baz"}}));

  StructType s0({f0, f1});
  StructType s1({f1, f0});

  AssertTypeEqual(s0, s1);
  AssertTypeNotEqual(s0, s1, /* check_metadata = */ true);

  ASSERT_EQ(s0.fingerprint(), s1.fingerprint());
  ASSERT_NE(s0.metadata_fingerprint(), s1.metadata_fingerprint());
}

TEST(TestStructType, FieldModifierMethods) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", utf8());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1};

  StructType struct_type(fields);

  ASSERT_OK_AND_ASSIGN(auto new_struct, struct_type.AddField(1, field("f2", int8())));
  ASSERT_EQ(3, new_struct->num_fields());
  ASSERT_EQ(1, new_struct->GetFieldIndex("f2"));

  ASSERT_OK_AND_ASSIGN(new_struct, new_struct->RemoveField(1));
  ASSERT_EQ(2, new_struct->num_fields());
  ASSERT_EQ(-1, new_struct->GetFieldIndex("f2"));  // No f2 after removal

  ASSERT_OK_AND_ASSIGN(new_struct, new_struct->SetField(1, field("f2", int8())));
  ASSERT_EQ(2, new_struct->num_fields());
  ASSERT_EQ(1, new_struct->GetFieldIndex("f2"));
  ASSERT_EQ(int8(), new_struct->GetFieldByName("f2")->type());

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  testing::HasSubstr("Invalid column index to add field"),
                                  new_struct->AddField(5, field("f5", int8())));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Invalid column index to remove field"),
      new_struct->RemoveField(-1));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  testing::HasSubstr("Invalid column index to set field"),
                                  new_struct->SetField(2, field("f5", int8())));
}

TEST(TestUnionType, Basics) {
  auto f0_type = int32();
  auto f0 = field("f0", f0_type);
  auto f1_type = utf8();
  auto f1 = field("f1", f1_type);
  auto f2_type = uint8();
  auto f2 = field("f2", f2_type);

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  std::vector<int8_t> type_codes1 = {0, 1, 2};
  std::vector<int8_t> type_codes2 = {10, 11, 12};
  std::vector<int> child_ids1(128, -1);
  std::vector<int> child_ids2(128, -1);
  child_ids1[0] = 0;
  child_ids1[1] = 1;
  child_ids1[2] = 2;
  child_ids2[10] = 0;
  child_ids2[11] = 1;
  child_ids2[12] = 2;

  auto ty1 = checked_pointer_cast<UnionType>(dense_union(fields));
  auto ty2 = checked_pointer_cast<UnionType>(dense_union(fields, type_codes1));
  auto ty3 = checked_pointer_cast<UnionType>(dense_union(fields, type_codes2));
  auto ty4 = checked_pointer_cast<UnionType>(sparse_union(fields));
  auto ty5 = checked_pointer_cast<UnionType>(sparse_union(fields, type_codes1));
  auto ty6 = checked_pointer_cast<UnionType>(sparse_union(fields, type_codes2));

  ASSERT_EQ(ty1->type_codes(), type_codes1);
  ASSERT_EQ(ty2->type_codes(), type_codes1);
  ASSERT_EQ(ty3->type_codes(), type_codes2);
  ASSERT_EQ(ty4->type_codes(), type_codes1);
  ASSERT_EQ(ty5->type_codes(), type_codes1);
  ASSERT_EQ(ty6->type_codes(), type_codes2);

  ASSERT_EQ(ty1->child_ids(), child_ids1);
  ASSERT_EQ(ty2->child_ids(), child_ids1);
  ASSERT_EQ(ty3->child_ids(), child_ids2);
  ASSERT_EQ(ty4->child_ids(), child_ids1);
  ASSERT_EQ(ty5->child_ids(), child_ids1);
  ASSERT_EQ(ty6->child_ids(), child_ids2);
}

TEST(TestDictionaryType, Basics) {
  auto value_type = int32();

  std::shared_ptr<DictionaryType> type1 =
      std::dynamic_pointer_cast<DictionaryType>(dictionary(int16(), value_type));

  auto type2 = std::dynamic_pointer_cast<DictionaryType>(
      ::arrow::dictionary(int16(), type1, true));

  ASSERT_TRUE(int16()->Equals(type1->index_type()));
  ASSERT_TRUE(type1->value_type()->Equals(value_type));

  ASSERT_TRUE(int16()->Equals(type2->index_type()));
  ASSERT_TRUE(type2->value_type()->Equals(type1));

  ASSERT_EQ("dictionary<values=int32, indices=int16, ordered=0>", type1->ToString());
  ASSERT_EQ(
      "dictionary<values="
      "dictionary<values=int32, indices=int16, ordered=0>, "
      "indices=int16, ordered=1>",
      type2->ToString());
}

TEST(TestDictionaryType, Equals) {
  auto t1 = dictionary(int8(), int32());
  auto t2 = dictionary(int8(), int32());
  auto t3 = dictionary(int16(), int32());
  auto t4 = dictionary(int8(), int16());

  AssertTypeEqual(*t1, *t2);
  AssertTypeNotEqual(*t1, *t3);
  AssertTypeNotEqual(*t1, *t4);

  auto t5 = dictionary(int8(), int32(), /*ordered=*/false);
  auto t6 = dictionary(int8(), int32(), /*ordered=*/true);
  AssertTypeNotEqual(*t5, *t6);
}

TEST(TypesTest, TestDecimal128Small) {
  Decimal128Type t1(8, 4);

  EXPECT_EQ(t1.id(), Type::DECIMAL128);
  EXPECT_EQ(t1.precision(), 8);
  EXPECT_EQ(t1.scale(), 4);

  EXPECT_EQ(t1.ToString(), std::string("decimal128(8, 4)"));

  // Test properties
  EXPECT_EQ(t1.byte_width(), 16);
  EXPECT_EQ(t1.bit_width(), 128);
}

TEST(TypesTest, TestDecimal128Medium) {
  Decimal128Type t1(12, 5);

  EXPECT_EQ(t1.id(), Type::DECIMAL128);
  EXPECT_EQ(t1.precision(), 12);
  EXPECT_EQ(t1.scale(), 5);

  EXPECT_EQ(t1.ToString(), std::string("decimal128(12, 5)"));

  // Test properties
  EXPECT_EQ(t1.byte_width(), 16);
  EXPECT_EQ(t1.bit_width(), 128);
}

TEST(TypesTest, TestDecimal128Large) {
  Decimal128Type t1(27, 7);

  EXPECT_EQ(t1.id(), Type::DECIMAL128);
  EXPECT_EQ(t1.precision(), 27);
  EXPECT_EQ(t1.scale(), 7);

  EXPECT_EQ(t1.ToString(), std::string("decimal128(27, 7)"));

  // Test properties
  EXPECT_EQ(t1.byte_width(), 16);
  EXPECT_EQ(t1.bit_width(), 128);
}

TEST(TypesTest, TestDecimal256Small) {
  Decimal256Type t1(8, 4);

  EXPECT_EQ(t1.id(), Type::DECIMAL256);
  EXPECT_EQ(t1.precision(), 8);
  EXPECT_EQ(t1.scale(), 4);

  EXPECT_EQ(t1.ToString(), std::string("decimal256(8, 4)"));

  // Test properties
  EXPECT_EQ(t1.byte_width(), 32);
  EXPECT_EQ(t1.bit_width(), 256);
}

TEST(TypesTest, TestDecimal256Medium) {
  Decimal256Type t1(12, 5);

  EXPECT_EQ(t1.id(), Type::DECIMAL256);
  EXPECT_EQ(t1.precision(), 12);
  EXPECT_EQ(t1.scale(), 5);

  EXPECT_EQ(t1.ToString(), std::string("decimal256(12, 5)"));

  // Test properties
  EXPECT_EQ(t1.byte_width(), 32);
  EXPECT_EQ(t1.bit_width(), 256);
}

TEST(TypesTest, TestDecimal256Large) {
  Decimal256Type t1(76, 38);

  EXPECT_EQ(t1.id(), Type::DECIMAL256);
  EXPECT_EQ(t1.precision(), 76);
  EXPECT_EQ(t1.scale(), 38);

  EXPECT_EQ(t1.ToString(), std::string("decimal256(76, 38)"));

  // Test properties
  EXPECT_EQ(t1.byte_width(), 32);
  EXPECT_EQ(t1.bit_width(), 256);
}

TEST(TypesTest, TestDecimalEquals) {
  Decimal128Type t1(8, 4);
  Decimal128Type t2(8, 4);
  Decimal128Type t3(8, 5);
  Decimal128Type t4(27, 5);

  Decimal256Type t5(8, 4);
  Decimal256Type t6(8, 4);
  Decimal256Type t7(8, 5);
  Decimal256Type t8(27, 5);

  FixedSizeBinaryType t9(16);
  FixedSizeBinaryType t10(32);

  AssertTypeEqual(t1, t2);
  AssertTypeNotEqual(t1, t3);
  AssertTypeNotEqual(t1, t4);
  AssertTypeNotEqual(t1, t9);

  AssertTypeEqual(t5, t6);
  AssertTypeNotEqual(t5, t1);
  AssertTypeNotEqual(t5, t7);
  AssertTypeNotEqual(t5, t8);
  AssertTypeNotEqual(t5, t10);
}

TEST(TypesTest, TestRunEndEncodedType) {
  auto int8_ree_expected = std::make_shared<RunEndEncodedType>(int32(), list(int8()));
  auto int8_ree_type = run_end_encoded(int32(), list(int8()));
  auto int32_ree_type = run_end_encoded(int32(), list(int32()));

  ASSERT_EQ(*int8_ree_expected, *int8_ree_type);
  ASSERT_NE(*int8_ree_expected, *int32_ree_type);

  ASSERT_EQ(int8_ree_type->id(), Type::RUN_END_ENCODED);
  ASSERT_EQ(int32_ree_type->id(), Type::RUN_END_ENCODED);

  auto int8_ree_type_cast = std::dynamic_pointer_cast<RunEndEncodedType>(int8_ree_type);
  auto int32_ree_type_cast = std::dynamic_pointer_cast<RunEndEncodedType>(int32_ree_type);
  ASSERT_EQ(*int8_ree_type_cast->value_type(), *list(int8()));
  ASSERT_EQ(*int32_ree_type_cast->value_type(), *list(int32()));

  ASSERT_TRUE(int8_ree_type_cast->field(0)->Equals(Field("run_ends", int32(), false)));
  ASSERT_TRUE(int8_ree_type_cast->field(1)->Equals(Field("values", list(int8()), true)));

  auto int16_int32_ree_type = run_end_encoded(int16(), list(int32()));
  auto int64_int32_ree_type = run_end_encoded(int64(), list(int32()));
  ASSERT_NE(*int32_ree_type, *int16_int32_ree_type);
  ASSERT_NE(*int32_ree_type, *int64_int32_ree_type);
  ASSERT_NE(*int16_int32_ree_type, *int64_int32_ree_type);

  ASSERT_EQ(int16_int32_ree_type->ToString(),
            "run_end_encoded<run_ends: int16, values: list<item: int32>>");
  ASSERT_EQ(int8_ree_type->ToString(),
            "run_end_encoded<run_ends: int32, values: list<item: int8>>");
  ASSERT_EQ(int64_int32_ree_type->ToString(),
            "run_end_encoded<run_ends: int64, values: list<item: int32>>");
}

#define TEST_PREDICATE(all_types, type_predicate)                 \
  for (auto type : all_types) {                                   \
    ASSERT_EQ(type_predicate(type->id()), type_predicate(*type)); \
  }

TEST(TypesTest, TestMembership) {
  std::vector<std::shared_ptr<DataType>> all_types;
  for (auto type : NumericTypes()) {
    all_types.push_back(type);
  }
  for (auto type : TemporalTypes()) {
    all_types.push_back(type);
  }
  for (auto type : IntervalTypes()) {
    all_types.push_back(type);
  }
  for (auto type : PrimitiveTypes()) {
    all_types.push_back(type);
  }
  TEST_PREDICATE(all_types, is_integer);
  TEST_PREDICATE(all_types, is_signed_integer);
  TEST_PREDICATE(all_types, is_unsigned_integer);
  TEST_PREDICATE(all_types, is_floating);
  TEST_PREDICATE(all_types, is_numeric);
  TEST_PREDICATE(all_types, is_decimal);
  TEST_PREDICATE(all_types, is_primitive);
  TEST_PREDICATE(all_types, is_base_binary_like);
  TEST_PREDICATE(all_types, is_binary_like);
  TEST_PREDICATE(all_types, is_large_binary_like);
  TEST_PREDICATE(all_types, is_binary);
  TEST_PREDICATE(all_types, is_string);
  TEST_PREDICATE(all_types, is_temporal);
  TEST_PREDICATE(all_types, is_interval);
  TEST_PREDICATE(all_types, is_dictionary);
  TEST_PREDICATE(all_types, is_fixed_size_binary);
  TEST_PREDICATE(all_types, is_fixed_width);
  TEST_PREDICATE(all_types, is_list_like);
  TEST_PREDICATE(all_types, is_nested);
  TEST_PREDICATE(all_types, is_union);
}

#undef TEST_PREDICATE

}  // namespace arrow
