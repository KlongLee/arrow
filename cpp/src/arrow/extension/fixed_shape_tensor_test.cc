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

#include "arrow/extension/fixed_shape_tensor.h"

#include "arrow/testing/matchers.h"

#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/tensor.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {

using FixedShapeTensorType = extension::FixedShapeTensorType;
using extension::fixed_shape_tensor;
using extension::FixedShapeTensorArray;

class TestExtensionType : public ::testing::Test {
 public:
  void SetUp() override {
    shape_ = {3, 3, 4};
    cell_shape_ = {3, 4};
    value_type_ = int64();
    cell_type_ = fixed_size_list(value_type_, 12);
    dim_names_ = {"x", "y"};
    ext_type_ = internal::checked_pointer_cast<ExtensionType>(
        fixed_shape_tensor(value_type_, cell_shape_, {}, dim_names_));
    values_ = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
               18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35};
    values_partial_ = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                       12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
    shape_partial_ = {2, 3, 4};
    tensor_strides_ = {96, 32, 8};
    cell_strides_ = {32, 8};
    serialized_ = R"({"shape":[3,4],"dim_names":["x","y"]})";
  }

 protected:
  std::vector<int64_t> shape_;
  std::vector<int64_t> shape_partial_;
  std::vector<int64_t> cell_shape_;
  std::shared_ptr<DataType> value_type_;
  std::shared_ptr<DataType> cell_type_;
  std::vector<std::string> dim_names_;
  std::shared_ptr<ExtensionType> ext_type_;
  std::vector<int64_t> values_;
  std::vector<int64_t> values_partial_;
  std::vector<int64_t> tensor_strides_;
  std::vector<int64_t> cell_strides_;
  std::string serialized_;
};

auto RoundtripBatch = [](const std::shared_ptr<RecordBatch>& batch,
                         std::shared_ptr<RecordBatch>* out) {
  ASSERT_OK_AND_ASSIGN(auto out_stream, io::BufferOutputStream::Create());
  ASSERT_OK(ipc::WriteRecordBatchStream({batch}, ipc::IpcWriteOptions::Defaults(),
                                        out_stream.get()));

  ASSERT_OK_AND_ASSIGN(auto complete_ipc_stream, out_stream->Finish());

  io::BufferReader reader(complete_ipc_stream);
  std::shared_ptr<RecordBatchReader> batch_reader;
  ASSERT_OK_AND_ASSIGN(batch_reader, ipc::RecordBatchStreamReader::Open(&reader));
  ASSERT_OK(batch_reader->ReadNext(out));
};

TEST_F(TestExtensionType, CheckDummyRegistration) {
  // We need a registered dummy type at runtime to allow for IPC deserialization
  auto registered_type = GetExtensionType("arrow.fixed_shape_tensor");
  ASSERT_TRUE(registered_type->type_id == Type::EXTENSION);
}

TEST_F(TestExtensionType, CreateExtensionType) {
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);

  // Test ExtensionType methods
  ASSERT_EQ(ext_type_->extension_name(), "arrow.fixed_shape_tensor");
  ASSERT_TRUE(ext_type_->Equals(*exact_ext_type));
  ASSERT_FALSE(ext_type_->Equals(*cell_type_));
  ASSERT_TRUE(ext_type_->storage_type()->Equals(*cell_type_));
  ASSERT_EQ(ext_type_->Serialize(), serialized_);
  ASSERT_OK_AND_ASSIGN(auto ds,
                       ext_type_->Deserialize(ext_type_->storage_type(), serialized_));
  auto deserialized = std::reinterpret_pointer_cast<ExtensionType>(ds);
  ASSERT_TRUE(deserialized->Equals(*ext_type_));

  // Test FixedShapeTensorType methods
  ASSERT_EQ(exact_ext_type->id(), Type::EXTENSION);
  ASSERT_EQ(exact_ext_type->ndim(), cell_shape_.size());
  ASSERT_EQ(exact_ext_type->shape(), cell_shape_);
  ASSERT_EQ(exact_ext_type->strides(), cell_strides_);
  ASSERT_EQ(exact_ext_type->dim_names(), dim_names_);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Invalid: permutation size must match shape size."),
      FixedShapeTensorType::Make(value_type_, cell_shape_, {0}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Invalid: dim_names size must match shape size."),
      FixedShapeTensorType::Make(value_type_, cell_shape_, {}, {"x"}));
}

TEST_F(TestExtensionType, EqualsCases) {
  auto ext_type_permutation_1 = fixed_shape_tensor(int64(), {3, 4}, {0, 1}, {"x", "y"});
  auto ext_type_permutation_2 = fixed_shape_tensor(int64(), {3, 4}, {1, 0}, {"x", "y"});
  auto ext_type_no_permutation = fixed_shape_tensor(int64(), {3, 4}, {}, {"x", "y"});

  ASSERT_TRUE(ext_type_permutation_1->Equals(ext_type_permutation_1));

  ASSERT_FALSE(fixed_shape_tensor(int32(), {3, 4}, {}, {"x", "y"})
                   ->Equals(ext_type_no_permutation));
  ASSERT_FALSE(fixed_shape_tensor(int64(), {2, 4}, {}, {"x", "y"})
                   ->Equals(ext_type_no_permutation));
  ASSERT_FALSE(fixed_shape_tensor(int64(), {3, 4}, {}, {"H", "W"})
                   ->Equals(ext_type_no_permutation));

  ASSERT_TRUE(ext_type_no_permutation->Equals(ext_type_permutation_1));
  ASSERT_TRUE(ext_type_permutation_1->Equals(ext_type_no_permutation));
  ASSERT_FALSE(ext_type_no_permutation->Equals(ext_type_permutation_2));
  ASSERT_FALSE(ext_type_permutation_2->Equals(ext_type_no_permutation));
  ASSERT_FALSE(ext_type_permutation_1->Equals(ext_type_permutation_2));
  ASSERT_FALSE(ext_type_permutation_2->Equals(ext_type_permutation_1));
}

TEST_F(TestExtensionType, CreateFromArray) {
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);

  std::vector<std::shared_ptr<Buffer>> buffers = {nullptr, Buffer::Wrap(values_)};
  auto arr_data = std::make_shared<ArrayData>(value_type_, values_.size(), buffers, 0, 0);
  auto arr = std::make_shared<Int64Array>(arr_data);
  ASSERT_OK_AND_ASSIGN(auto fsla_arr, FixedSizeListArray::FromArrays(arr, cell_type_));
  auto data = fsla_arr->data();
  data->type = ext_type_;
  auto ext_arr = exact_ext_type->MakeArray(data);
  ASSERT_EQ(ext_arr->length(), shape_[0]);
  ASSERT_EQ(ext_arr->null_count(), 0);
}

TEST_F(TestExtensionType, CreateFromTensor) {
  std::vector<int64_t> column_major_strides = {8, 24, 72};
  std::vector<int64_t> neither_major_strides = {96, 8, 32};

  ASSERT_OK_AND_ASSIGN(auto tensor,
                       Tensor::Make(value_type_, Buffer::Wrap(values_), shape_));

  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);
  ASSERT_OK_AND_ASSIGN(auto ext_arr, FixedShapeTensorArray::FromTensor(tensor));

  ASSERT_OK(ext_arr->ValidateFull());
  ASSERT_TRUE(tensor->is_row_major());
  ASSERT_EQ(tensor->strides(), tensor_strides_);
  ASSERT_EQ(ext_arr->length(), shape_[0]);

  auto ext_type_2 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4}, {0, 1}));
  ASSERT_OK_AND_ASSIGN(auto ext_arr_2, FixedShapeTensorArray::FromTensor(tensor));

  ASSERT_OK_AND_ASSIGN(
      auto column_major_tensor,
      Tensor::Make(value_type_, Buffer::Wrap(values_), shape_, column_major_strides));
  auto ext_type_3 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4}, {0, 1}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr(
          "Invalid: Only first-major tensors can be zero-copy converted to arrays"),
      FixedShapeTensorArray::FromTensor(column_major_tensor));
  ASSERT_THAT(FixedShapeTensorArray::FromTensor(column_major_tensor),
              Raises(StatusCode::Invalid));

  auto neither_major_tensor = std::make_shared<Tensor>(value_type_, Buffer::Wrap(values_),
                                                       shape_, neither_major_strides);
  auto ext_type_4 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4}, {1, 0}));
  ASSERT_OK_AND_ASSIGN(auto ext_arr_4,
                       FixedShapeTensorArray::FromTensor(neither_major_tensor));

  auto ext_type_5 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(binary(), {1, 2}));
  auto arr = ArrayFromJSON(binary(), R"(["abc", "def"])");

  ASSERT_OK_AND_ASSIGN(auto fsla_arr,
                       FixedSizeListArray::FromArrays(arr, fixed_size_list(binary(), 1)));
  auto data = fsla_arr->data();
  data->type = ext_type_5;
  auto ext_arr_5 = ext_type_5->MakeArray(data);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("binary is not valid data type for a tensor"),
      exact_ext_type->ToTensor(ext_arr_5));

  auto ext_type_6 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {1, 2}));
  auto arr_with_null = ArrayFromJSON(int64(), "[0, null]");
  ASSERT_OK_AND_ASSIGN(auto fsla_arr_6, FixedSizeListArray::FromArrays(
                                            arr_with_null, fixed_size_list(int64(), 1)));
  auto data6 = fsla_arr_6->data();
  data6->type = ext_type_6;
  data6->null_count = 1;

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Null values not supported in tensors."),
      ext_type_6->ToTensor(ext_type_6->MakeArray(data6)));
}

void CheckTensorRoundtrip(const std::shared_ptr<Tensor>& tensor,
                          std::shared_ptr<DataType> expected_ext_type) {
  auto ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(expected_ext_type);
  ASSERT_OK_AND_ASSIGN(auto ext_arr, FixedShapeTensorArray::FromTensor(tensor));
  auto generated_ext_type =
      internal::checked_pointer_cast<FixedShapeTensorType>(ext_arr->type());

  // Check that generated type is equal to the expected type
  ASSERT_EQ(generated_ext_type->type_name(), ext_type->type_name());
  ASSERT_EQ(generated_ext_type->shape(), ext_type->shape());
  ASSERT_EQ(generated_ext_type->dim_names(), ext_type->dim_names());
  ASSERT_EQ(generated_ext_type->permutation(), ext_type->permutation());
  ASSERT_TRUE(generated_ext_type->storage_type()->Equals(*ext_type->storage_type()));
  ASSERT_TRUE(generated_ext_type->Equals(ext_type));

  // Check Tensor roundtrip
  ASSERT_OK_AND_ASSIGN(auto tensor_from_array, generated_ext_type->ToTensor(ext_arr));
  ASSERT_EQ(tensor->type(), tensor_from_array->type());
  ASSERT_EQ(tensor->shape(), tensor_from_array->shape());
  for (size_t i = 1; i < tensor->dim_names().size(); i++) {
    ASSERT_EQ(tensor->dim_names()[i], tensor_from_array->dim_names()[i]);
  }
  ASSERT_EQ(tensor->strides(), tensor_from_array->strides());
  ASSERT_TRUE(tensor->data()->Equals(*tensor_from_array->data()));
  ASSERT_TRUE(tensor->Equals(*tensor_from_array));
}

TEST_F(TestExtensionType, RoundtripTensor) {
  auto values = Buffer::Wrap(values_);
  ASSERT_OK_AND_ASSIGN(auto tensor1, Tensor::Make(value_type_, values, {3, 3, 4},
                                                  {96, 32, 8}, {"", "y", "z"}));
  ASSERT_OK_AND_ASSIGN(auto tensor2,
                       Tensor::Make(value_type_, values, {3, 3, 4}, {96, 8, 24}));
  ASSERT_OK_AND_ASSIGN(auto tensor3,
                       Tensor::Make(value_type_, values, {3, 4, 3}, {96, 24, 8}));
  ASSERT_OK_AND_ASSIGN(auto tensor4,
                       Tensor::Make(value_type_, values, {3, 4, 3}, {96, 8, 32}));
  ASSERT_OK_AND_ASSIGN(auto tensor5,
                       Tensor::Make(value_type_, values, {6, 2, 3}, {48, 24, 8}));
  ASSERT_OK_AND_ASSIGN(auto tensor6,
                       Tensor::Make(value_type_, values, {6, 2, 3}, {48, 8, 16}));
  ASSERT_OK_AND_ASSIGN(auto tensor7,
                       Tensor::Make(value_type_, values, {2, 3, 6}, {144, 48, 8}));
  ASSERT_OK_AND_ASSIGN(auto tensor8,
                       Tensor::Make(value_type_, values, {2, 3, 6}, {144, 8, 24}));
  ASSERT_OK_AND_ASSIGN(auto tensor9,
                       Tensor::Make(value_type_, values, {2, 3, 2, 3}, {144, 48, 24, 8}));
  ASSERT_OK_AND_ASSIGN(auto tensor10,
                       Tensor::Make(value_type_, values, {2, 3, 2, 3}, {144, 8, 24, 48}));

  CheckTensorRoundtrip(tensor1,
                       fixed_shape_tensor(value_type_, {3, 4}, {0, 1}, {"y", "z"}));
  CheckTensorRoundtrip(tensor2, fixed_shape_tensor(value_type_, {3, 4}, {1, 0}, {}));
  CheckTensorRoundtrip(tensor3, fixed_shape_tensor(value_type_, {4, 3}, {0, 1}));
  CheckTensorRoundtrip(tensor4, fixed_shape_tensor(value_type_, {4, 3}, {1, 0}));
  CheckTensorRoundtrip(tensor5, fixed_shape_tensor(value_type_, {2, 3}, {0, 1}));
  CheckTensorRoundtrip(tensor6, fixed_shape_tensor(value_type_, {2, 3}, {1, 0}));
  CheckTensorRoundtrip(tensor7, fixed_shape_tensor(value_type_, {3, 6}, {0, 1}));
  CheckTensorRoundtrip(tensor8, fixed_shape_tensor(value_type_, {3, 6}, {1, 0}));
  CheckTensorRoundtrip(tensor9, fixed_shape_tensor(value_type_, {3, 2, 3}, {0, 1, 2}));
  CheckTensorRoundtrip(tensor10, fixed_shape_tensor(value_type_, {3, 2, 3}, {2, 1, 0}));
}

TEST_F(TestExtensionType, SliceTensor) {
  ASSERT_OK_AND_ASSIGN(auto tensor,
                       Tensor::Make(value_type_, Buffer::Wrap(values_), shape_));
  ASSERT_OK_AND_ASSIGN(
      auto tensor_partial,
      Tensor::Make(value_type_, Buffer::Wrap(values_partial_), shape_partial_));
  ASSERT_EQ(tensor->strides(), tensor_strides_);
  ASSERT_EQ(tensor_partial->strides(), tensor_strides_);
  auto ext_type = fixed_shape_tensor(value_type_, cell_shape_, {}, dim_names_);
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);

  ASSERT_OK_AND_ASSIGN(auto ext_arr, FixedShapeTensorArray::FromTensor(tensor));
  ASSERT_OK_AND_ASSIGN(auto ext_arr_partial,
                       FixedShapeTensorArray::FromTensor(tensor_partial));
  ASSERT_OK(ext_arr->ValidateFull());
  ASSERT_OK(ext_arr_partial->ValidateFull());

  auto sliced = internal::checked_pointer_cast<ExtensionArray>(ext_arr->Slice(0, 2));
  auto partial = internal::checked_pointer_cast<ExtensionArray>(ext_arr_partial);

  ASSERT_TRUE(sliced->Equals(*partial));
  ASSERT_OK(sliced->ValidateFull());
  ASSERT_OK(partial->ValidateFull());
  ASSERT_TRUE(sliced->storage()->Equals(*partial->storage()));
  ASSERT_EQ(sliced->length(), partial->length());
}

void CheckSerializationRoundtrip(const std::shared_ptr<DataType>& ext_type) {
  auto fst_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type);
  auto serialized = fst_type->Serialize();
  ASSERT_OK_AND_ASSIGN(auto deserialized,
                       fst_type->Deserialize(fst_type->storage_type(), serialized));
  ASSERT_TRUE(fst_type->Equals(*deserialized));
}

void CheckDeserializationRaises(const std::shared_ptr<DataType>& storage_type,
                                const std::string& serialized,
                                const std::string& expected_message) {
  auto fst_type = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr(expected_message),
                                  fst_type->Deserialize(storage_type, serialized));
}

TEST_F(TestExtensionType, MetadataSerializationRoundtrip) {
  CheckSerializationRoundtrip(ext_type_);
  CheckSerializationRoundtrip(fixed_shape_tensor(value_type_, {}, {}, {}));
  CheckSerializationRoundtrip(fixed_shape_tensor(value_type_, {0}, {}, {}));
  CheckSerializationRoundtrip(fixed_shape_tensor(value_type_, {1}, {0}, {"x"}));
  CheckSerializationRoundtrip(
      fixed_shape_tensor(value_type_, {256, 256, 3}, {0, 1, 2}, {"H", "W", "C"}));
  CheckSerializationRoundtrip(
      fixed_shape_tensor(value_type_, {256, 256, 3}, {2, 0, 1}, {"C", "H", "W"}));

  auto storage_type = fixed_size_list(int64(), 12);
  CheckDeserializationRaises(boolean(), R"({"shape":[3,4]})",
                             "Expected FixedSizeList storage type, got bool");
  CheckDeserializationRaises(storage_type, R"({"dim_names":["x","y"]})",
                             "Invalid serialized JSON data");
  CheckDeserializationRaises(storage_type, R"({"shape":(3,4)})",
                             "Invalid serialized JSON data");
  CheckDeserializationRaises(storage_type, R"({"shape":[3,4],"permutation":[1,0,2]})",
                             "Invalid permutation");
  CheckDeserializationRaises(storage_type, R"({"shape":[3],"dim_names":["x","y"]})",
                             "Invalid dim_names");
}

TEST_F(TestExtensionType, RoudtripBatch) {
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);

  std::vector<std::shared_ptr<Buffer>> buffers = {nullptr, Buffer::Wrap(values_)};
  auto arr_data = std::make_shared<ArrayData>(value_type_, values_.size(), buffers, 0, 0);
  auto arr = std::make_shared<Int64Array>(arr_data);
  ASSERT_OK_AND_ASSIGN(auto fsla_arr, FixedSizeListArray::FromArrays(arr, cell_type_));
  auto data = fsla_arr->data();
  data->type = ext_type_;
  auto ext_arr = exact_ext_type->MakeArray(data);

  auto ext_metadata =
      key_value_metadata({{"ARROW:extension:name", exact_ext_type->extension_name()},
                          {"ARROW:extension:metadata", serialized_}});
  auto ext_field = field("f0", exact_ext_type, true, ext_metadata);
  auto batch = RecordBatch::Make(schema({ext_field}), ext_arr->length(), {ext_arr});
  std::shared_ptr<RecordBatch> read_batch;
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, /*compare_metadata=*/true);
}

TEST_F(TestExtensionType, RoudtripBatchFromTensor) {
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);
  ASSERT_OK_AND_ASSIGN(auto tensor, Tensor::Make(value_type_, Buffer::Wrap(values_),
                                                 shape_, {}, {"n", "x", "y"}));
  ASSERT_OK_AND_ASSIGN(auto ext_arr, FixedShapeTensorArray::FromTensor(tensor));
  ext_arr->data()->type = exact_ext_type;

  auto ext_metadata =
      key_value_metadata({{"ARROW:extension:name", ext_type_->extension_name()},
                          {"ARROW:extension:metadata", serialized_}});
  auto ext_field = field("f0", ext_type_, true, ext_metadata);
  auto batch = RecordBatch::Make(schema({ext_field}), ext_arr->length(), {ext_arr});
  std::shared_ptr<RecordBatch> read_batch;
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, /*compare_metadata=*/true);
}

TEST_F(TestExtensionType, ComputeStrides) {
  auto exact_ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(ext_type_);

  auto ext_type_1 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), cell_shape_, {}, dim_names_));
  auto ext_type_2 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), cell_shape_, {}, dim_names_));
  auto ext_type_3 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int32(), cell_shape_, {}, dim_names_));
  ASSERT_TRUE(ext_type_1->Equals(*ext_type_2));
  ASSERT_FALSE(ext_type_1->Equals(*ext_type_3));

  auto ext_type_4 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4, 7}, {}, {"x", "y", "z"}));
  ASSERT_EQ(ext_type_4->strides(), (std::vector<int64_t>{224, 56, 8}));
  ext_type_4 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4, 7}, {0, 1, 2}, {"x", "y", "z"}));
  ASSERT_EQ(ext_type_4->strides(), (std::vector<int64_t>{224, 56, 8}));

  auto ext_type_5 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4, 7}, {1, 0, 2}));
  ASSERT_EQ(ext_type_5->strides(), (std::vector<int64_t>{168, 56, 8}));
  ASSERT_EQ(ext_type_5->Serialize(), R"({"shape":[3,4,7],"permutation":[1,0,2]})");

  auto ext_type_6 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int64(), {3, 4, 7}, {1, 2, 0}, {}));
  ASSERT_EQ(ext_type_6->strides(), (std::vector<int64_t>{168, 24, 8}));
  ASSERT_EQ(ext_type_6->Serialize(), R"({"shape":[3,4,7],"permutation":[1,2,0]})");

  auto ext_type_7 = internal::checked_pointer_cast<FixedShapeTensorType>(
      fixed_shape_tensor(int32(), {3, 4, 7}, {2, 0, 1}, {}));
  ASSERT_EQ(ext_type_7->strides(), (std::vector<int64_t>{48, 16, 4}));
  ASSERT_EQ(ext_type_7->Serialize(), R"({"shape":[3,4,7],"permutation":[2,0,1]})");
}

}  // namespace arrow
