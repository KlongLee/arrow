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

#include <string>
#include <vector>

#include "arrow/adapters/orc/adapter_util.h"
#include "arrow/array/builder_base.h"
#include "arrow/builder.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/range.h"

#include "orc/Exceptions.hh"
#include "orc/OrcFile.hh"

// alias to not interfere with nested orc namespace
namespace liborc = orc;

namespace arrow {

namespace adapters {

namespace orc {

using internal::checked_cast;

// The number of nanoseconds in a second
constexpr int64_t kOneSecondNanos = 1000000000LL;

Status AppendStructBatch(const liborc::Type* type, liborc::ColumnVectorBatch* cbatch,
                         int64_t offset, int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<StructBuilder*>(abuilder);
  auto batch = checked_cast<liborc::StructVectorBatch*>(cbatch);

  const uint8_t* valid_bytes = nullptr;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }
  RETURN_NOT_OK(builder->AppendValues(length, valid_bytes));

  for (int i = 0; i < builder->num_fields(); i++) {
    RETURN_NOT_OK(AppendBatch(type->getSubtype(i), batch->fields[i], offset, length,
                              builder->field_builder(i)));
  }
  return Status::OK();
}

Status AppendListBatch(const liborc::Type* type, liborc::ColumnVectorBatch* cbatch,
                       int64_t offset, int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<ListBuilder*>(abuilder);
  auto batch = checked_cast<liborc::ListVectorBatch*>(cbatch);
  liborc::ColumnVectorBatch* elements = batch->elements.get();
  const liborc::Type* elemtype = type->getSubtype(0);

  const bool has_nulls = batch->hasNulls;
  for (int64_t i = offset; i < length + offset; i++) {
    if (!has_nulls || batch->notNull[i]) {
      int64_t start = batch->offsets[i];
      int64_t end = batch->offsets[i + 1];
      RETURN_NOT_OK(builder->Append());
      RETURN_NOT_OK(
          AppendBatch(elemtype, elements, start, end - start, builder->value_builder()));
    } else {
      RETURN_NOT_OK(builder->AppendNull());
    }
  }
  return Status::OK();
}

Status AppendMapBatch(const liborc::Type* type, liborc::ColumnVectorBatch* cbatch,
                      int64_t offset, int64_t length, ArrayBuilder* abuilder) {
  auto list_builder = checked_cast<ListBuilder*>(abuilder);
  auto struct_builder = checked_cast<StructBuilder*>(list_builder->value_builder());
  auto batch = checked_cast<liborc::MapVectorBatch*>(cbatch);
  liborc::ColumnVectorBatch* keys = batch->keys.get();
  liborc::ColumnVectorBatch* vals = batch->elements.get();
  const liborc::Type* keytype = type->getSubtype(0);
  const liborc::Type* valtype = type->getSubtype(1);

  const bool has_nulls = batch->hasNulls;
  for (int64_t i = offset; i < length + offset; i++) {
    RETURN_NOT_OK(list_builder->Append());
    int64_t start = batch->offsets[i];
    int64_t list_length = batch->offsets[i + 1] - start;
    if (list_length && (!has_nulls || batch->notNull[i])) {
      RETURN_NOT_OK(struct_builder->AppendValues(list_length, nullptr));
      RETURN_NOT_OK(AppendBatch(keytype, keys, start, list_length,
                                struct_builder->field_builder(0)));
      RETURN_NOT_OK(AppendBatch(valtype, vals, start, list_length,
                                struct_builder->field_builder(1)));
    }
  }
  return Status::OK();
}

template <class builder_type, class batch_type, class elem_type>
Status AppendNumericBatch(liborc::ColumnVectorBatch* cbatch, int64_t offset,
                          int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<builder_type*>(abuilder);
  auto batch = checked_cast<batch_type*>(cbatch);

  if (length == 0) {
    return Status::OK();
  }
  const uint8_t* valid_bytes = nullptr;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }
  const elem_type* source = batch->data.data() + offset;
  RETURN_NOT_OK(builder->AppendValues(source, length, valid_bytes));
  return Status::OK();
}

template <class builder_type, class target_type, class batch_type, class source_type>
Status AppendNumericBatchCast(liborc::ColumnVectorBatch* cbatch, int64_t offset,
                              int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<builder_type*>(abuilder);
  auto batch = checked_cast<batch_type*>(cbatch);

  if (length == 0) {
    return Status::OK();
  }

  const uint8_t* valid_bytes = nullptr;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }
  const source_type* source = batch->data.data() + offset;
  auto cast_iter = internal::MakeLazyRange(
      [&source](int64_t index) { return static_cast<target_type>(source[index]); },
      length);

  RETURN_NOT_OK(builder->AppendValues(cast_iter.begin(), cast_iter.end(), valid_bytes));

  return Status::OK();
}

Status AppendBoolBatch(liborc::ColumnVectorBatch* cbatch, int64_t offset, int64_t length,
                       ArrayBuilder* abuilder) {
  auto builder = checked_cast<BooleanBuilder*>(abuilder);
  auto batch = checked_cast<liborc::LongVectorBatch*>(cbatch);

  if (length == 0) {
    return Status::OK();
  }

  const uint8_t* valid_bytes = nullptr;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }
  const int64_t* source = batch->data.data() + offset;

  auto cast_iter = internal::MakeLazyRange(
      [&source](int64_t index) { return static_cast<bool>(source[index]); }, length);

  RETURN_NOT_OK(builder->AppendValues(cast_iter.begin(), cast_iter.end(), valid_bytes));

  return Status::OK();
}

Status AppendTimestampBatch(liborc::ColumnVectorBatch* cbatch, int64_t offset,
                            int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<TimestampBuilder*>(abuilder);
  auto batch = checked_cast<liborc::TimestampVectorBatch*>(cbatch);

  if (length == 0) {
    return Status::OK();
  }

  const uint8_t* valid_bytes = nullptr;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }

  const int64_t* seconds = batch->data.data() + offset;
  const int64_t* nanos = batch->nanoseconds.data() + offset;

  auto transform_timestamp = [seconds, nanos](int64_t index) {
    return seconds[index] * kOneSecondNanos + nanos[index];
  };

  auto transform_range = internal::MakeLazyRange(transform_timestamp, length);

  RETURN_NOT_OK(
      builder->AppendValues(transform_range.begin(), transform_range.end(), valid_bytes));
  return Status::OK();
}

template <class builder_type>
Status AppendBinaryBatch(liborc::ColumnVectorBatch* cbatch, int64_t offset,
                         int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<builder_type*>(abuilder);
  auto batch = checked_cast<liborc::StringVectorBatch*>(cbatch);

  const bool has_nulls = batch->hasNulls;
  for (int64_t i = offset; i < length + offset; i++) {
    if (!has_nulls || batch->notNull[i]) {
      RETURN_NOT_OK(
          builder->Append(batch->data[i], static_cast<int32_t>(batch->length[i])));
    } else {
      RETURN_NOT_OK(builder->AppendNull());
    }
  }
  return Status::OK();
}

Status AppendFixedBinaryBatch(liborc::ColumnVectorBatch* cbatch, int64_t offset,
                              int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<FixedSizeBinaryBuilder*>(abuilder);
  auto batch = checked_cast<liborc::StringVectorBatch*>(cbatch);

  const bool has_nulls = batch->hasNulls;
  for (int64_t i = offset; i < length + offset; i++) {
    if (!has_nulls || batch->notNull[i]) {
      RETURN_NOT_OK(builder->Append(batch->data[i]));
    } else {
      RETURN_NOT_OK(builder->AppendNull());
    }
  }
  return Status::OK();
}

Status AppendDecimalBatch(const liborc::Type* type, liborc::ColumnVectorBatch* cbatch,
                          int64_t offset, int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<Decimal128Builder*>(abuilder);

  const bool has_nulls = cbatch->hasNulls;
  if (type->getPrecision() == 0 || type->getPrecision() > 18) {
    auto batch = checked_cast<liborc::Decimal128VectorBatch*>(cbatch);
    for (int64_t i = offset; i < length + offset; i++) {
      if (!has_nulls || batch->notNull[i]) {
        RETURN_NOT_OK(builder->Append(
            Decimal128(batch->values[i].getHighBits(), batch->values[i].getLowBits())));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }
  } else {
    auto batch = checked_cast<liborc::Decimal64VectorBatch*>(cbatch);
    for (int64_t i = offset; i < length + offset; i++) {
      if (!has_nulls || batch->notNull[i]) {
        RETURN_NOT_OK(builder->Append(Decimal128(batch->values[i])));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }
  }
  return Status::OK();
}

Status AppendBatch(const liborc::Type* type, liborc::ColumnVectorBatch* batch,
                   int64_t offset, int64_t length, ArrayBuilder* builder) {
  if (type == nullptr) {
    return Status::OK();
  }
  liborc::TypeKind kind = type->getKind();
  switch (kind) {
    case liborc::STRUCT:
      return AppendStructBatch(type, batch, offset, length, builder);
    case liborc::LIST:
      return AppendListBatch(type, batch, offset, length, builder);
    case liborc::MAP:
      return AppendMapBatch(type, batch, offset, length, builder);
    case liborc::LONG:
      return AppendNumericBatch<Int64Builder, liborc::LongVectorBatch, int64_t>(
          batch, offset, length, builder);
    case liborc::INT:
      return AppendNumericBatchCast<Int32Builder, int32_t, liborc::LongVectorBatch,
                                    int64_t>(batch, offset, length, builder);
    case liborc::SHORT:
      return AppendNumericBatchCast<Int16Builder, int16_t, liborc::LongVectorBatch,
                                    int64_t>(batch, offset, length, builder);
    case liborc::BYTE:
      return AppendNumericBatchCast<Int8Builder, int8_t, liborc::LongVectorBatch,
                                    int64_t>(batch, offset, length, builder);
    case liborc::DOUBLE:
      return AppendNumericBatch<DoubleBuilder, liborc::DoubleVectorBatch, double>(
          batch, offset, length, builder);
    case liborc::FLOAT:
      return AppendNumericBatchCast<FloatBuilder, float, liborc::DoubleVectorBatch,
                                    double>(batch, offset, length, builder);
    case liborc::BOOLEAN:
      return AppendBoolBatch(batch, offset, length, builder);
    case liborc::VARCHAR:
    case liborc::STRING:
      return AppendBinaryBatch<StringBuilder>(batch, offset, length, builder);
    case liborc::BINARY:
      return AppendBinaryBatch<BinaryBuilder>(batch, offset, length, builder);
    case liborc::CHAR:
      return AppendFixedBinaryBatch(batch, offset, length, builder);
    case liborc::DATE:
      return AppendNumericBatchCast<Date32Builder, int32_t, liborc::LongVectorBatch,
                                    int64_t>(batch, offset, length, builder);
    case liborc::TIMESTAMP:
      return AppendTimestampBatch(batch, offset, length, builder);
    case liborc::DECIMAL:
      return AppendDecimalBatch(type, batch, offset, length, builder);
    default:
      return Status::NotImplemented("Not implemented type kind: ", kind);
  }
}

// template <class array_type, class batch_type, class elem_type>
// Status FillNumericBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t& orcOffset, int64_t length, Array* parray){
//   auto array = checked_cast<array_type*>(parray);
//   auto batch = checked_cast<batch_type*>(cbatch);
//   int64_t arrowLength = array.length();
//   if (!arrowLength)
//     return Status::OK();
//   int64_t arrowEnd = arrowOffset + arrowLength;
//   int64_t initORCOffset = orcOffset;
//   if (array->null_count)
//     batch->hasNulls = true;
//   for (; orcOffset < length && arrowOffset < arrowEnd; orcOffset++, arrowOffset++) {
//     if (array->IsNull(arrowOffset)) {
//       batch->notNull[orcOffset] = false;
//     }
//     else {
//       batch->data[orcOffset] = array->Value(arrowOffset);
//     }
//   }
//   batch->numElements += orcOffset - initORCOffset;
//   return Status::OK();
// }



// Status FillStructBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t offset, int64_t length, Array* parray){
//   auto array = checked_cast<StructArray*>(parray);
//   auto batch = checked_cast<liborc::StructVectorBatch*>(cbatch);
//   auto size = type->fields().size();
//   int64_t lastIndex = offset + length;
//   for (auto i = 0; i < size; i++) {
//     for (auto j = offset; j < lastIndex; j++) {
//       if 
//       subarray = array->field(i).get();
//     }
//   }
// }

// Status FillListBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t& orcOffset, int64_t length, Array* parray){
//   auto array = checked_cast<ListArray*>(parray);
//   auto batch = checked_cast<liborc::ListVectorBatch*>(cbatch);
//   int64_t arrowLength = array.length();
//   if (!arrowLength)
//     return Status::OK();
//   int64_t arrowEnd = arrowOffset + arrowLength;
//   int64_t initORCOffset = orcOffset;
//   if (array->null_count)
//     batch->hasNulls = true;
//   for (; orcOffset < length && arrowOffset < arrowEnd; orcOffset++, arrowOffset++) {
//     if (array->IsNull(arrowOffset)) {
//       batch->notNull[orcOffset] = false;
//     }
//     else {
//       batch->data[orcOffset] = array->Value(arrowOffset);
//       batch->data[orcOffset] = array->Value(arrowOffset);
//       FillBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t orcOffset, int64_t length, ChunkedArray* pchunkedArray)
//     }
//   }
//   batch->numElements += orcOffset - initORCOffset;
//   return Status::OK();
// }

// Status FillBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t orcOffset, int64_t length, ChunkedArray* pchunkedArray){

// }


Status GetArrowType(const liborc::Type* type, std::shared_ptr<DataType>* out) {
  // When subselecting fields on read, liborc will set some nodes to nullptr,
  // so we need to check for nullptr before progressing
  if (type == nullptr) {
    *out = null();
    return Status::OK();
  }
  liborc::TypeKind kind = type->getKind();
  const int subtype_count = static_cast<int>(type->getSubtypeCount());

  switch (kind) {
    case liborc::BOOLEAN:
      *out = boolean();
      break;
    case liborc::BYTE:
      *out = int8();
      break;
    case liborc::SHORT:
      *out = int16();
      break;
    case liborc::INT:
      *out = int32();
      break;
    case liborc::LONG:
      *out = int64();
      break;
    case liborc::FLOAT:
      *out = float32();
      break;
    case liborc::DOUBLE:
      *out = float64();
      break;
    case liborc::VARCHAR:
    case liborc::STRING:
      *out = utf8();
      break;
    case liborc::BINARY:
      *out = binary();
      break;
    case liborc::CHAR:
      *out = fixed_size_binary(static_cast<int>(type->getMaximumLength()));
      break;
    case liborc::TIMESTAMP:
      *out = timestamp(TimeUnit::NANO);
      break;
    case liborc::DATE:
      *out = date32();
      break;
    case liborc::DECIMAL: {
      const int precision = static_cast<int>(type->getPrecision());
      const int scale = static_cast<int>(type->getScale());
      if (precision == 0) {
        // In HIVE 0.11/0.12 precision is set as 0, but means max precision
        *out = decimal(38, 6);
      } else {
        *out = decimal(precision, scale);
      }
      break;
    }
    case liborc::LIST: {
      if (subtype_count != 1) {
        return Status::Invalid("Invalid Orc List type");
      }
      std::shared_ptr<DataType> elemtype;
      RETURN_NOT_OK(GetArrowType(type->getSubtype(0), &elemtype));
      *out = list(elemtype);
      break;
    }
    case liborc::MAP: {
      if (subtype_count != 2) {
        return Status::Invalid("Invalid Orc Map type");
      }
      std::shared_ptr<DataType> keytype;
      std::shared_ptr<DataType> valtype;
      RETURN_NOT_OK(GetArrowType(type->getSubtype(0), &keytype));
      RETURN_NOT_OK(GetArrowType(type->getSubtype(1), &valtype));
      *out = list(struct_({field("key", keytype), field("value", valtype)}));
      break;
    }
    case liborc::STRUCT: {
      std::vector<std::shared_ptr<Field>> fields;
      for (int child = 0; child < subtype_count; ++child) {
        std::shared_ptr<DataType> elemtype;
        RETURN_NOT_OK(GetArrowType(type->getSubtype(child), &elemtype));
        std::string name = type->getFieldName(child);
        fields.push_back(field(name, elemtype));
      }
      *out = struct_(fields);
      break;
    }
    case liborc::UNION: {
      std::vector<std::shared_ptr<Field>> fields;
      std::vector<int8_t> type_codes;
      for (int child = 0; child < subtype_count; ++child) {
        std::shared_ptr<DataType> elemtype;
        RETURN_NOT_OK(GetArrowType(type->getSubtype(child), &elemtype));
        fields.push_back(field("_union_" + std::to_string(child), elemtype));
        type_codes.push_back(static_cast<int8_t>(child));
      }
      *out = sparse_union(fields, type_codes);
      break;
    }
    default: {
      return Status::Invalid("Unknown Orc type kind: ", kind);
    }
  }
  return Status::OK();
}

Status GetORCType(const DataType* type, ORC_UNIQUE_PTR<liborc::Type> out) {

  //Check for nullptr
  if (type == nullptr){
    out.reset();
    return Status::OK();
  }

  Type::type kind = type->id();
  const int subtype_count = static_cast<int>(type->num_fields());

  switch (kind) {
    case Type::type::NA:
      out.reset(); //Makes out nullptr
      break;
    case Type::type::BOOL:
      out = std::move(orc::createPrimitiveType(liborc::TypeKind::BOOLEAN));
      break;
    case Type::type::UINT8:
    case Type::type::INT8:
      out = std::move(orc::createPrimitiveType(liborc::TypeKind::BYTE));
      break;
    case Type::type::UINT16:
    case Type::type::INT16:
      out = std::move(orc::createPrimitiveType(liborc::TypeKind::SHORT));
      break;
    case Type::type::UINT32:
    case Type::type::INT32:
    case Type::type::INTERVAL_MONTHS:
      out = std::move(orc::createPrimitiveType(liborc::TypeKind::INT));
      break;
    case Type::type::UINT64:
    case Type::type::INT64:
    case Type::type::INTERVAL_DAY_TIME:
    case Type::type::DURATION:
      out = std::move(orc::createPrimitiveType(liborc::TypeKind::LONG));
      break;
    case Type::type::HALF_FLOAT://Convert to float32 since ORC does not have float16
    case Type::type::FLOAT:
      out = std::move(orc::createPrimitiveType(liborc::TypeKind::FLOAT));
      break;
    case Type::type::DOUBLE:
      out = std::move(orc::createPrimitiveType(liborc::TypeKind::DOUBLE));
      break;
    //Use STRING instead of VARCHAR for now, both use UTF-8
    case Type::type::STRING:
    case Type::type::LARGE_STRING:
      out = std::move(orc::createPrimitiveType(liborc::TypeKind::STRING));
      break;
    case Type::type::FIXED_SIZE_BINARY:
      out = std::move(orc::createCharType(liborc::TypeKind::CHAR, internal::GetByteWidth(*type)));
      break;
    case Type::type::BINARY:
    case Type::type::LARGE_BINARY:
      out = std::move(orc::createPrimitiveType(liborc::TypeKind::BINARY));
      break;
    case Type::type::DATE32:
      out = std::move(orc::createPrimitiveType(liborc::TypeKind::DATE));
      break;
    case Type::type::TIMESTAMP:
    case Type::type::TIME32:
    case Type::type::TIME64:
    case Type::type::DATE64:
      out = std::move(orc::createPrimitiveType(liborc::TypeKind::TIMESTAMP));
      break;

    case Type::type::DECIMAL: {
      const int precision = static_cast<int>(type->precision());
      const int scale = static_cast<int>(type->scale());
      if (precision == 0) {
        // In HIVE 0.11/0.12 precision is set as 0, but means max precision
        out = std::move(orc::createDecimalType(liborc::TypeKind::DECIMAL));
      } else {
        out = std::move(orc::createDecimalType(liborc::TypeKind::DECIMAL, precision, scale));
      }
      break;
    }
    case Type::type::LIST:
    case Type::type::FIXED_SIZE_LIST:
    case Type::type::LAֵֵRGE_LIST: {
      DataType* arrowChildType = type->value_type().get();
      ORC_UNIQUE_PTR<liborc::Type> orcSubtype;
      RETURN_NOT_OK(GetORCType(arrowChildType, orcSubtype));
      out = std::move(orc::createListType(orcSubtype));
      break;
    }
    case Type::type::STRUCT: {
      out = std::move(orc::createStructType());
      std::vector<std::shared_ptr<Field>> arrowFields = type->fields();
      for (std::vector<std::shared_ptr<Field>>::iterator it = arrowFields.begin(); it != arrowFields.end(); ++it) {
        std::string fieldName = it->name();
        DataType* arrowChildType = it->type().get();
        ORC_UNIQUE_PTR<liborc::Type> orcSubtype;
        RETURN_NOT_OK(GetORCType(arrowChildType, orcSubtype));
        out->addStructField(fieldName, orcSubtype);
      }
      break;
    }
    case Type::type::MAP: {
      DataType* keyArrowType = type->key_type().get();
      DataType* valueArrowType = type->key_type().get();
      ORC_UNIQUE_PTR<liborc::Type> keyORCType;
      RETURN_NOT_OK(GetORCType(keyArrowType, keyORCType));
      ORC_UNIQUE_PTR<liborc::Type> valueORCType;
      RETURN_NOT_OK(GetORCType(valueArrowType, valueORCType));
      out = std::move(orc::createMapType(keyORCType, valueORCType));
      break;
    }
    case Type::type::DENSE_UNION:
    case Type::type::SPARSE_UNION: {
      out = std::move(orc::createUnionType());
      std::vector<std::shared_ptr<Field>> arrowFields = type->fields();
      for (std::vector<std::shared_ptr<Field>>::iterator it = arrowFields.begin(); it != arrowFields.end(); ++it) {
        std::string fieldName = it->name();
        DataType* arrowChildType = it->type().get();
        ORC_UNIQUE_PTR<liborc::Type> orcSubtype;
        RETURN_NOT_OK(GetORCType(arrowChildType, orcSubtype));
        out->addUnionChild(fieldName, orcSubtype);
      }
      break;
    }
    //Dictionary is an encoding method, not a TypeKind in ORC. Hence we need to get the actual value type.
    case Type::type::DICTIONARY: {
      DataType* arrowValueType = type->value_type().get();
      RETURN_NOT_OK(GetORCType(arrowValueType, out));
    }
    default: {
      return Status::Invalid("Unknown Arrow type kind: ", kind);
    }
  }
  return Status::OK();
}

}  // namespace orc
}  // namespace adapters
}  // namespace arrow
