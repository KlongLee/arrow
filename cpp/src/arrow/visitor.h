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

#ifndef ARROW_VISITOR_H
#define ARROW_VISITOR_H

#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {

class ARROW_EXPORT ArrayVisitor {
 public:
  virtual ~ArrayVisitor() = default;

  virtual Status Visit(const NullArray& array);
  virtual Status Visit(const BooleanArray& array);
  virtual Status Visit(const Int8Array& array);
  virtual Status Visit(const Int16Array& array);
  virtual Status Visit(const Int32Array& array);
  virtual Status Visit(const Int64Array& array);
  virtual Status Visit(const UInt8Array& array);
  virtual Status Visit(const UInt16Array& array);
  virtual Status Visit(const UInt32Array& array);
  virtual Status Visit(const UInt64Array& array);
  virtual Status Visit(const HalfFloatArray& array);
  virtual Status Visit(const FloatArray& array);
  virtual Status Visit(const DoubleArray& array);
  virtual Status Visit(const StringArray& array);
  virtual Status Visit(const BinaryArray& array);
  virtual Status Visit(const FixedSizeBinaryArray& array);
  virtual Status Visit(const Date32Array& array);
  virtual Status Visit(const Date64Array& array);
  virtual Status Visit(const Time32Array& array);
  virtual Status Visit(const Time64Array& array);
  virtual Status Visit(const TimestampArray& array);
  virtual Status Visit(const IntervalArray& array);
  virtual Status Visit(const Decimal128Array& array);
  virtual Status Visit(const ListArray& array);
  virtual Status Visit(const StructArray& array);
  virtual Status Visit(const UnionArray& array);
  virtual Status Visit(const DictionaryArray& array);
  virtual Status Visit(const ExtensionArray& array);
};

class ARROW_EXPORT TypeVisitor {
 public:
  virtual ~TypeVisitor() = default;

  virtual Status Visit(const NullType& type);
  virtual Status Visit(const BooleanType& type);
  virtual Status Visit(const Int8Type& type);
  virtual Status Visit(const Int16Type& type);
  virtual Status Visit(const Int32Type& type);
  virtual Status Visit(const Int64Type& type);
  virtual Status Visit(const UInt8Type& type);
  virtual Status Visit(const UInt16Type& type);
  virtual Status Visit(const UInt32Type& type);
  virtual Status Visit(const UInt64Type& type);
  virtual Status Visit(const HalfFloatType& type);
  virtual Status Visit(const FloatType& type);
  virtual Status Visit(const DoubleType& type);
  virtual Status Visit(const StringType& type);
  virtual Status Visit(const BinaryType& type);
  virtual Status Visit(const FixedSizeBinaryType& type);
  virtual Status Visit(const Date64Type& type);
  virtual Status Visit(const Date32Type& type);
  virtual Status Visit(const Time32Type& type);
  virtual Status Visit(const Time64Type& type);
  virtual Status Visit(const TimestampType& type);
  virtual Status Visit(const IntervalType& type);
  virtual Status Visit(const Decimal128Type& type);
  virtual Status Visit(const ListType& type);
  virtual Status Visit(const StructType& type);
  virtual Status Visit(const UnionType& type);
  virtual Status Visit(const DictionaryType& type);
  virtual Status Visit(const ExtensionType& type);
};

}  // namespace arrow

#endif  // ARROW_VISITOR_H
