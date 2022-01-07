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

export { MessageHeader } from './fb/message-header.js';

export {
    Type,
    BufferType,
    DateUnit,
    TimeUnit,
    Precision,
    UnionMode,
    IntervalUnit,
    MetadataVersion,
} from './enum.js';

export { Data, makeData } from './data.js';
export {
    DataType, TypeMap,
    Null,
    Bool,
    Int, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64,
    Float, Float16, Float32, Float64,
    Utf8,
    Binary,
    FixedSizeBinary,
    Date_, DateDay, DateMillisecond,
    Timestamp, TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
    Time, TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond,
    Decimal,
    List,
    Struct,
    Union, DenseUnion, SparseUnion,
    Dictionary,
    Interval, IntervalDayTime, IntervalYearMonth,
    FixedSizeList,
    Map_
} from './type.js';

export { Table, makeTable, tableFromArrays } from './table.js';
export { Vector, makeVector, vectorFromArray } from './vector.js';
export { Visitor } from './visitor.js';
export { Schema, Field } from './schema.js';

export { MapRow } from './row/map.js';
export { StructRow } from './row/struct.js';

export {
    Builder,
    BinaryBuilder,
    BoolBuilder,
    DateBuilder, DateDayBuilder, DateMillisecondBuilder,
    DecimalBuilder,
    DictionaryBuilder,
    FixedSizeBinaryBuilder,
    FixedSizeListBuilder,
    FloatBuilder, Float16Builder, Float32Builder, Float64Builder,
    IntervalBuilder, IntervalDayTimeBuilder, IntervalYearMonthBuilder,
    IntBuilder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, Uint8Builder, Uint16Builder, Uint32Builder, Uint64Builder,
    ListBuilder,
    MapBuilder,
    NullBuilder,
    StructBuilder,
    TimestampBuilder, TimestampSecondBuilder, TimestampMillisecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
    TimeBuilder, TimeSecondBuilder, TimeMillisecondBuilder, TimeMicrosecondBuilder, TimeNanosecondBuilder,
    UnionBuilder, DenseUnionBuilder, SparseUnionBuilder,
    Utf8Builder,
} from './builder/index.js';

export { ByteStream, AsyncByteStream, AsyncByteQueue, ReadableSource, WritableSink } from './io/stream.js';
export { RecordBatchReader, RecordBatchFileReader, RecordBatchStreamReader, AsyncRecordBatchFileReader, AsyncRecordBatchStreamReader } from './ipc/reader.js';
export { RecordBatchWriter, RecordBatchFileWriter, RecordBatchStreamWriter, RecordBatchJSONWriter } from './ipc/writer.js';
export { serialize, deserialize } from './ipc/serialization.js';
export { MessageReader, AsyncMessageReader, JSONMessageReader } from './ipc/message.js';
export { Message } from './ipc/metadata/message.js';
export { RecordBatch } from './recordbatch.js';
export { ArrowJSONLike, FileHandle, Readable, Writable, ReadableWritable, ReadableDOMStreamOptions } from './io/interfaces.js';

import * as util_bn_ from './util/bn.js';
import * as util_int_ from './util/int.js';
import * as util_bit_ from './util/bit.js';
import * as util_math_ from './util/math.js';
import * as util_buffer_ from './util/buffer.js';
import * as util_vector_ from './util/vector.js';
import { compareSchemas, compareFields, compareTypes } from './visitor/typecomparator.js';

/** @ignore */
export const util = {
    ...util_bn_,
    ...util_int_,
    ...util_bit_,
    ...util_math_,
    ...util_buffer_,
    ...util_vector_,
    compareSchemas,
    compareFields,
    compareTypes,
};
