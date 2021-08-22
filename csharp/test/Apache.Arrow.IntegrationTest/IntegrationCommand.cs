﻿// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Apache.Arrow.Arrays;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Apache.Arrow.IntegrationTest
{
    public class IntegrationCommand
    {
        public string Mode { get; set; }
        public FileInfo JsonFileInfo { get; set; }
        public FileInfo ArrowFileInfo { get; set; }

        public IntegrationCommand(string mode, FileInfo jsonFileInfo, FileInfo arrowFileInfo)
        {
            Mode = mode;
            JsonFileInfo = jsonFileInfo;
            ArrowFileInfo = arrowFileInfo;
        }

        public async Task Execute()
        {
            Func<Task> commandDelegate = Mode switch
            {
                "validate" => Validate,
                "json-to-arrow" => JsonToArrow,
                "stream-to-file" => StreamToFile,
                "file-to-stream" => FileToStream,
                _ => () =>
                {
                    Console.WriteLine($"Mode '{Mode}' is not supported.");
                    return Task.CompletedTask;
                }
            };
            await commandDelegate();
        }

        private Task Validate()
        {
            return Task.CompletedTask;
        }

        private async Task JsonToArrow()
        {
            JsonFile jsonFile = await ParseJsonFile();
            Schema schema = CreateSchema(jsonFile.Schema);

            using (FileStream fs = ArrowFileInfo.Create())
            {
                ArrowFileWriter writer = new ArrowFileWriter(fs, schema);
                foreach (var jsonRecordBatch in jsonFile.Batches)
                {
                    RecordBatch batch = CreateRecordBatch(schema, jsonRecordBatch);
                    await writer.WriteRecordBatchAsync(batch);
                }
                await writer.WriteEndAsync();
            }
        }

        private RecordBatch CreateRecordBatch(Schema schema, JsonRecordBatch jsonRecordBatch)
        {
            if (schema.Fields.Count != jsonRecordBatch.Columns.Count)
            {
                throw new NotSupportedException($"jsonRecordBatch.Columns.Count '{jsonRecordBatch.Columns.Count}' doesn't match schema field count '{schema.Fields.Count}'");
            }

            List<IArrowArray> arrays = new List<IArrowArray>(jsonRecordBatch.Columns.Count);
            for (int i = 0; i < jsonRecordBatch.Columns.Count; i++)
            {
                JsonFieldData data = jsonRecordBatch.Columns[i];
                Field field = schema.GetFieldByName(data.Name);
                ArrayCreator creator = new ArrayCreator(data);
                field.DataType.Accept(creator);
                arrays.Add(creator.Array);
            }

            return new RecordBatch(schema, arrays, jsonRecordBatch.Count);
        }

        private static Schema CreateSchema(JsonSchema jsonSchema)
        {
            Schema.Builder builder = new Schema.Builder();
            for (int i = 0; i < jsonSchema.Fields.Count; i++)
            {
                builder.Field(f => CreateField(f, jsonSchema.Fields[i]));
            }
            return builder.Build();
        }

        private static void CreateField(Field.Builder builder, JsonField jsonField)
        {
            builder.Name(jsonField.Name)
                .DataType(ToArrowType(jsonField.Type))
                .Nullable(jsonField.Nullable);

            if (jsonField.Metadata != null)
            {
                builder.Metadata(jsonField.Metadata);
            }
        }

        private static IArrowType ToArrowType(JsonArrowType type)
        {
            return type.Name switch
            {
                "bool" => BooleanType.Default,
                "int" => ToIntArrowType(type),
                "floatingpoint" => ToFloatingPointArrowType(type),
                "binary" => BinaryType.Default,
                "utf8" => StringType.Default,
                "fixedsizebinary" => new FixedSizeBinaryType(type.ByteWidth),
                _ => throw new NotSupportedException($"JsonArrowType not supported: {type.Name}")
            };
        }

        private static IArrowType ToIntArrowType(JsonArrowType type)
        {
            return (type.BitWidth, type.IsSigned) switch
            {
                (8, true) => Int8Type.Default,
                (8, false) => UInt8Type.Default,
                (16, true) => Int16Type.Default,
                (16, false) => UInt16Type.Default,
                (32, true) => Int32Type.Default,
                (32, false) => UInt32Type.Default,
                (64, true) => Int64Type.Default,
                (64, false) => UInt64Type.Default,
                _ => throw new NotSupportedException($"Int type not supported: {type.BitWidth}, {type.IsSigned}")
            };
        }

        private static IArrowType ToFloatingPointArrowType(JsonArrowType type)
        {
            return type.Precision switch
            {
                "SINGLE" => FloatType.Default,
                "DOUBLE" => DoubleType.Default,
                _ => throw new NotSupportedException($"FloatingPoint type not supported: {type.Precision}")
            };
        }

        private class ArrayCreator :
            IArrowTypeVisitor<BooleanType>,
            IArrowTypeVisitor<Int8Type>,
            IArrowTypeVisitor<Int16Type>,
            IArrowTypeVisitor<Int32Type>,
            IArrowTypeVisitor<Int64Type>,
            IArrowTypeVisitor<UInt8Type>,
            IArrowTypeVisitor<UInt16Type>,
            IArrowTypeVisitor<UInt32Type>,
            IArrowTypeVisitor<UInt64Type>,
            IArrowTypeVisitor<FloatType>,
            IArrowTypeVisitor<DoubleType>,
            IArrowTypeVisitor<Decimal128Type>,
            IArrowTypeVisitor<Decimal256Type>,
            IArrowTypeVisitor<Date32Type>,
            IArrowTypeVisitor<Date64Type>,
            IArrowTypeVisitor<TimestampType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<BinaryType>,
            IArrowTypeVisitor<FixedSizeBinaryType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<StructType>
        {
            private JsonFieldData JsonFieldData { get; }
            public IArrowArray Array { get; private set; }

            public ArrayCreator(JsonFieldData jsonFieldData)
            {
                JsonFieldData = jsonFieldData;
            }

            public void Visit(BooleanType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);
                ArrowBuffer.BitmapBuilder valueBuilder = new ArrowBuffer.BitmapBuilder(validityBuffer.Length);

                var json = JsonFieldData.Data.GetRawText();
                bool[] values = JsonSerializer.Deserialize<bool[]>(json);

                foreach (bool value in values)
                {
                    valueBuilder.Append(value);
                }
                ArrowBuffer valueBuffer = valueBuilder.Build();

                Array = new BooleanArray(
                    valueBuffer, validityBuffer,
                    JsonFieldData.Count, nullCount, 0);
            }

            public void Visit(Int8Type type) => GenerateArray<sbyte, Int8Array>((v, n, c, nc, o) => new Int8Array(v, n, c, nc, o));
            public void Visit(Int16Type type) => GenerateArray<short, Int16Array>((v, n, c, nc, o) => new Int16Array(v, n, c, nc, o));
            public void Visit(Int32Type type) => GenerateArray<int, Int32Array>((v, n, c, nc, o) => new Int32Array(v, n, c, nc, o));
            public void Visit(Int64Type type) => GenerateLongArray<long, Int64Array>((v, n, c, nc, o) => new Int64Array(v, n, c, nc, o), s => long.Parse(s));
            public void Visit(UInt8Type type) => GenerateArray<byte, UInt8Array>((v, n, c, nc, o) => new UInt8Array(v, n, c, nc, o));
            public void Visit(UInt16Type type) => GenerateArray<ushort, UInt16Array>((v, n, c, nc, o) => new UInt16Array(v, n, c, nc, o));
            public void Visit(UInt32Type type) => GenerateArray<uint, UInt32Array>((v, n, c, nc, o) => new UInt32Array(v, n, c, nc, o));
            public void Visit(UInt64Type type) => GenerateLongArray<ulong, UInt64Array>((v, n, c, nc, o) => new UInt64Array(v, n, c, nc, o), s => ulong.Parse(s));
            public void Visit(FloatType type) => GenerateArray<float, FloatArray>((v, n, c, nc, o) => new FloatArray(v, n, c, nc, o));
            public void Visit(DoubleType type) => GenerateArray<double, DoubleArray>((v, n, c, nc, o) => new DoubleArray(v, n, c, nc, o));

            public void Visit(Decimal128Type type)
            {
                throw new NotImplementedException();
            }

            public void Visit(Decimal256Type type)
            {
                throw new NotImplementedException();
            }

            public void Visit(Date32Type type)
            {
                throw new NotImplementedException();
            }

            public void Visit(Date64Type type)
            {
                throw new NotImplementedException();
            }

            public void Visit(TimestampType type)
            {
                throw new NotImplementedException();
            }

            public void Visit(StringType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);
                ArrowBuffer offsetBuffer = GetOffsetBuffer();

                var json = JsonFieldData.Data.GetRawText();
                string[] values = JsonSerializer.Deserialize<string[]>(json, s_options);

                ArrowBuffer.Builder<byte> valueBuilder = new ArrowBuffer.Builder<byte>();
                foreach (string value in values)
                {
                    valueBuilder.Append(Encoding.UTF8.GetBytes(value));
                }
                ArrowBuffer valueBuffer = valueBuilder.Build(default);

                Array = new StringArray(JsonFieldData.Count, offsetBuffer, valueBuffer, validityBuffer, nullCount);
            }

            public void Visit(BinaryType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);
                ArrowBuffer offsetBuffer = GetOffsetBuffer();

                var json = JsonFieldData.Data.GetRawText();
                string[] values = JsonSerializer.Deserialize<string[]>(json, s_options);
                
                ArrowBuffer.Builder<byte> valueBuilder = new ArrowBuffer.Builder<byte>();
                foreach (string value in values)
                {
                    valueBuilder.Append(ConvertHexStringToByteArray(value));
                }
                ArrowBuffer valueBuffer = valueBuilder.Build(default);

                ArrayData arrayData = new ArrayData(type, JsonFieldData.Count, nullCount, 0, new[] { validityBuffer, offsetBuffer, valueBuffer });
                Array = new BinaryArray(arrayData);
            }

            public void Visit(FixedSizeBinaryType type)
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                var json = JsonFieldData.Data.GetRawText();
                string[] values = JsonSerializer.Deserialize<string[]>(json, s_options);

                ArrowBuffer.Builder<byte> valueBuilder = new ArrowBuffer.Builder<byte>();
                foreach (string value in values)
                {
                    valueBuilder.Append(ConvertHexStringToByteArray(value));
                }
                ArrowBuffer valueBuffer = valueBuilder.Build(default);

                ArrayData arrayData = new ArrayData(type, JsonFieldData.Count, nullCount, 0, new[] { validityBuffer, valueBuffer });
                Array = new FixedSizeBinaryArray(arrayData);
            }

            public void Visit(ListType type)
            {
                throw new NotImplementedException();
            }

            public void Visit(StructType type)
            {
                throw new NotImplementedException();
            }

            private static byte[] ConvertHexStringToByteArray(string hexString)
            {
                byte[] data = new byte[hexString.Length / 2];
                for (int index = 0; index < data.Length; index++)
                {
                    data[index] = byte.Parse(hexString.AsSpan(index * 2, 2) , NumberStyles.HexNumber, CultureInfo.InvariantCulture);
                }

                return data;
            }

            private static readonly JsonSerializerOptions s_options = new JsonSerializerOptions()
            {
                Converters =
                {
                    new ByteArrayConverter()
                }
            };

            private void GenerateArray<T, TArray>(Func<ArrowBuffer, ArrowBuffer, int, int, int, TArray> createArray)
                where TArray : PrimitiveArray<T>
                where T : struct
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                ArrowBuffer.Builder<T> valueBuilder = new ArrowBuffer.Builder<T>(JsonFieldData.Count);
                var json = JsonFieldData.Data.GetRawText();
                T[] values = JsonSerializer.Deserialize<T[]>(json, s_options);

                foreach (T value in values)
                {
                    valueBuilder.Append(value);
                }
                ArrowBuffer valueBuffer = valueBuilder.Build();

                Array = createArray(
                    valueBuffer, validityBuffer,
                    JsonFieldData.Count, nullCount, 0);
            }

            private void GenerateLongArray<T, TArray>(Func<ArrowBuffer, ArrowBuffer, int, int, int, TArray> createArray, Func<string, T> parse)
                where TArray : PrimitiveArray<T>
                where T : struct
            {
                ArrowBuffer validityBuffer = GetValidityBuffer(out int nullCount);

                ArrowBuffer.Builder<T> valueBuilder = new ArrowBuffer.Builder<T>(JsonFieldData.Count);
                var json = JsonFieldData.Data.GetRawText();
                string[] values = JsonSerializer.Deserialize<string[]>(json);

                foreach (string value in values)
                {
                    valueBuilder.Append(parse(value));
                }
                ArrowBuffer valueBuffer = valueBuilder.Build();

                Array = createArray(
                    valueBuffer, validityBuffer,
                    JsonFieldData.Count, nullCount, 0);
            }

            private ArrowBuffer GetOffsetBuffer()
            {
                ArrowBuffer.Builder<int> valueOffsets = new ArrowBuffer.Builder<int>(JsonFieldData.Offset.Length);
                valueOffsets.AppendRange(JsonFieldData.Offset);
                return valueOffsets.Build(default);
            }

            private ArrowBuffer GetValidityBuffer(out int nullCount)
            {
                if (JsonFieldData.Validity == null)
                {
                    nullCount = 0;
                    return ArrowBuffer.Empty;
                }

                ArrowBuffer.BitmapBuilder validityBuilder = new ArrowBuffer.BitmapBuilder(JsonFieldData.Validity.Length);
                validityBuilder.AppendRange(JsonFieldData.Validity);

                nullCount = validityBuilder.UnsetBitCount;
                return validityBuilder.Build();
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException($"{type.Name} not implemented");
            }
        }

        private Task StreamToFile()
        {
            return Task.CompletedTask;
        }

        private Task FileToStream()
        {
            return Task.CompletedTask;
        }

        private async ValueTask<JsonFile> ParseJsonFile()
        {
            using var fileStream = JsonFileInfo.OpenRead();
            JsonSerializerOptions options = new JsonSerializerOptions()
            {
                 PropertyNamingPolicy = JsonFileNamingPolicy.Instance,
            };
            options.Converters.Add(new ValidityConverter());

            return await JsonSerializer.DeserializeAsync<JsonFile>(fileStream, options);
        }
    }
}
