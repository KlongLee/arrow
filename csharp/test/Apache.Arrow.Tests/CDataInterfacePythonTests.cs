// Licensed to the Apache Software Foundation (ASF) under one or more
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
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Python.Runtime;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class CDataSchemaPythonTest
    {
        public CDataSchemaPythonTest()
        {
            bool inCIJob = Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true";
            bool inVerificationJob = Environment.GetEnvironmentVariable("TEST_CSHARP") == "1";
            bool pythonSet = Environment.GetEnvironmentVariable("PYTHONNET_PYDLL") != null;
            // We only skip if this is not in CI
            if (inCIJob && !inVerificationJob && !pythonSet)
            {
                throw new Exception("PYTHONNET_PYDLL not set; skipping C Data Interface tests.");
            }
            else
            {
                Skip.If(!pythonSet, "PYTHONNET_PYDLL not set; skipping C Data Interface tests.");
            }


            PythonEngine.Initialize();

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
                !PythonEngine.PythonPath.Contains("dlls", StringComparison.OrdinalIgnoreCase))
            {
                dynamic sys = Py.Import("sys");
                sys.path.append(Path.Combine(Path.GetDirectoryName(Environment.GetEnvironmentVariable("PYTHONNET_PYDLL")), "DLLs"));
            }
        }

        private static Schema GetTestSchema()
        {
            using (Py.GIL())
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("null").DataType(NullType.Default).Nullable(true))
                    .Field(f => f.Name("bool").DataType(BooleanType.Default).Nullable(true))
                    .Field(f => f.Name("i8").DataType(Int8Type.Default).Nullable(true))
                    .Field(f => f.Name("u8").DataType(UInt8Type.Default).Nullable(true))
                    .Field(f => f.Name("i16").DataType(Int16Type.Default).Nullable(true))
                    .Field(f => f.Name("u16").DataType(UInt16Type.Default).Nullable(true))
                    .Field(f => f.Name("i32").DataType(Int32Type.Default).Nullable(true))
                    .Field(f => f.Name("u32").DataType(UInt32Type.Default).Nullable(true))
                    .Field(f => f.Name("i64").DataType(Int64Type.Default).Nullable(true))
                    .Field(f => f.Name("u64").DataType(UInt64Type.Default).Nullable(true))

                    .Field(f => f.Name("f16").DataType(HalfFloatType.Default).Nullable(true))
                    .Field(f => f.Name("f32").DataType(FloatType.Default).Nullable(true))
                    .Field(f => f.Name("f64").DataType(DoubleType.Default).Nullable(true))

                    .Field(f => f.Name("decimal128_19_3").DataType(new Decimal128Type(19, 3)).Nullable(true))
                    .Field(f => f.Name("decimal256_19_3").DataType(new Decimal256Type(19, 3)).Nullable(true))
                    .Field(f => f.Name("decimal256_40_2").DataType(new Decimal256Type(40, 2)).Nullable(false))

                    .Field(f => f.Name("binary").DataType(BinaryType.Default).Nullable(false))
                    .Field(f => f.Name("string").DataType(StringType.Default).Nullable(false))
                    .Field(f => f.Name("fw_binary_10").DataType(new FixedSizeBinaryType(10)).Nullable(false))

                    .Field(f => f.Name("date32").DataType(Date32Type.Default).Nullable(false))
                    .Field(f => f.Name("date64").DataType(Date64Type.Default).Nullable(false))
                    .Field(f => f.Name("time32_s").DataType(new Time32Type(TimeUnit.Second)).Nullable(false))
                    .Field(f => f.Name("time32_ms").DataType(new Time32Type(TimeUnit.Millisecond)).Nullable(false))
                    .Field(f => f.Name("time64_us").DataType(new Time64Type(TimeUnit.Microsecond)).Nullable(false))
                    .Field(f => f.Name("time64_ns").DataType(new Time64Type(TimeUnit.Nanosecond)).Nullable(false))

                    .Field(f => f.Name("timestamp_ns").DataType(new TimestampType(TimeUnit.Nanosecond, "")).Nullable(false))
                    .Field(f => f.Name("timestamp_us").DataType(new TimestampType(TimeUnit.Microsecond, "")).Nullable(false))
                    .Field(f => f.Name("timestamp_us_paris").DataType(new TimestampType(TimeUnit.Microsecond, "Europe/Paris")).Nullable(true))

                    .Field(f => f.Name("list_string").DataType(new ListType(StringType.Default)).Nullable(false))
                    .Field(f => f.Name("list_list_i32").DataType(new ListType(new ListType(Int32Type.Default))).Nullable(false))

                    .Field(f => f.Name("dict_string").DataType(new DictionaryType(Int32Type.Default, StringType.Default, false)).Nullable(false))
                    .Field(f => f.Name("dict_string_ordered").DataType(new DictionaryType(Int32Type.Default, StringType.Default, true)).Nullable(false))
                    .Field(f => f.Name("list_dict_string").DataType(new ListType(new DictionaryType(Int32Type.Default, StringType.Default, false))).Nullable(false))

                    // Checking wider characters.
                    .Field(f => f.Name("hello 你好 😄").DataType(BooleanType.Default).Nullable(true))

                    .Build();
                return schema;
            }
        }

        private static IEnumerable<dynamic> GetPythonFields()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                yield return pa.field("null", pa.GetAttr("null").Invoke(), true);
                yield return pa.field("bool", pa.bool_(), true);
                yield return pa.field("i8", pa.int8(), true);
                yield return pa.field("u8", pa.uint8(), true);
                yield return pa.field("i16", pa.int16(), true);
                yield return pa.field("u16", pa.uint16(), true);
                yield return pa.field("i32", pa.int32(), true);
                yield return pa.field("u32", pa.uint32(), true);
                yield return pa.field("i64", pa.int64(), true);
                yield return pa.field("u64", pa.uint64(), true);

                yield return pa.field("f16", pa.float16(), true);
                yield return pa.field("f32", pa.float32(), true);
                yield return pa.field("f64", pa.float64(), true);

                yield return pa.field("decimal128_19_3", pa.decimal128(19, 3), true);
                yield return pa.field("decimal256_19_3", pa.decimal256(19, 3), true);
                yield return pa.field("decimal256_40_2", pa.decimal256(40, 2), false);

                yield return pa.field("binary", pa.binary(), false);
                yield return pa.field("string", pa.utf8(), false);
                yield return pa.field("fw_binary_10", pa.binary(10), false);

                yield return pa.field("date32", pa.date32(), false);
                yield return pa.field("date64", pa.date64(), false);
                yield return pa.field("time32_s", pa.time32("s"), false);
                yield return pa.field("time32_ms", pa.time32("ms"), false);
                yield return pa.field("time64_us", pa.time64("us"), false);
                yield return pa.field("time64_ns", pa.time64("ns"), false);

                yield return pa.field("timestamp_ns", pa.timestamp("ns"), false);
                yield return pa.field("timestamp_us", pa.timestamp("us"), false);
                yield return pa.field("timestamp_us_paris", pa.timestamp("us", "Europe/Paris"), true);

                yield return pa.field("list_string", pa.list_(pa.utf8()), false);
                yield return pa.field("list_list_i32", pa.list_(pa.list_(pa.int32())), false);

                yield return pa.field("dict_string", pa.dictionary(pa.int32(), pa.utf8(), false), false);
                yield return pa.field("dict_string_ordered", pa.dictionary(pa.int32(), pa.utf8(), true), false);
                yield return pa.field("list_dict_string", pa.list_(pa.dictionary(pa.int32(), pa.utf8(), false)), false);

                yield return pa.field("hello 你好 😄", pa.bool_(), true);
            }
        }

        private static dynamic GetPythonSchema()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                return pa.schema(GetPythonFields().ToList());
            }
        }

        private IArrowArray GetTestArray()
        {
            var builder = new StringArray.Builder();
            builder.Append("hello");
            builder.Append("world");
            builder.AppendNull();
            builder.Append("foo");
            builder.Append("bar");
            return builder.Build();
        }

        private dynamic GetPythonArray()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                return pa.array(new[] { "hello", "world", null, "foo", "bar" });
            }
        }

        private RecordBatch GetTestRecordBatch()
        {
            Field[] fields = new[]
            {
                new Field("col1", Int64Type.Default, true),
                new Field("col2", StringType.Default, true),
                new Field("col3", DoubleType.Default, true),
            };
            return new RecordBatch(
                new Schema(fields, null),
                new IArrowArray[]
                {
                    new Int64Array.Builder().AppendRange(new long[] { 1, 2, 3 }).AppendNull().Append(5).Build(),
                    GetTestArray(),
                    new DoubleArray.Builder().AppendRange(new double[] { 0.0, 1.4, 2.5, 3.6, 4.7 }).Build(),
                },
                5);
        }

        private dynamic GetPythonRecordBatch()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic table = pa.table(
                    new PyList(new PyObject[]
                    {
                        pa.array(new long?[] { 1, 2, 3, null, 5 }),
                        pa.array(new[] { "hello", "world", null, "foo", "bar" }),
                        pa.array(new[] { 0.0, 1.4, 2.5, 3.6, 4.7 })
                    }),
                    new[] { "col1", "col2", "col3" });

                return table.to_batches()[0];
            }
        }

        private IArrowArrayStream GetTestArrayStream()
        {
            RecordBatch recordBatch = GetTestRecordBatch();
            return new TestArrayStream(recordBatch.Schema, recordBatch);
        }

        private dynamic GetPythonRecordBatchReader()
        {
            dynamic recordBatch = GetPythonRecordBatch();
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                return pa.RecordBatchReader.from_batches(recordBatch.schema, new PyList(new PyObject[] { recordBatch }));
            }
        }

        // Schemas created in Python, used in CSharp
        [SkippableFact]
        public unsafe void ImportType()
        {
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.FieldsList
                .Zip(pyFields))
            {
                CArrowSchema* cSchema = CArrowSchema.Create();

                using (Py.GIL())
                {
                    dynamic pyDatatype = pyField.type;
                    // Python expects the pointer as an integer
                    long longPtr = ((IntPtr)cSchema).ToInt64();
                    pyDatatype._export_to_c(longPtr);
                }

                var dataTypeComparer = new ArrayTypeComparer(field.DataType);
                ArrowType importedType = CArrowSchemaImporter.ImportType(cSchema);
                dataTypeComparer.Visit(importedType);

                if (importedType is DictionaryType importedDictType)
                {
                    Assert.Equal(((DictionaryType)field.DataType).Ordered, importedDictType.Ordered);
                }

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.Free(cSchema);
            }
        }

        [SkippableFact]
        public unsafe void ImportField()
        {
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.FieldsList
                .Zip(pyFields))
            {
                CArrowSchema* cSchema = CArrowSchema.Create();

                using (Py.GIL())
                {
                    long longPtr = ((IntPtr)cSchema).ToInt64();
                    pyField._export_to_c(longPtr);
                }

                Field importedField = CArrowSchemaImporter.ImportField(cSchema);
                FieldComparer.Compare(field, importedField);

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.Free(cSchema);
            }
        }

        [SkippableFact]
        public unsafe void ImportSchema()
        {
            Schema schema = GetTestSchema();
            dynamic pySchema = GetPythonSchema();

            CArrowSchema* cSchema = CArrowSchema.Create();

            using (Py.GIL())
            {
                long longPtr = ((IntPtr)cSchema).ToInt64();
                pySchema._export_to_c(longPtr);
            }

            Schema importedSchema = CArrowSchemaImporter.ImportSchema(cSchema);
            SchemaComparer.Compare(schema, importedSchema);

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowSchema.Free(cSchema);
        }


        // Schemas created in CSharp, exported to Python
        [SkippableFact]
        public unsafe void ExportType()
        {
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.FieldsList
                .Zip(pyFields))
            {
                IArrowType datatype = field.DataType;
                CArrowSchema* cSchema = CArrowSchema.Create();
                CArrowSchemaExporter.ExportType(datatype, cSchema);

                // For Python, we need to provide the pointer
                long longPtr = ((IntPtr)cSchema).ToInt64();

                using (Py.GIL())
                {
                    dynamic pa = Py.Import("pyarrow");
                    dynamic expectedPyType = pyField.type;
                    dynamic exportedPyType = pa.DataType._import_from_c(longPtr);
                    Assert.True(exportedPyType == expectedPyType);

                    if (pa.types.is_dictionary(exportedPyType))
                    {
                        Assert.Equal(expectedPyType.ordered, exportedPyType.ordered);
                    }
                }

                // Python should have called release once `exportedPyType` went out-of-scope.
                Assert.True(cSchema->release == null);
                Assert.True(cSchema->format == null);
                Assert.Equal(0, cSchema->flags);
                Assert.Equal(0, cSchema->n_children);
                Assert.True(cSchema->dictionary == null);

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.Free(cSchema);
            }
        }

        [SkippableFact]
        public unsafe void ExportField()
        {
            Schema schema = GetTestSchema();
            IEnumerable<dynamic> pyFields = GetPythonFields();

            foreach ((Field field, dynamic pyField) in schema.FieldsList
                .Zip(pyFields))
            {
                CArrowSchema* cSchema = CArrowSchema.Create();
                CArrowSchemaExporter.ExportField(field, cSchema);

                // For Python, we need to provide the pointer
                long longPtr = ((IntPtr)cSchema).ToInt64();

                using (Py.GIL())
                {
                    dynamic pa = Py.Import("pyarrow");
                    dynamic exportedPyField = pa.Field._import_from_c(longPtr);
                    Assert.True(exportedPyField == pyField);
                }

                // Python should have called release once `exportedPyField` went out-of-scope.
                Assert.True(cSchema->name == null);
                Assert.True(cSchema->release == null);
                Assert.True(cSchema->format == null);

                // Since we allocated, we are responsible for freeing the pointer.
                CArrowSchema.Free(cSchema);
            }
        }

        [SkippableFact]
        public unsafe void ExportSchema()
        {
            Schema schema = GetTestSchema();
            dynamic pySchema = GetPythonSchema();

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(schema, cSchema);

            // For Python, we need to provide the pointer
            long longPtr = ((IntPtr)cSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPySchema = pa.Schema._import_from_c(longPtr);
                Assert.True(exportedPySchema == pySchema);
            }

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowSchema.Free(cSchema);
        }

        [SkippableFact]
        public unsafe void ImportArray()
        {
            CArrowArray* cArray = CArrowArray.Create();
            CArrowSchema* cSchema = CArrowSchema.Create();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic array = pa.array(new[] { "hello", "world", null, "foo", "bar" });

                long arrayPtr = ((IntPtr)cArray).ToInt64();
                long schemaPtr = ((IntPtr)cSchema).ToInt64();
                array._export_to_c(arrayPtr, schemaPtr);
            }

            ArrowType type = CArrowSchemaImporter.ImportType(cSchema);
            IArrowArray importedArray = CArrowArrayImporter.ImportArray(cArray, type);

            Assert.Equal(5, importedArray.Length);
        }

        [SkippableFact]
        public unsafe void ImportRecordBatch()
        {
            CArrowArray* cArray = CArrowArray.Create();
            CArrowSchema* cSchema = CArrowSchema.Create();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic table = pa.table(
                    new PyList(new PyObject[]
                    {
                        pa.array(new long?[] { 1, 2, 3, null, 5 }),
                        pa.array(new[] { "hello", "world", null, "foo", "bar" }),
                        pa.array(new[] { 0.0, 1.4, 2.5, 3.6, 4.7 })
                    }),
                    new[] { "col1", "col2", "col3" });

                dynamic batch = table.to_batches()[0];

                long batchPtr = ((IntPtr)cArray).ToInt64();
                long schemaPtr = ((IntPtr)cSchema).ToInt64();
                batch._export_to_c(batchPtr, schemaPtr);
            }

            Schema schema = CArrowSchemaImporter.ImportSchema(cSchema);
            RecordBatch recordBatch = CArrowArrayImporter.ImportRecordBatch(cArray, schema);

            Assert.Equal(5, recordBatch.Length);
        }

        [SkippableFact]
        public unsafe void ImportArrayStream()
        {
            CArrowArrayStream* cArrayStream = CArrowArrayStream.Create();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic table = pa.table(
                    new PyList(new PyObject[]
                    {
                        pa.array(new long?[] { 1, 2, 3, null, 5 }),
                        pa.array(new[] { "hello", "world", null, "foo", "bar" }),
                        pa.array(new[] { 0.0, 1.4, 2.5, 3.6, 4.7 })
                    }),
                    new[] { "col1", "col2", "col3" });

                dynamic batchReader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches());

                long streamPtr = ((IntPtr)cArrayStream).ToInt64();
                batchReader._export_to_c(streamPtr);
            }

            IArrowArrayStream stream = CArrowArrayStreamImporter.ImportArrayStream(cArrayStream);
            var batch1 = stream.ReadNextRecordBatchAsync().Result;
            Assert.Equal(5, batch1.Length);

            var batch2 = stream.ReadNextRecordBatchAsync().Result;
            Assert.Null(batch2);
        }

        [SkippableFact]
        public unsafe void ExportArray()
        {
            IArrowArray array = GetTestArray();
            dynamic pyArray = GetPythonArray();

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(array, cArray);

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportType(array.Data.DataType, cSchema);

            // For Python, we need to provide the pointers
            long arrayPtr = ((IntPtr)cArray).ToInt64();
            long schemaPtr = ((IntPtr)cSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyArray = pa.Array._import_from_c(arrayPtr, schemaPtr);
                Assert.True(exportedPyArray == pyArray);
            }

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowArray.Free(cArray);
            CArrowSchema.Free(cSchema);
        }

        [SkippableFact]
        public unsafe void ExportBatch()
        {
            RecordBatch batch = GetTestRecordBatch();
            dynamic pyBatch = GetPythonRecordBatch();

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(batch.Schema, cSchema);

            // For Python, we need to provide the pointers
            long arrayPtr = ((IntPtr)cArray).ToInt64();
            long schemaPtr = ((IntPtr)cSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyArray = pa.RecordBatch._import_from_c(arrayPtr, schemaPtr);
                Assert.True(exportedPyArray == pyBatch);
            }

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowArray.Free(cArray);
            CArrowSchema.Free(cSchema);
        }

        [SkippableFact]
        public unsafe void ExportBatchReader()
        {
            RecordBatch batch = GetTestRecordBatch();
            dynamic pyBatch = GetPythonRecordBatch();

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(batch.Schema, cSchema);

            // For Python, we need to provide the pointers
            long arrayPtr = ((IntPtr)cArray).ToInt64();
            long schemaPtr = ((IntPtr)cSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyArray = pa.RecordBatch._import_from_c(arrayPtr, schemaPtr);
                Assert.True(exportedPyArray == pyBatch);
            }

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowArray.Free(cArray);
            CArrowSchema.Free(cSchema);
        }

        [SkippableFact]
        public unsafe void ExportArrayStream()
        {
            IArrowArrayStream arrayStream = GetTestArrayStream();
            dynamic pyRecordBatchReader = GetPythonRecordBatchReader();

            CArrowArrayStream* cArrayStream = CArrowArrayStream.Create();
            CArrowArrayStreamExporter.ExportArrayStream(arrayStream, cArrayStream);

            // For Python, we need to provide the pointers
            long arrayStreamPtr = ((IntPtr)cArrayStream).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyReader = pa.RecordBatchReader._import_from_c(arrayStreamPtr);
                Assert.True(exportedPyReader.read_all() == pyRecordBatchReader.read_all());
            }

            // Since we allocated, we are responsible for freeing the pointer.
            CArrowArrayStream.Free(cArrayStream);
        }

        sealed class TestArrayStream : IArrowArrayStream
        {
            private readonly RecordBatch[] _batches;
            private int _index;

            public TestArrayStream(Schema schema, params RecordBatch[] batches)
            {
                Schema = schema;
                _batches = batches;
            }

            public Schema Schema { get; }

            public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_index < 0) { throw new ObjectDisposedException(nameof(TestArrayStream)); }

                RecordBatch result = _index < _batches.Length ? _batches[_index++] : null;
                return new ValueTask<RecordBatch>(result);
            }

            public void Dispose()
            {
                _index = -1;
            }
        }
    }
}
