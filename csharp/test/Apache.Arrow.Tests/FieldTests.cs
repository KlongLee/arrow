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
using System.Collections;
using System.Collections.Generic;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class FieldTests
    {
        public class Build
        {
            [Fact]
            public void DataType_Should_ThrowInvalidCastException()
            {
                // Arrange
                var builder = new Field.Builder().Name("test");

                // Act & Assert
                try
                {
                    builder.DataType(typeof(object));
                }
                catch (InvalidCastException e)
                {
                    Assert.Equal($"Cannot convert System.Type<{typeof(object)}> to ArrowType", e.Message);
                }
            }

            [Fact]
            public void DataType_Should_InferDataType_From_NullableInt()
            {
                // Arrange
                Field builder = new Field.Builder().Name("test").DataType(typeof(int?)).Build();

                // Assert
                Assert.Equal(typeof(Int32Type), builder.DataType.GetType());
                Assert.True(builder.IsNullable);
            }

            [Fact]
            public void DataType_Should_InferDataType_From_Int()
            {
                // Arrange
                Field builder = new Field.Builder().Name("test").DataType(typeof(int)).Build();

                // Assert
                Assert.Equal(typeof(Int32Type), builder.DataType.GetType());
                Assert.False(builder.IsNullable);
            }

            [Fact]
            public void DataType_Should_InferDataType_From_NullableDecimal()
            {
                // Arrange
                Field builder = new Field.Builder().Name("test").DataType(typeof(decimal?)).Build();
                var dtype = builder.DataType as Decimal128Type;

                // Assert
                Assert.Equal(typeof(Decimal128Type), builder.DataType.GetType());
                Assert.Equal(38, dtype.Precision);
                Assert.Equal(18, dtype.Scale);
                Assert.True(builder.IsNullable);
            }

            [Fact]
            public void DataType_Should_InferDataType_From_Decimal()
            {
                // Arrange
                Field builder = new Field.Builder().Name("test").DataType(typeof(decimal)).Build();
                var dtype = builder.DataType as Decimal128Type;

                // Assert
                Assert.Equal(typeof(Decimal128Type), builder.DataType.GetType());
                Assert.Equal(38, dtype.Precision);
                Assert.Equal(18, dtype.Scale);
                Assert.False(builder.IsNullable);
            }

# if NETCOREAPP
            [Fact]
            public void DataType_Should_InferDataType_From_IEnumerable()
            {
                // Arrange
                Field builder = new Field.Builder().Name("test").DataType(typeof(IEnumerable<string>)).Build();
                var dtype = builder.DataType as ListType;
                Field child = dtype.Fields[0];

                // Assert
                Assert.Equal(typeof(ListType), builder.DataType.GetType());
                Assert.Equal(typeof(StringType), child.DataType.GetType());
                Assert.True(dtype.Fields[0].IsNullable);
            }
# endif
        }
    }
}
