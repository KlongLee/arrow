﻿// Licensed to the Apache Software Foundation (ASF) under one or moreDate32Array
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

using Apache.Arrow.Types;
using System;
using System.IO;

namespace Apache.Arrow
{
    /// <summary>
    /// The <see cref="Time32Array"/> class holds an array of ints, where each value is
    /// stored as the number of seconds/ milliseconds (depending on the Time32Type) since midnight.
    /// </summary>
    public class Time32Array : PrimitiveArray<int>
    {
        private static readonly int seconds_in_millisecond = 1_000;
        /// <summary>
        /// The <see cref="Builder"/> class can be used to fluently build <see cref="Time32Array"/> objects.
        /// </summary>
        public class Builder : PrimitiveArrayBuilder<int, int, Time32Array, Builder>
        {
            internal class Time32Builder : PrimitiveArrayBuilder<int, Time32Array, Time32Builder>
            {
                internal Time32Builder (Time32Type type)
                {
                    DataType = type ?? throw new ArgumentException(nameof(type));
                }

                protected Time32Type DataType { get; }

                protected override Time32Array Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new Time32Array(DataType, valueBuffer, nullBitmapBuffer, length, nullCount, offset);
            }

            protected Time32Type DataType { get; }

            public Builder()
                : this(Time32Type.Default) { }

            public Builder(TimeUnit unit)
                : this(new Time32Type(unit)) { }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class.
            /// </summary>
            public Builder(Time32Type type)
                : base(new Time32Builder(type))
            {
                DataType = type;
            }

            protected override int ConvertTo(int value)
            {
                // We must return the time since midnight in the specified unit
                // Since there is no conversion required, return it as-is

                return value;
            }
        }

        public Time32Array(
            Time32Type type,
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(type, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public Time32Array(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Time32);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);


        /// <summary>
        /// Get the time at the specified index as seconds
        /// </summary>
        /// <param name="index">Index at which to get the date.</param>
        /// <returns>Returns an int, or <c>null</c> if there is no object at that index.
        /// </returns>
        public int? GetSeconds(int index)
        {
            int? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time32Type) Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Second => value,
                TimeUnit.Millisecond => value / seconds_in_millisecond,
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {unit}")
            };
        }

        /// <summary>
        /// Get the time at the specified index as milliseconds
        /// </summary>
        /// <param name="index">Index at which to get the date.</param>
        /// <returns>Returns an int, or <c>null</c> if there is no object at that index.
        /// </returns>
        public int? GetMilliSeconds(int index)
        {
            int? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time32Type)Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Second => value * seconds_in_millisecond,
                TimeUnit.Millisecond => value,
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {unit}")
            };
        }
    }
}
