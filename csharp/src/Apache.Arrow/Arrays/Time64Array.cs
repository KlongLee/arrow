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
    /// The <see cref="Time64Array"/> class holds an array of longs, where each value is
    /// stored as the number of seconds/ milliseconds (depending on the Time64Type) since midnight.
    /// </summary>
    public class Time64Array : PrimitiveArray<long>
    {
        /// <summary>
        /// The <see cref="Builder"/> class can be used to fluently build <see cref="Time64Array"/> objects.
        /// </summary>
        public class Builder : PrimitiveArrayBuilder<long, long, Time64Array, Builder>
        {
            internal class Time64Builder : PrimitiveArrayBuilder<long, Time64Array, Time64Builder>
            {
                internal Time64Builder (Time64Type type)
                {
                    DataType = type ?? throw new ArgumentException(nameof(type));
                }

                protected Time64Type DataType { get; }

                protected override Time64Array Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new Time64Array(DataType, valueBuffer, nullBitmapBuffer, length, nullCount, offset);
            }

            protected Time64Type DataType { get; }

            public Builder()
                : this(Time64Type.Default) { }

            public Builder(TimeUnit unit)
                : this(new Time64Type(unit)) { }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class.
            /// </summary>
            public Builder(Time64Type type)
                : base(new Time64Builder(type))
            {
                DataType = type;
            }

            protected override long ConvertTo(long value)
            {
                // We must return the time since midnight in the specified unit
                // Since there is no conversion required, return it as-is

                return value;
            }
        }

        public Time64Array(
            Time64Type type,
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(type, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public Time64Array(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Time64);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);


        /// <summary>
        /// Get the time at the specified index as seconds
        /// </summary>
        /// <param name="index">Index at which to get the date.</param>
        /// <returns>Returns a long, or <c>null</c> if there is no object at that index.
        /// </returns>
        public long? GetSeconds(int index)
        {
            long? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time64Type) Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Second => value,
                TimeUnit.Millisecond => value / 1_000,
                TimeUnit.Microsecond => value / 1_000_000,
                TimeUnit.Nanosecond => value / 1_000_000_000,
                _ => throw new InvalidDataException($"Unsupported time unit for Time64Type: {unit}")
            };
        }

        /// <summary>
        /// Get the time at the specified index as milliseconds
        /// </summary>
        /// <param name="index">Index at which to get the date.</param>
        /// <returns>Returns a long, or <c>null</c> if there is no object at that index.
        /// </returns>
        public long? GetMilliSeconds(int index)
        {
            long? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time64Type)Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Second => value * 1_000,
                TimeUnit.Millisecond => value,
                TimeUnit.Microsecond => value / 1_000,
                TimeUnit.Nanosecond => value / 1_000_000,
                _ => throw new InvalidDataException($"Unsupported time unit for Time64Type: {unit}")
            };
        }

        /// <summary>
        /// Get the time at the specified index as microseconds
        /// </summary>
        /// <param name="index">Index at which to get the date.</param>
        /// <returns>Returns a long, or <c>null</c> if there is no object at that index.
        /// </returns>
        public long? GetMicroSeconds(int index)
        {
            long? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time64Type)Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Second => value * 1_000_000,
                TimeUnit.Millisecond => value * 1_000,
                TimeUnit.Microsecond => value,
                TimeUnit.Nanosecond => value / 1_000,
                _ => throw new InvalidDataException($"Unsupported time unit for Time64Type: {unit}")
            };
        }

        /// <summary>
        /// Get the time at the specified index as nanoseconds
        /// </summary>
        /// <param name="index">Index at which to get the date.</param>
        /// <returns>Returns a long, or <c>null</c> if there is no object at that index.
        /// </returns>
        public long? GetNanoSeconds(int index)
        {
            long? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time64Type)Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Second => value * 1_000_000_000,
                TimeUnit.Millisecond => value * 1_000_000,
                TimeUnit.Microsecond => value * 1_000,
                TimeUnit.Nanosecond => value,
                _ => throw new InvalidDataException($"Unsupported time unit for Time64Type: {unit}")
            };
        }
    }
}
