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

using Apache.Arrow.Types;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Apache.Arrow
{
    public class TimestampArray: PrimitiveArray<long>
    {
        private static readonly DateTimeOffset s_epoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        public class Builder: PrimitiveArrayBuilder<DateTimeOffset, long, TimestampArray, Builder>
        {
            internal class TimestampBuilder : PrimitiveArrayBuilder<long, TimestampArray, TimestampBuilder>
            {
                internal TimestampBuilder(TimestampType type)
                {
                    DataType = type ?? throw new ArgumentNullException(nameof(type));
                }

                protected override TimestampArray Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new TimestampArray(DataType as TimestampType, valueBuffer, nullBitmapBuffer,
                        length, nullCount, offset);
            }

            public TimestampType TimeType => DataType as TimestampType;

            public Builder()
                : this(TimestampType.Default) { }

            public Builder(TimeUnit unit, TimeZoneInfo timezone)
                : this(new TimestampType(unit, timezone)) { }

            public Builder(TimeUnit unit = TimeUnit.Millisecond, string timezone = "+00:00")
                : this(new TimestampType(unit, timezone)) { }

            public Builder(TimeUnit unit)
                : this(new TimestampType(unit, (string) null)) { }

            public Builder(TimestampType type)
                : base(new TimestampBuilder(type))
            {
            }

            public new Builder AppendRange(IEnumerable<DateTimeOffset> values)
            {
                switch (TimeType.Unit)
                {
                    case TimeUnit.Second:
                        ArrayBuilder.AppendRange(values.Select(value => value.ToUnixTimeSeconds()));
                        break;
                    case TimeUnit.Millisecond:
                        ArrayBuilder.AppendRange(values.Select(value => value.ToUnixTimeMilliseconds()));
                        break;
                    case TimeUnit.Microsecond:
                        ArrayBuilder.AppendRange(values.Select(value => value.ToUnixTimeMicroseconds()));
                        break;
                    case TimeUnit.Nanosecond:
                        ArrayBuilder.AppendRange(values.Select(value => value.ToUnixTimeNanoseconds()));
                        break;
                    default:
                        throw new InvalidOperationException($"unsupported time unit <{TimeType.Unit}>");
                }

                return this;
            }

            public Builder AppendRange(IEnumerable<DateTimeOffset?> values)
            {
                switch (TimeType.Unit)
                {
                    case TimeUnit.Second:
                        foreach (DateTimeOffset? value in values)
                        {
                            if (value.HasValue)
                                ArrayBuilder.Append(value.Value.ToUnixTimeSeconds());
                            else
                                ArrayBuilder.AppendNull();
                        }
                        break;
                    case TimeUnit.Millisecond:
                        foreach (DateTimeOffset? value in values)
                        {
                            if (value.HasValue)
                                ArrayBuilder.Append(value.Value.ToUnixTimeMilliseconds());
                            else
                                ArrayBuilder.AppendNull();
                        }
                        break;
                    case TimeUnit.Microsecond:
                        foreach (DateTimeOffset? value in values)
                        {
                            if (value.HasValue)
                                ArrayBuilder.Append(value.Value.ToUnixTimeMicroseconds());
                            else
                                ArrayBuilder.AppendNull();
                        }
                        break;
                    case TimeUnit.Nanosecond:
                        foreach (DateTimeOffset? value in values)
                        {
                            if (value.HasValue)
                                ArrayBuilder.Append(value.Value.ToUnixTimeNanoseconds());
                            else
                                ArrayBuilder.AppendNull();
                        }
                        break;
                    default:
                        throw new InvalidOperationException($"unsupported time unit <{TimeType.Unit}>");
                }

                return this;
            }

            protected override long ConvertTo(DateTimeOffset value)
            {
                // We must return the absolute time since the UNIX epoch while
                // respecting the timezone offset; the calculation is as follows:
                //
                // - Compute time span between epoch and specified time
                // - Compute time divisions per tick

                switch (TimeType.Unit)
                {
                    case TimeUnit.Nanosecond:
                        return value.ToUnixTimeNanoseconds();
                    case TimeUnit.Microsecond:
                        return value.ToUnixTimeMicroseconds();
                    case TimeUnit.Millisecond:
                        return value.ToUnixTimeMilliseconds();
                    case TimeUnit.Second:
                        return value.ToUnixTimeSeconds();
                    default:
                        throw new InvalidOperationException($"unsupported time unit <{TimeType.Unit}>");
                }
            }
        }

        public TimestampArray(
            TimestampType type,
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(type, length, nullCount, offset,
                new[] {nullBitmapBuffer, valueBuffer})) { }

        public TimestampArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Timestamp);

            Debug.Assert(Data.DataType is TimestampType);
        }

        public TimestampType TimeType => Data.DataType as TimestampType;

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public DateTimeOffset GetTimestampUnchecked(int index)
        {
            long value = Values[index];

            long ticks;

            switch (TimeType.Unit)
            {
                case TimeUnit.Nanosecond:
                    ticks = value / 100;
                    break;
                case TimeUnit.Microsecond:
                    ticks = value * 10;
                    break;
                case TimeUnit.Millisecond:
                    ticks = value * TimeSpan.TicksPerMillisecond;
                    break;
                case TimeUnit.Second:
                    ticks = value * TimeSpan.TicksPerSecond;
                    break;
                default:
                    throw new InvalidDataException(
                        $"Unsupported timestamp unit <{TimeType.Unit}>");
            }

            return new DateTimeOffset(s_epoch.Ticks + ticks, TimeSpan.Zero);
        }

        public DateTimeOffset? GetTimestamp(int index)
        {
            if (IsNull(index))
            {
                return null;
            }

            return GetTimestampUnchecked(index);
        }

        public new DateTimeOffset?[] ToArray()
        {
            ReadOnlySpan<long> span = Values;
            DateTimeOffset?[] alloc = new DateTimeOffset?[Length];

            // Initialize the values
            switch(TimeType.Unit)
            {
                case TimeUnit.Second:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = IsValid(i) ? DateTimeOffset.FromUnixTimeSeconds(span[i]) : null;
                    }
                    break;
                case TimeUnit.Millisecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = IsValid(i) ? DateTimeOffset.FromUnixTimeMilliseconds(span[i]) : null;
                    }
                    break;
                case TimeUnit.Microsecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = IsValid(i) ? DateTimeOffsetExtensions.FromUnixTimeMicroseconds(span[i]) : null;
                    }
                    break;
                case TimeUnit.Nanosecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = IsValid(i) ? DateTimeOffsetExtensions.FromUnixTimeNanoseconds(span[i]) : null;
                    }
                    break;
                default:
                    throw new InvalidDataException($"Unsupported time unit for TimestampType: {TimeType.Unit}");
            }

            return alloc;
        }

        public new DateTimeOffset[] ToArray(bool nullable = false)
        {
            ReadOnlySpan<long> span = Values;
            DateTimeOffset[] alloc = new DateTimeOffset[Length];

            // Initialize the values
            // Initialize the values
            switch (TimeType.Unit)
            {
                case TimeUnit.Second:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = DateTimeOffset.FromUnixTimeSeconds(span[i]);
                    }
                    break;
                case TimeUnit.Millisecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = DateTimeOffset.FromUnixTimeMilliseconds(span[i]);
                    }
                    break;
                case TimeUnit.Microsecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = DateTimeOffsetExtensions.FromUnixTimeMicroseconds(span[i]);
                    }
                    break;
                case TimeUnit.Nanosecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = DateTimeOffsetExtensions.FromUnixTimeNanoseconds(span[i]);
                    }
                    break;
                default:
                    throw new InvalidDataException($"Unsupported time unit for TimestampType: {TimeType.Unit}");
            }

            return alloc;
        }
    }
}
