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
using System.Text;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Arrays.DictionaryArrays
{
    public class UInt64DictionaryArray : PrimitiveDictionaryArray<ulong>
    {
        public class Builder : PrimitiveDictionaryArrayBuilder<ulong, UInt64DictionaryArray, Builder>
        {
            public Builder() : base(null, null) { }

            public Builder(IEqualityComparer<ulong> comparer = null, HashFunctionDelegate hashFunc = null) : base(comparer, hashFunc)
            {
            }

            public override UInt64DictionaryArray Build(MemoryAllocator allocator)
            {
                allocator = allocator ?? MemoryAllocator.Default.Value;

                return new UInt64DictionaryArray(IndicesBuffer.Length, ValuesBuffer.Length, IndicesBuffer.Build(allocator), ValuesBuffer.Build(allocator),
                    ArrowBuffer.Empty);
            }
        }

        public UInt64DictionaryArray(ArrayData data, int uniqueValuesCount) : base(data, uniqueValuesCount)
        {
        }

        public UInt64DictionaryArray(int length, int uniqueValues, ArrowBuffer indices, ArrowBuffer dataBuffer, ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0) :
            this(new ArrayData(DictionaryType.Default(ArrowTypeId.UInt64), length, nullCount, offset, new[] { nullBitmapBuffer, indices, dataBuffer }), uniqueValues)
        {
        }
    }
}
