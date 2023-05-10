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

namespace Apache.Arrow.Types
{
    public sealed class Decimal128Type : FixedSizeBinaryType, IDecimalType
    {
        // Arbitrary chose to max byte encoding
        public static readonly Decimal128Type Default = new(23, 9);
        public override ArrowTypeId TypeId => ArrowTypeId.Decimal128;
        public override string Name => "decimal128";

        public int Precision { get; }
        public int Scale { get; }

        public Decimal128Type(int precision, int scale)
            : base(16)
        {
            Precision = precision;
            Scale = scale;
        }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);
    }
}
