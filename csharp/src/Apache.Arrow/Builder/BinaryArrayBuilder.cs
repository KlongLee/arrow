﻿using System;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class BinaryArrayBuilder : VariableBinaryArrayBuilder
    {
        public BinaryArrayBuilder(int capacity = 32)
            : base(BinaryType.Default, capacity)
        {
        }

        internal BinaryArrayBuilder(IArrowType dtype, int capacity = 32)
            : base(dtype, capacity)
        {
        }

        public override IArrayBuilder AppendDotNet(DotNetScalar value) => AppendDotNet(value, true);
        public BinaryArrayBuilder AppendDotNet(DotNetScalar value, bool bin = true)
        {
            switch (value.ArrowType.TypeId)
            {
                case ArrowTypeId.Binary:
                    if (value.IsValid)
                        AppendValue(value.AsBytes());
                    else
                        AppendNull();
                    break;
                default:
                    throw new ArgumentException($"Cannot dynamically append values of type {value.DotNetType}");
            };
            return this;
        }

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public BinaryArray Build(MemoryAllocator allocator = default, bool bin = true)
            => new BinaryArray(FinishInternal(allocator));
    }

    public class StringArrayBuilder : BinaryArrayBuilder
    {
        public StringArrayBuilder(int capacity = 32)
            : this(StringType.Default, capacity)
        {
        }

        internal StringArrayBuilder(IArrowType dtype, int capacity = 32)
            : base(dtype, capacity)
        {
        }

        public override IArrayBuilder AppendDotNet(DotNetScalar value) => AppendDotNet(value, false, true);
        public StringArrayBuilder AppendDotNet(DotNetScalar value, bool bin = false, bool str = true)
        {
            Validate(value);

            switch (value.ArrowType.TypeId)
            {
                case ArrowTypeId.String:
                    AppendValue(value.ValueAs<string>());
                    break;
                default:
                    base.AppendDotNet(value);
                    break;
            };
            return this;
        }

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public StringArray Build(MemoryAllocator allocator = default, bool bin = false, bool str = true)
            => new StringArray(FinishInternal(allocator));
    }
}
