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
using Apache.Arrow.Ipc;
using CommunityToolkit.HighPerformance;
using K4os.Compression.LZ4.Streams;

namespace Apache.Arrow.Compression
{
    internal sealed class Lz4CompressionCodec : ICompressionCodec
    {
        /// <summary>
        /// Singleton instance, used as this class doesn't need to be disposed and has no state
        /// </summary>
        public static readonly Lz4CompressionCodec Instance = new Lz4CompressionCodec();

        public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
        {
            using var sourceStream = source.AsStream();
            using var destStream = destination.AsStream();
            using var decompressedStream = LZ4Stream.Decode(sourceStream);
            decompressedStream.CopyTo(destStream);
            return (int) destStream.Length;
        }

        public void Dispose()
        {
        }
    }
}
