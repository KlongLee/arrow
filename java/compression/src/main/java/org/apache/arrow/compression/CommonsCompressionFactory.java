/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.compression;

import org.apache.arrow.flatbuf.CompressionType;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.NoCompressionCodec;

/**
 * A factory implementation based on Apache Commons library.
 */
public class CommonsCompressionFactory implements CompressionCodec.Factory {

  @Override
  public CompressionCodec createCodec(byte codecType) {
    switch (codecType) {
      case NoCompressionCodec.COMPRESSION_TYPE:
        return NoCompressionCodec.INSTANCE;
      case CompressionType.LZ4_FRAME:
        return new Lz4CompressionCodec();
      default:
        throw new IllegalArgumentException("Compression type not supported: " + codecType);
    }
  }

}
