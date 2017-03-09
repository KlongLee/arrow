/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.file.ArrowFileWriter;
import org.apache.arrow.vector.stream.ArrowStreamReader;

/**
 * Converts an Arrow stream to an Arrow file.
 */
public class StreamToFile {
  public static void convert(InputStream in, OutputStream out) throws IOException {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
      try (ArrowFileWriter writer = new ArrowFileWriter(reader.getVectorSchemaRoot(), reader, Channels.newChannel(out))) {
        writer.start();
        while (true) {
          reader.loadNextBatch();
          if (reader.getVectorSchemaRoot().getRowCount() == 0) {
            break;
          }
          writer.writeBatch();
        }
        writer.end();
      }
    }
  }

  public static void main(String[] args) throws IOException {
    InputStream in = System.in;
    OutputStream out = System.out;
    if (args.length == 2) {
      in = new FileInputStream(new File(args[0]));
      out = new FileOutputStream(new File(args[1]));
    }
    convert(in, out);
  }
}
