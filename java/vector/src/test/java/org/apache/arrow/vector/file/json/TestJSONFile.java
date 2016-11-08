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
package org.apache.arrow.vector.file.json;

import java.io.File;
import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.file.BaseFileTest;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJSONFile extends BaseFileTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestJSONFile.class);

  @Test
  public void testWriteReadComplexJSON() throws IOException {
    File file = new File("target/mytest_complex.json");
    int count = COUNT;

    // write
    try (
        BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", originalVectorAllocator, null)) {
      writeComplexData(count, parent);
      writeJSON(file, new VectorSchemaRoot(parent.getChild("root")));
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        ) {
      JsonFileReader reader = new JsonFileReader(file, readerAllocator);
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateComplexContent(count, root);
      }
      reader.close();
    }
  }

  @Test
  public void testWriteComplexJSON() throws IOException {
    File file = new File("target/mytest_write_complex.json");
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NullableMapVector parent = new NullableMapVector("parent", vectorAllocator, null)) {
      writeComplexData(count, parent);
      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));
      validateComplexContent(root.getRowCount(), root);
      writeJSON(file, root);
    }
  }

  public void writeJSON(File file, VectorSchemaRoot root) throws IOException {
    JsonFileWriter writer = new JsonFileWriter(file, JsonFileWriter.config().pretty(true));
    writer.start(root.getSchema());
    writer.write(root);
    writer.close();
  }


  @Test
  public void testWriteReadUnionJSON() throws IOException {
    File file = new File("target/mytest_write_union.json");
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NullableMapVector parent = new NullableMapVector("parent", vectorAllocator, null)) {

      writeUnionData(count, parent);

      printVectors(parent.getChildrenFromFields());

      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));
      validateUnionData(count, root);

      writeJSON(file, root);
    }
 // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        ) {
      JsonFileReader reader = new JsonFileReader(file, readerAllocator);
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateUnionData(count, root);
      }
    }
  }

}
