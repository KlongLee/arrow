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
package org.apache.arrow.vector.file;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableTinyIntVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryUtils;
import org.apache.arrow.vector.schema.ArrowBuffer;
import org.apache.arrow.vector.schema.ArrowMessage;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.stream.MessageSerializerTest;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestArrowFile extends BaseFileTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestArrowFile.class);

  @Test
  public void testWrite() throws IOException {
    File file = new File("target/mytest_write.arrow");
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", vectorAllocator, null)) {
      writeData(count, parent);
      write(parent.getChild("root"), file, new ByteArrayOutputStream());
    }
  }

  @Test
  public void testWriteComplex() throws IOException {
    File file = new File("target/mytest_write_complex.arrow");
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NullableMapVector parent = new NullableMapVector("parent", vectorAllocator, null, null)) {
      writeComplexData(count, parent);
      FieldVector root = parent.getChild("root");
      validateComplexContent(count, new VectorSchemaRoot(root));
      write(root, file, new ByteArrayOutputStream());
    }
  }

  @Test
  public void testWriteRead() throws IOException {
    File file = new File("target/mytest.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;

    // write
    try (BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         MapVector parent = new MapVector("parent", originalVectorAllocator, null)) {
      writeData(count, parent);
      write(parent.getChild("root"), file, stream);
    }

    // read
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator){
            @Override
            protected ArrowMessage readMessage(SeekableReadChannel in, BufferAllocator allocator) throws IOException {
              ArrowMessage message = super.readMessage(in, allocator);
              if (message != null) {
                ArrowRecordBatch batch = (ArrowRecordBatch) message;
                List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();
                for (ArrowBuffer arrowBuffer : buffersLayout) {
                  Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
                }
              }
              return message;
            }
         }) {
      Schema schema = arrowReader.getVectorSchemaRoot().getSchema();
      LOGGER.debug("reading schema: " + schema);
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      for (ArrowBlock rbBlock : arrowReader.getRecordBlocks()) {
        arrowReader.loadRecordBatch(rbBlock);
        Assert.assertEquals(count, root.getRowCount());
        validateContent(count, root);
      }
    }

    // Read from stream.
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator){
           @Override
           protected ArrowMessage readMessage(ReadChannel in, BufferAllocator allocator) throws IOException {
             ArrowMessage message = super.readMessage(in, allocator);
             if (message != null) {
               ArrowRecordBatch batch = (ArrowRecordBatch) message;
               List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();
               for (ArrowBuffer arrowBuffer : buffersLayout) {
                 Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
               }
             }
             return message;
           }
         }) {

      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      Assert.assertEquals(count, root.getRowCount());
      validateContent(count, root);
    }
  }

  @Test
  public void testWriteReadComplex() throws IOException {
    File file = new File("target/mytest_complex.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;

    // write
    try (BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         MapVector parent = new MapVector("parent", originalVectorAllocator, null)) {
      writeComplexData(count, parent);
      write(parent.getChild("root"), file, stream);
    }

    // read
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);

      for (ArrowBlock rbBlock : arrowReader.getRecordBlocks()) {
        arrowReader.loadRecordBatch(rbBlock);
        Assert.assertEquals(count, root.getRowCount());
        validateComplexContent(count, root);
      }
    }

    // Read from stream.
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      Assert.assertEquals(count, root.getRowCount());
      validateComplexContent(count, root);
    }
  }

  @Test
  public void testWriteReadMultipleRBs() throws IOException {
    File file = new File("target/mytest_multiple.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int[] counts = { 10, 5 };

    // write
    try (BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         MapVector parent = new MapVector("parent", originalVectorAllocator, null);
         FileOutputStream fileOutputStream = new FileOutputStream(file);){
      writeData(counts[0], parent);
      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));

      try(ArrowFileWriter fileWriter = new ArrowFileWriter(root, null, fileOutputStream.getChannel());
          ArrowStreamWriter streamWriter = new ArrowStreamWriter(root, null, stream)) {
        fileWriter.start();
        streamWriter.start();

        fileWriter.writeBatch();
        streamWriter.writeBatch();

        parent.allocateNew();
        writeData(counts[1], parent); // if we write the same data we don't catch that the metadata is stored in the wrong order.
        root.setRowCount(counts[1]);

        fileWriter.writeBatch();
        streamWriter.writeBatch();

        fileWriter.end();
        streamWriter.end();
      }
    }

    // read file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      int i = 0;
      List<ArrowBlock> recordBatches = arrowReader.getRecordBlocks();
      Assert.assertEquals(2, recordBatches.size());
      long previousOffset = 0;
      for (ArrowBlock rbBlock : recordBatches) {
        Assert.assertTrue(rbBlock.getOffset() + " > " + previousOffset, rbBlock.getOffset() > previousOffset);
        previousOffset = rbBlock.getOffset();
        arrowReader.loadRecordBatch(rbBlock);
        Assert.assertEquals("RB #" + i, counts[i], root.getRowCount());
        validateContent(counts[i], root);
        ++i;
      }
    }

    // read stream
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      int i = 0;

      for (int n = 0; n < 2; n++) {
        arrowReader.loadNextBatch();
        Assert.assertEquals("RB #" + i, counts[i], root.getRowCount());
        validateContent(counts[i], root);
        ++i;
      }
      arrowReader.loadNextBatch();
      Assert.assertEquals(0, root.getRowCount());
    }
  }

  @Test
  public void testWriteReadUnion() throws IOException {
    File file = new File("target/mytest_write_union.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;

    // write
    try (BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         NullableMapVector parent = new NullableMapVector("parent", vectorAllocator, null, null)) {
      writeUnionData(count, parent);
      validateUnionData(count, new VectorSchemaRoot(parent.getChild("root")));
      write(parent.getChild("root"), file, stream);
    }

    // read file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      validateUnionData(count, root);
    }

    // Read from stream.
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      validateUnionData(count, root);
    }
  }

  @Test
  public void testWriteReadTiny() throws IOException {
    File file = new File("target/mytest_write_tiny.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    try (VectorSchemaRoot root = VectorSchemaRoot.create(MessageSerializerTest.testSchema(), allocator)) {
      root.getFieldVectors().get(0).allocateNew();
      NullableTinyIntVector.Mutator mutator = (NullableTinyIntVector.Mutator) root.getFieldVectors().get(0).getMutator();
      for (int i = 0; i < 16; i++) {
        mutator.set(i, i < 8 ? 1 : 0, (byte)(i + 1));
      }
      mutator.setValueCount(16);
      root.setRowCount(16);

      // write file
      try (FileOutputStream fileOutputStream = new FileOutputStream(file);
           ArrowFileWriter arrowWriter = new ArrowFileWriter(root, null, fileOutputStream.getChannel())) {
        LOGGER.debug("writing schema: " + root.getSchema());
        arrowWriter.start();
        arrowWriter.writeBatch();
        arrowWriter.end();
      }
      // write stream
      try (ArrowStreamWriter arrowWriter = new ArrowStreamWriter(root, null, stream)) {
        arrowWriter.start();
        arrowWriter.writeBatch();
        arrowWriter.end();
      }
    }

    // read file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("fileReader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      validateTinyData(root);
    }

    // Read from stream.
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("streamReader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      validateTinyData(root);
    }
  }

  private void validateTinyData(VectorSchemaRoot root) {
    Assert.assertEquals(16, root.getRowCount());
    NullableTinyIntVector vector = (NullableTinyIntVector) root.getFieldVectors().get(0);
    for (int i = 0; i < 16; i++) {
      if (i < 8) {
        Assert.assertEquals((byte)(i + 1), vector.getAccessor().get(i));
      } else {
        Assert.assertTrue(vector.getAccessor().isNull(i));
      }
    }
  }

  @Test
  public void testWriteReadDictionary() throws IOException {
    File file = new File("target/mytest_dict.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    // write
    try (BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         NullableVarCharVector vector = new NullableVarCharVector("varchar", originalVectorAllocator, null);
         NullableVarCharVector dictionaryVector = new NullableVarCharVector("dict", originalVectorAllocator, null)) {
      vector.allocateNewSafe();
      NullableVarCharVector.Mutator mutator = vector.getMutator();
      mutator.set(0, "foo".getBytes(StandardCharsets.UTF_8));
      mutator.set(1, "bar".getBytes(StandardCharsets.UTF_8));
      mutator.set(3, "baz".getBytes(StandardCharsets.UTF_8));
      mutator.set(4, "bar".getBytes(StandardCharsets.UTF_8));
      mutator.set(5, "baz".getBytes(StandardCharsets.UTF_8));
      mutator.setValueCount(6);

      dictionaryVector.allocateNewSafe();
      mutator = dictionaryVector.getMutator();
      mutator.set(0, "foo".getBytes(StandardCharsets.UTF_8));
      mutator.set(1, "bar".getBytes(StandardCharsets.UTF_8));
      mutator.set(2, "baz".getBytes(StandardCharsets.UTF_8));
      mutator.setValueCount(3);

      Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
      MapDictionaryProvider provider = new MapDictionaryProvider();
      provider.put(dictionary);

      FieldVector encodedVector = (FieldVector) DictionaryUtils.encode(vector, dictionary);

      List<Field> fields = ImmutableList.of(encodedVector.getField());
      List<FieldVector> vectors = ImmutableList.of(encodedVector);
      VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, 6);

      try (FileOutputStream fileOutputStream = new FileOutputStream(file);
           ArrowFileWriter fileWriter = new ArrowFileWriter(root, provider, fileOutputStream.getChannel());
           ArrowStreamWriter streamWriter = new ArrowStreamWriter(root, provider, stream)) {
        LOGGER.debug("writing schema: " + root.getSchema());
        fileWriter.start();
        streamWriter.start();
        fileWriter.writeBatch();
        streamWriter.writeBatch();
        fileWriter.end();
        streamWriter.end();
      }

      dictionaryVector.close();
      encodedVector.close();
    }

    // read from file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      validateDictionary(root.getFieldVectors().get(0), arrowReader.getDictionaryVectors());
    }

    // Read from stream
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      validateDictionary(root.getFieldVectors().get(0), arrowReader.getDictionaryVectors());
    }
  }

  private void validateDictionary(FieldVector vector, Map<Long, Dictionary> dictionaries) {
    Assert.assertNotNull(vector);

    DictionaryEncoding encoding = vector.getDictionaryEncoding();
    Assert.assertNotNull(encoding);
    Assert.assertEquals(1L, encoding.getId());

    FieldVector.Accessor accessor = vector.getAccessor();
    Assert.assertEquals(6, accessor.getValueCount());
    Assert.assertEquals(0, accessor.getObject(0));
    Assert.assertEquals(1, accessor.getObject(1));
    Assert.assertEquals(null, accessor.getObject(2));
    Assert.assertEquals(2, accessor.getObject(3));
    Assert.assertEquals(1, accessor.getObject(4));
    Assert.assertEquals(2, accessor.getObject(5));

    Dictionary dictionary = dictionaries.get(1L);
    Assert.assertNotNull(dictionary);
    NullableVarCharVector.Accessor dictionaryAccessor = ((NullableVarCharVector) dictionary.getVector()).getAccessor();
    Assert.assertEquals(3, dictionaryAccessor.getValueCount());
    Assert.assertEquals(new Text("foo"), dictionaryAccessor.getObject(0));
    Assert.assertEquals(new Text("bar"), dictionaryAccessor.getObject(1));
    Assert.assertEquals(new Text("baz"), dictionaryAccessor.getObject(2));
  }

  /**
   * Writes the contents of parents to file. If outStream is non-null, also writes it
   * to outStream in the streaming serialized format.
   */
  private void write(FieldVector parent, File file, OutputStream outStream) throws IOException {
    VectorSchemaRoot root = new VectorSchemaRoot(parent);

    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
         ArrowFileWriter arrowWriter = new ArrowFileWriter(root, null, fileOutputStream.getChannel());) {
      LOGGER.debug("writing schema: " + root.getSchema());
      arrowWriter.start();
      arrowWriter.writeBatch();
      arrowWriter.end();
    }

    // Also try serializing to the stream writer.
    if (outStream != null) {
      try (ArrowStreamWriter arrowWriter = new ArrowStreamWriter(root, null, outStream)) {
        arrowWriter.start();
        arrowWriter.writeBatch();
        arrowWriter.end();
      }
    }
  }
}
