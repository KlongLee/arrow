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

package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFixedSizeListVector {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testIntType() {
    try (FixedSizeListVector vector = FixedSizeListVector.empty("list", 2, allocator)) {
      IntVector nested = (IntVector) vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType())).getVector();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        vector.setNotNull(i);
        nested.set(i * 2, i);
        nested.set(i * 2 + 1, i + 10);
      }
      vector.setValueCount(10);

      UnionFixedSizeListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        Assert.assertTrue(reader.isSet());
        Assert.assertTrue(reader.next());
        assertEquals(i, reader.reader().readInteger().intValue());
        Assert.assertTrue(reader.next());
        assertEquals(i + 10, reader.reader().readInteger().intValue());
        Assert.assertFalse(reader.next());
        assertEquals(Arrays.asList(i, i + 10), reader.readObject());
      }
    }
  }

  @Test
  public void testFloatTypeNullable() {
    try (FixedSizeListVector vector = FixedSizeListVector.empty("list", 2, allocator)) {
      Float4Vector nested = (Float4Vector) vector.addOrGetVector(FieldType.nullable(MinorType.FLOAT4.getType()))
          .getVector();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector.setNotNull(i);
          nested.set(i * 2, i + 0.1f);
          nested.set(i * 2 + 1, i + 10.1f);
        }
      }
      vector.setValueCount(10);

      UnionFixedSizeListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        if (i % 2 == 0) {
          Assert.assertTrue(reader.isSet());
          Assert.assertTrue(reader.next());
          assertEquals(i + 0.1f, reader.reader().readFloat(), 0.00001);
          Assert.assertTrue(reader.next());
          assertEquals(i + 10.1f, reader.reader().readFloat(), 0.00001);
          Assert.assertFalse(reader.next());
          assertEquals(Arrays.asList(i + 0.1f, i + 10.1f), reader.readObject());
        } else {
          Assert.assertFalse(reader.isSet());
          Assert.assertNull(reader.readObject());
        }
      }
    }
  }

  @Test
  public void testNestedInList() {
    try (ListVector vector = ListVector.empty("list", allocator)) {
      FixedSizeListVector tuples = (FixedSizeListVector) vector.addOrGetVector(
          FieldType.nullable(new ArrowType.FixedSizeList(2))).getVector();
      IntVector innerVector = (IntVector) tuples.addOrGetVector(FieldType.nullable(MinorType.INT.getType()))
          .getVector();
      vector.allocateNew();

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          int position = vector.startNewValue(i);
          for (int j = 0; j < i % 7; j++) {
            tuples.setNotNull(position + j);
            innerVector.set((position + j) * 2, j);
            innerVector.set((position + j) * 2 + 1, j + 1);
          }
          vector.endValue(i, i % 7);
        }
      }
      vector.setValueCount(10);

      UnionListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        if (i % 2 == 0) {
          for (int j = 0; j < i % 7; j++) {
            Assert.assertTrue(reader.next());
            FieldReader innerListReader = reader.reader();
            for (int k = 0; k < 2; k++) {
              Assert.assertTrue(innerListReader.next());
              assertEquals(k + j, innerListReader.reader().readInteger().intValue());
            }
            Assert.assertFalse(innerListReader.next());
          }
          Assert.assertFalse(reader.next());
        } else {
          Assert.assertFalse(reader.isSet());
          Assert.assertNull(reader.readObject());
        }
      }
    }
  }

  @Test
  public void testTransferPair() {
    try (FixedSizeListVector from = new FixedSizeListVector("from", allocator, 2, null, null);
         FixedSizeListVector to = new FixedSizeListVector("to", allocator, 2, null, null)) {
      Float4Vector nested = (Float4Vector) from.addOrGetVector(FieldType.nullable(MinorType.FLOAT4.getType()))
          .getVector();
      from.allocateNew();

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          from.setNotNull(i);
          nested.set(i * 2, i + 0.1f);
          nested.set(i * 2 + 1, i + 10.1f);
        }
      }
      from.setValueCount(10);

      TransferPair pair = from.makeTransferPair(to);

      pair.copyValueSafe(0, 1);
      pair.copyValueSafe(2, 2);
      to.copyFromSafe(4, 3, from);

      to.setValueCount(10);

      UnionFixedSizeListReader reader = to.getReader();

      reader.setPosition(0);
      Assert.assertFalse(reader.isSet());
      Assert.assertNull(reader.readObject());

      reader.setPosition(1);
      Assert.assertTrue(reader.isSet());
      Assert.assertTrue(reader.next());
      assertEquals(0.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertTrue(reader.next());
      assertEquals(10.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertFalse(reader.next());
      assertEquals(Arrays.asList(0.1f, 10.1f), reader.readObject());

      reader.setPosition(2);
      Assert.assertTrue(reader.isSet());
      Assert.assertTrue(reader.next());
      assertEquals(2.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertTrue(reader.next());
      assertEquals(12.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertFalse(reader.next());
      assertEquals(Arrays.asList(2.1f, 12.1f), reader.readObject());

      reader.setPosition(3);
      Assert.assertTrue(reader.isSet());
      Assert.assertTrue(reader.next());
      assertEquals(4.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertTrue(reader.next());
      assertEquals(14.1f, reader.reader().readFloat(), 0.00001);
      Assert.assertFalse(reader.next());
      assertEquals(Arrays.asList(4.1f, 14.1f), reader.readObject());

      for (int i = 4; i < 10; i++) {
        reader.setPosition(i);
        Assert.assertFalse(reader.isSet());
        Assert.assertNull(reader.readObject());
      }
    }
  }

  @Test
  public void testConsistentChildName() throws Exception {
    try (FixedSizeListVector listVector = FixedSizeListVector.empty("sourceVector", 2, allocator)) {
      String emptyListStr = listVector.getField().toString();
      Assert.assertTrue(emptyListStr.contains(ListVector.DATA_VECTOR_NAME));

      listVector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));
      String emptyVectorStr = listVector.getField().toString();
      Assert.assertTrue(emptyVectorStr.contains(ListVector.DATA_VECTOR_NAME));
    }
  }

  @Test
  public void testUnionFixedSizeListWriter() throws Exception {
    try (final FixedSizeListVector vector1 = FixedSizeListVector.empty("vector", 3, allocator)) {

      UnionFixedSizeListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      int[] values1 = new int[] {1, 2, 3};
      int[] values2 = new int[] {4, 5, 6};
      int[] values3 = new int[] {7, 8, 9};

      //set some values
      writeListVector(writer1, values1);
      writeListVector(writer1, values2);
      writeListVector(writer1, values3);
      writer1.setValueCount(3);

      assertEquals(3, vector1.getValueCount());

      int[] realValue1 = convertListToIntArray((JsonStringArrayList) vector1.getObject(0));
      assertTrue(Arrays.equals(values1, realValue1));
      int[] realValue2 = convertListToIntArray((JsonStringArrayList) vector1.getObject(1));
      assertTrue(Arrays.equals(values2, realValue2));
      int[] realValue3 = convertListToIntArray((JsonStringArrayList) vector1.getObject(2));
      assertTrue(Arrays.equals(values3, realValue3));
    }
  }

  @Test
  public void testWriteDecimal() throws Exception {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("vector", /*listSize=*/3, allocator)) {

      UnionFixedSizeListWriter writer = vector.getWriter();
      writer.allocate();

      final int valueCount = 100;

      for (int i = 0; i < valueCount; i++) {
        writer.startList();
        writer.decimal().writeDecimal(new BigDecimal(i));
        writer.decimal().writeDecimal(new BigDecimal(i * 2));
        writer.decimal().writeDecimal(new BigDecimal(i * 3));
        writer.endList();
      }
      vector.setValueCount(valueCount);

      for (int i = 0; i < valueCount; i++) {
        List<BigDecimal> values = (List<BigDecimal>) vector.getObject(i);
        assertEquals(3, values.size());
        assertEquals(new BigDecimal(i), values.get(0));
        assertEquals(new BigDecimal(i * 2), values.get(1));
        assertEquals(new BigDecimal(i * 3), values.get(2));
      }
    }
  }

  @Test
  public void testDecimalIndexCheck() throws Exception {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("vector", /*listSize=*/3, allocator)) {

      UnionFixedSizeListWriter writer = vector.getWriter();
      writer.allocate();

      IllegalStateException e = assertThrows(IllegalStateException.class, () -> {
        writer.startList();
        writer.decimal().writeDecimal(new BigDecimal(1));
        writer.decimal().writeDecimal(new BigDecimal(2));
        writer.decimal().writeDecimal(new BigDecimal(3));
        writer.decimal().writeDecimal(new BigDecimal(4));
        writer.endList();
      });
      assertEquals("values at index 0 is greater than listSize 3", e.getMessage());
    }
  }


  @Test(expected = IllegalStateException.class)
  public void testWriteIllegalData() throws Exception {
    try (final FixedSizeListVector vector1 = FixedSizeListVector.empty("vector", 3, allocator)) {

      UnionFixedSizeListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      int[] values1 = new int[] {1, 2, 3};
      int[] values2 = new int[] {4, 5, 6, 7, 8};

      //set some values
      writeListVector(writer1, values1);
      writeListVector(writer1, values2);
      writer1.setValueCount(3);

      assertEquals(3, vector1.getValueCount());
      int[] realValue1 = convertListToIntArray((JsonStringArrayList) vector1.getObject(0));
      assertTrue(Arrays.equals(values1, realValue1));
      int[] realValue2 = convertListToIntArray((JsonStringArrayList) vector1.getObject(1));
      assertTrue(Arrays.equals(values2, realValue2));
    }
  }

  @Test
  public void testWriteListWithFixedList() {
    try (final ListVector listVector = ListVector.empty("listVector", allocator)) {

      UnionListWriter listWriter = listVector.getWriter();
      listWriter.allocate();

      BaseWriter.ListWriter w = listWriter.fixedSizeList(2);

      listWriter.startList();

      w.startList();
      w.integer().writeInt(0);
      w.integer().writeInt(1);
      w.endList();

      w.startList();
      w.integer().writeInt(2);
      w.integer().writeInt(3);
      w.endList();
      listWriter.endList();

      listWriter.startList();
      w.startList();
      w.integer().writeInt(4);
      w.integer().writeInt(5);
      w.endList();

      w.startList();
      w.integer().writeInt(6);
      w.integer().writeInt(7);
      w.endList();
      listWriter.endList();

      listVector.setValueCount(2);

      FixedSizeListVector fixedVector = (FixedSizeListVector) listVector.getDataVector();
      assertEquals(4, fixedVector.getValueCount());
      IntVector intVector = (IntVector) fixedVector.getDataVector();
      assertEquals(8, intVector.getValueCount());

      // verify data
      for (int i = 0; i < 8; i++) {
        assertEquals(i, intVector.get(i));
      }
    }
  }

  @Test
  public void testWriteFixedListWithFixedList() {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("fixedVector", 3, allocator)) {

      UnionFixedSizeListWriter fixedWriter = vector.getWriter();
      fixedWriter.allocate();

      BaseWriter.ListWriter w = fixedWriter.fixedSizeList(2);

      fixedWriter.startList();

      w.startList();
      w.integer().writeInt(0);
      w.integer().writeInt(1);
      w.endList();

      w.startList();
      w.integer().writeInt(2);
      w.integer().writeInt(3);
      w.endList();
      fixedWriter.endList();

      fixedWriter.startList();
      w.startList();
      w.integer().writeInt(4);
      w.integer().writeInt(5);
      w.endList();

      w.startList();
      w.integer().writeInt(6);
      w.integer().writeInt(7);
      w.endList();
      fixedWriter.endList();

      fixedWriter.setValueCount(2);

      FixedSizeListVector fixedVector = (FixedSizeListVector) vector.getDataVector();
      assertEquals(6, fixedVector.getValueCount());
      IntVector intVector = (IntVector) fixedVector.getDataVector();
      assertEquals(12, intVector.getValueCount());

      // verify data
      Integer[] expected = new Integer[] {0, 1, 2, 3, null, null, 4, 5, 6, 7, null, null};
      for (int i = 0; i < 12; i++) {
        if (expected[i] == null) {
          assertTrue(intVector.isNull(i));
        } else {
          assertEquals(expected[i].intValue(), intVector.get(i));
        }
      }
    }
  }

  @Test
  public void testSplitAndTransfer() throws Exception {
    try (final FixedSizeListVector vector1 = FixedSizeListVector.empty("vector", 3, allocator)) {

      UnionFixedSizeListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      int[] values1 = new int[] {1, 2, 3};
      int[] values2 = new int[] {4, 5, 6};
      int[] values3 = new int[] {7, 8, 9};

      //set some values
      writeListVector(writer1, values1);
      writeListVector(writer1, values2);
      writeListVector(writer1, values3);
      writer1.setValueCount(3);

      TransferPair transferPair = vector1.getTransferPair(allocator);
      transferPair.splitAndTransfer(0, 2);
      FixedSizeListVector targetVector = (FixedSizeListVector) transferPair.getTo();

      assertEquals(2, targetVector.getValueCount());
      int[] realValue1 = convertListToIntArray((JsonStringArrayList) targetVector.getObject(0));
      assertTrue(Arrays.equals(values1, realValue1));
      int[] realValue2 = convertListToIntArray((JsonStringArrayList) targetVector.getObject(1));
      assertTrue(Arrays.equals(values2, realValue2));

      targetVector.clear();
    }
  }

  private int[] convertListToIntArray(JsonStringArrayList list) {
    int[] values = new int[list.size()];
    for (int i = 0; i < list.size(); i++) {
      values[i] = (int) list.get(i);
    }
    return values;
  }

  private void writeListVector(UnionFixedSizeListWriter writer, int[] values) throws Exception {
    writer.startList();
    for (int v: values) {
      writer.integer().writeInt(v);
    }
    writer.endList();
  }

}
