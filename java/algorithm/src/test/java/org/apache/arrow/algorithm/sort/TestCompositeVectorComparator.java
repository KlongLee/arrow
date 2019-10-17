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

package org.apache.arrow.algorithm.sort;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link CompositeVectorComparator}.
 */
public class TestCompositeVectorComparator {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testCompareVectorSchemaRoot() {
    final int vectorLength = 10;
    IntVector intVec1 = new IntVector("int1", allocator);
    VarCharVector strVec1 = new VarCharVector("str1", allocator);

    IntVector intVec2 = new IntVector("int2", allocator);
    VarCharVector strVec2 = new VarCharVector("str2", allocator);

    try (VectorSchemaRoot batch1 = new VectorSchemaRoot(Arrays.asList(intVec1, strVec1));
         VectorSchemaRoot batch2 = new VectorSchemaRoot(Arrays.asList(intVec2, strVec2))) {

      intVec1.allocateNew(vectorLength);
      strVec1.allocateNew(vectorLength * 10, vectorLength);
      intVec2.allocateNew(vectorLength);
      strVec2.allocateNew(vectorLength * 10, vectorLength);

      for (int i = 0; i < vectorLength; i++) {
        intVec1.set(i, i);
        strVec1.set(i, new String("a" + i).getBytes());
        intVec2.set(i, i);
        strVec2.set(i, new String("a" + i).getBytes());
      }

      VectorValueComparator<IntVector> innerComparator1 =
              DefaultVectorComparators.createDefaultComparator(intVec1);
      innerComparator1.attachVectors(intVec1, intVec2);
      VectorValueComparator<VarCharVector> innerComparator2 =
              DefaultVectorComparators.createDefaultComparator(strVec1);
      innerComparator2.attachVectors(strVec1, strVec2);

      VectorValueComparator<ValueVector> comparator = new CompositeVectorComparator(
          new VectorValueComparator[]{innerComparator1, innerComparator2}
      );

      for (int i = 0; i < vectorLength; i++) {
        for (int j = 0; j < vectorLength; j++) {
          int result1 = comparator.compare(i, j);
          int result2 = comparator.compareNotNull(i, j);
          if (i < j) {
            assertTrue(result1 < 0);
            assertTrue(result2 < 0);
          } else if (i == j) {
            assertTrue(result1 == 0);
            assertTrue(result2 == 0);
          } else {
            assertTrue(result1 > 0);
            assertTrue(result2 > 0);
          }
        }
      }
    }
  }
}
