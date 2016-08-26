/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.pojo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Tuple;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Test conversion between Flatbuf and Pojo field representations
 */
public class TestConvert {

  private static final Set<MinorType> NO_SIMPLE = ImmutableSet.of(
          MinorType.MAP,
          MinorType.LIST,
          MinorType.UNION,
          MinorType.DECIMAL
  );

  @Test
  public void simple() {
    for (MinorType minorType : MinorType.values()) {
      if (NO_SIMPLE.contains(minorType)) {
        continue;
      }
      Field field = minorType.getField(minorType.name());
      run(field);
    }
  }

  @Test
  public void complex() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("child1", true, Utf8.INSTANCE, null));
    childrenBuilder.add(new Field("child2", true, new FloatingPoint(0), ImmutableList.<Field>of()));

    Field initialField = new Field("a", true, Tuple.INSTANCE, childrenBuilder.build());
    run(initialField);
  }

  @Test
  public void schema() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("child1", true, Utf8.INSTANCE, null));
    childrenBuilder.add(new Field("child2", true, new FloatingPoint(0), ImmutableList.<Field>of()));
    Schema initialSchema = new Schema(childrenBuilder.build());
    run(initialSchema);

  }

  private void run(Field initialField) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    builder.finish(initialField.getField(builder));
    org.apache.arrow.flatbuf.Field flatBufField = org.apache.arrow.flatbuf.Field.getRootAsField(builder.dataBuffer());
    Field finalField = Field.convertField(flatBufField);
    assertEquals(initialField, finalField);
  }

  private void run(Schema initialSchema) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    builder.finish(initialSchema.getSchema(builder));
    org.apache.arrow.flatbuf.Schema flatBufSchema = org.apache.arrow.flatbuf.Schema.getRootAsSchema(builder.dataBuffer());
    Schema finalSchema = Schema.convertSchema(flatBufSchema);
    assertEquals(initialSchema, finalSchema);
  }
}
