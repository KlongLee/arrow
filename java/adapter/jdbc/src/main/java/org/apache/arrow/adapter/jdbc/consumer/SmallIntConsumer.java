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

package org.apache.arrow.adapter.jdbc.consumer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.vector.SmallIntVector;

/**
 * Consumer which consume smallInt type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.SmallIntVector}.
 */
public abstract class SmallIntConsumer implements JdbcConsumer<SmallIntVector> {

  /**
   * Creates a consumer for {@link SmallIntVector}.
   */
  public static SmallIntConsumer createConsumer(SmallIntVector vector, int index, boolean nullable) {
    if (nullable) {
      return new NullableSmallIntConsumer(vector, index);
    } else {
      return new NonNullableSmallIntConsumer(vector, index);
    }
  }

  private SmallIntVector vector;
  private final int columnIndexInResultSet;

  private int currentIndex;

  /**
   * Instantiate a SmallIntConsumer.
   */
  public SmallIntConsumer(SmallIntVector vector, int index) {
    this.vector = vector;
    this.columnIndexInResultSet = index;
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException {
    short value = resultSet.getShort(columnIndexInResultSet);
    if (!wasNull(resultSet)) {
      vector.setSafe(currentIndex, value);
    }
    currentIndex++;
  }

  @Override
  public void close() throws Exception {
    this.vector.close();
  }

  @Override
  public void resetValueVector(SmallIntVector vector) {
    this.vector = vector;
    this.currentIndex = 0;
  }

  /**
   * Nullable consumer for small int.
   */
  static class NullableSmallIntConsumer extends SmallIntConsumer {

    /**
     * Instantiate a SmallIntConsumer.
     */
    public NullableSmallIntConsumer(SmallIntVector vector, int index) {
      super(vector, index);
    }

    @Override
    public boolean wasNull(ResultSet resultSet) throws SQLException {
      return resultSet.wasNull();
    }
  }

  /**
   * Non-nullable consumer for small int.
   */
  static class NonNullableSmallIntConsumer extends SmallIntConsumer {

    /**
     * Instantiate a SmallIntConsumer.
     */
    public NonNullableSmallIntConsumer(SmallIntVector vector, int index) {
      super(vector, index);
    }

    @Override
    public boolean wasNull(ResultSet resultSet) throws SQLException {
      return false;
    }
  }
}
