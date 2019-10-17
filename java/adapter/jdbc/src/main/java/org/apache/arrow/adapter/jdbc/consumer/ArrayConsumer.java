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

import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.vector.complex.ListVector;

/**
 * Consumer which consume array type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.complex.ListVector}.
 */
public abstract class ArrayConsumer implements JdbcConsumer<ListVector> {

  /**
   * Creates a consumer for {@link ListVector}.
   */
  public static ArrayConsumer createConsumer(
          ListVector vector, JdbcConsumer delegate, int index, boolean nullable) {
    if (nullable) {
      return new ArrayConsumer.NullableArrayConsumer(vector, delegate, index);
    } else {
      return new ArrayConsumer.NonNullableArrayConsumer(vector, delegate, index);
    }
  }

  private final JdbcConsumer delegate;
  private final int columnIndexInResultSet;

  private ListVector vector;
  private int currentIndex;

  /**
   * Instantiate a ArrayConsumer.
   */
  public ArrayConsumer(ListVector vector, JdbcConsumer delegate, int index) {
    this.columnIndexInResultSet = index;
    this.delegate = delegate;
    this.vector = vector;
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException, IOException {
    final Array array = resultSet.getArray(columnIndexInResultSet);
    if (!wasNull(resultSet)) {

      vector.startNewValue(currentIndex);
      int count = 0;
      try (ResultSet rs = array.getResultSet()) {
        while (rs.next()) {
          delegate.consume(rs);
          count++;
        }
      }
      vector.endValue(currentIndex, count);
    }
    currentIndex++;
  }

  @Override
  public void close() throws Exception {
    this.vector.close();
    this.delegate.close();
  }

  @Override
  public void resetValueVector(ListVector vector) {
    this.vector = vector;
    this.currentIndex = 0;
  }

  /**
   * Nullable consumer for {@link ListVector}.
   */
  static class NullableArrayConsumer extends ArrayConsumer {

    /**
     * Instantiate a nullable array consumer.
     */
    public NullableArrayConsumer(ListVector vector, JdbcConsumer delegate, int index) {
      super(vector, delegate, index);
    }

    @Override
    public boolean wasNull(ResultSet resultSet) throws SQLException {
      return resultSet.wasNull();
    }
  }

  /**
   * Non-nullable consumer for {@link ListVector}.
   */
  static class NonNullableArrayConsumer extends ArrayConsumer {

    /**
     * Instantiate a nullable array consumer.
     */
    public NonNullableArrayConsumer(ListVector vector, JdbcConsumer delegate, int index) {
      super(vector, delegate, index);
    }

    @Override
    public boolean wasNull(ResultSet resultSet) throws SQLException {
      return false;
    }
  }
}
