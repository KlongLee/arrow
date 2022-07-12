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

package org.apache.arrow.adapter.jdbc.binder;

import java.sql.Types;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Visitor to create the base ColumnBinder for a vector.
 * <p>
 * To handle null values, wrap the returned binder in a {@link NullableColumnBinder}.
 */
public class ColumnBinderArrowTypeVisitor implements ArrowType.ArrowTypeVisitor<ColumnBinder> {
  private final FieldVector vector;
  private final Integer jdbcType;

  /**
   * Create a binder using the default JDBC type code.
   */
  public ColumnBinderArrowTypeVisitor(FieldVector vector) {
    this.vector = vector;
    this.jdbcType = null;
  }

  /**
   * Create a binder using a custom JDBC type code.
   */
  public ColumnBinderArrowTypeVisitor(FieldVector vector, int jdbcType) {
    this.vector = vector;
    this.jdbcType = jdbcType;
  }

  @Override
  public ColumnBinder visit(ArrowType.Null type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Struct type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.List type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.LargeList type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.FixedSizeList type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Union type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Map type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Int type) {
    switch (type.getBitWidth()) {
      case 8:
        if (type.getIsSigned()) {
          return jdbcType == null ? new TinyIntBinder((TinyIntVector) vector) :
              new TinyIntBinder((TinyIntVector) vector, jdbcType);
        } else {
          throw new UnsupportedOperationException(
              "No column binder implemented for unsigned type " + type);
        }
      case 16:
        if (type.getIsSigned()) {
          return jdbcType == null ? new SmallIntBinder((SmallIntVector) vector) :
              new SmallIntBinder((SmallIntVector) vector, jdbcType);
        } else {
          throw new UnsupportedOperationException(
              "No column binder implemented for unsigned type " + type);
        }
      case 32:
        if (type.getIsSigned()) {
          return jdbcType == null ? new IntBinder((IntVector) vector) :
              new IntBinder((IntVector) vector, jdbcType);
        } else {
          throw new UnsupportedOperationException(
              "No column binder implemented for unsigned type " + type);
        }
      case 64:
        if (type.getIsSigned()) {
          return jdbcType == null ? new BigIntBinder((BigIntVector) vector) :
              new BigIntBinder((BigIntVector) vector, jdbcType);
        } else {
          throw new UnsupportedOperationException(
              "No column binder implemented for unsigned type " + type);
        }
      default:
        throw new UnsupportedOperationException("No column binder implemented for type " + type);
    }
  }

  @Override
  public ColumnBinder visit(ArrowType.FloatingPoint type) {
    switch (type.getPrecision()) {
      case SINGLE:
        return jdbcType == null ? new Float4Binder((Float4Vector) vector) :
            new Float4Binder((Float4Vector) vector, jdbcType);
      case DOUBLE:
        return jdbcType == null ? new Float8Binder((Float8Vector) vector) :
            new Float8Binder((Float8Vector) vector, jdbcType);
      default:
        throw new UnsupportedOperationException("No column binder implemented for type " + type);
    }
  }

  @Override
  public ColumnBinder visit(ArrowType.Utf8 type) {
    VarCharVector varChar = (VarCharVector) vector;
    return jdbcType == null ? new VarCharBinder<>(varChar, Types.VARCHAR) :
        new VarCharBinder<>(varChar, jdbcType);
  }

  @Override
  public ColumnBinder visit(ArrowType.LargeUtf8 type) {
    LargeVarCharVector varChar = (LargeVarCharVector) vector;
    return jdbcType == null ? new VarCharBinder<>(varChar, Types.LONGVARCHAR) :
        new VarCharBinder<>(varChar, jdbcType);
  }

  @Override
  public ColumnBinder visit(ArrowType.Binary type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.LargeBinary type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.FixedSizeBinary type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Bool type) {
    return jdbcType == null ? new BitBinder((BitVector) vector) : new BitBinder((BitVector) vector, jdbcType);
  }

  @Override
  public ColumnBinder visit(ArrowType.Decimal type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Date type) {
    switch (type.getUnit()) {
      case DAY:
        return jdbcType == null ? new DateDayBinder((DateDayVector) vector) :
            new DateDayBinder((DateDayVector) vector, /*calendar*/null, jdbcType);
      case MILLISECOND:
        return jdbcType == null ? new DateMilliBinder((DateMilliVector) vector) :
            new DateMilliBinder((DateMilliVector) vector, /*calendar*/null, jdbcType);
      default:
        throw new UnsupportedOperationException("No column binder implemented for type " + type);
    }
  }

  @Override
  public ColumnBinder visit(ArrowType.Time type) {
    switch (type.getUnit()) {
      case SECOND:
        return jdbcType == null ? new Time32Binder((TimeSecVector) vector) :
            new Time32Binder((TimeSecVector) vector, jdbcType);
      case MILLISECOND:
        return jdbcType == null ? new Time32Binder((TimeMilliVector) vector) :
            new Time32Binder((TimeMilliVector) vector, jdbcType);
      case MICROSECOND:
        return jdbcType == null ? new Time64Binder((TimeMicroVector) vector) :
            new Time64Binder((TimeMicroVector) vector, jdbcType);
      case NANOSECOND:
        return jdbcType == null ? new Time64Binder((TimeNanoVector) vector) :
            new Time64Binder((TimeNanoVector) vector, jdbcType);
      default:
        throw new UnsupportedOperationException("No column binder implemented for type " + type);
    }
  }

  @Override
  public ColumnBinder visit(ArrowType.Timestamp type) {
    Calendar calendar = null;
    final String timezone = type.getTimezone();
    if (timezone != null && !timezone.isEmpty()) {
      calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(timezone)));
    }
    return new TimeStampBinder((TimeStampVector) vector, calendar);
  }

  @Override
  public ColumnBinder visit(ArrowType.Interval type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }

  @Override
  public ColumnBinder visit(ArrowType.Duration type) {
    throw new UnsupportedOperationException("No column binder implemented for type " + type);
  }
}
