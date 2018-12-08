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

package org.apache.arrow.adapter.jdbc;

import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;

public class JdbcToArrowConfigTest {

  private static final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
  private static final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);

  @Test(expected = NullPointerException.class)
  public void testNullArguments() {
    new JdbcToArrowConfig(null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullCalendar() {
    new JdbcToArrowConfig(allocator, null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullAllocator() {
    new JdbcToArrowConfig(null, calendar);
  }

  @Test(expected = NullPointerException.class)
  public void testSetNullAllocator() {
    JdbcToArrowConfig config = new JdbcToArrowConfig(allocator, calendar);
    config.setAllocator(null);
  }

  @Test(expected = NullPointerException.class)
  public void testSetNullCalendar() {
    JdbcToArrowConfig config = new JdbcToArrowConfig(allocator, calendar);
    config.setCalendar(null);
  }

  @Test
  public void testConfig() {
    JdbcToArrowConfig config = new JdbcToArrowConfig(allocator, calendar);
    assertTrue(config.isValid());
    assertTrue(allocator == config.getAllocator());
    assertTrue(calendar == config.getCalendar());

    Calendar newCalendar = Calendar.getInstance();
    BaseAllocator newAllocator = new RootAllocator(Integer.SIZE);

    config.setAllocator(newAllocator).setCalendar(newCalendar);

    assertTrue(config.isValid());
    assertTrue(newAllocator == config.getAllocator());
    assertTrue(newCalendar == config.getCalendar());
  }

  @Test public void testIncludeMetadata() {
    JdbcToArrowConfig config = new JdbcToArrowConfig(allocator, calendar);
    assertTrue(config.isValid());
    assertFalse(config.getIncludeMetadata());

    config.setIncludeMetadata(true);
    assertTrue(config.getIncludeMetadata());

    config.setIncludeMetadata(false);
    assertFalse(config.getIncludeMetadata());

    config = new JdbcToArrowConfig(allocator, calendar, true);
    assertTrue(config.isValid());
    assertTrue(config.getIncludeMetadata());

    config = new JdbcToArrowConfig(allocator, calendar, false);
    assertTrue(config.isValid());
    assertFalse(config.getIncludeMetadata());
  }
}
