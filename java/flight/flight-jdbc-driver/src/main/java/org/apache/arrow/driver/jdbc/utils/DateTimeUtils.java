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

package org.apache.arrow.driver.jdbc.utils;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Datetime utility functions.
 */
public class DateTimeUtils {
  private DateTimeUtils() {
    // Prevent instantiation.
  }

  /**
   * Subtracts given Calendar's TimeZone offset from epoch milliseconds.
   */
  public static long applyCalendarOffset(long milliseconds, Calendar calendar) {
    if (calendar == null) {
      return milliseconds - Calendar.getInstance(TimeZone.getDefault()).getTimeZone().getOffset(milliseconds);
    }
    return milliseconds - calendar.getTimeZone().getOffset(milliseconds);
  }
}
