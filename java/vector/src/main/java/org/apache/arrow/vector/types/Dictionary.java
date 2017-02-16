/*******************************************************************************

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
 ******************************************************************************/
package org.apache.arrow.vector.types;

import org.apache.arrow.vector.FieldVector;

import java.util.Objects;

public class Dictionary {

  private Long id;
  private FieldVector dictionary;
  private boolean ordered;

  public Dictionary(FieldVector dictionary) {
    this(dictionary, null, false);
  }

  public Dictionary(FieldVector dictionary, Long id, boolean ordered) {
    this.id = id;
    this.dictionary = dictionary;
    this.ordered = ordered;
  }

  public Long getId() { return id; }

  public FieldVector getVector() {
  return dictionary;
  }

  public boolean isOrdered() {
  return ordered;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Dictionary that = (Dictionary) o;
    return id == that.id &&
      ordered == that.ordered &&
      Objects.equals(dictionary, that.dictionary);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, dictionary, ordered);
  }
}
