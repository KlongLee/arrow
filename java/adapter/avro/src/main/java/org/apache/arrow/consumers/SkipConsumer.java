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

package org.apache.arrow.consumers;

import java.io.IOException;

import org.apache.arrow.vector.FieldVector;
import org.apache.avro.io.Decoder;

/**
 * Consumer which skip (throw away) data from the decoder.
 */
public class SkipConsumer implements Consumer {

  private final SkipFunction skipFunction;

  public SkipConsumer(SkipFunction skipFunction) {
    this.skipFunction = skipFunction;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    skipFunction.apply(decoder);
  }

  @Override
  public void addNull() {
  }

  @Override
  public void setPosition(int index) {
  }

  @Override
  public FieldVector getVector() {
    return null;
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public boolean resetValueVector(FieldVector vector) {
    return false;
  }

  @Override
  public boolean skippable() {
    return true;
  }
}
