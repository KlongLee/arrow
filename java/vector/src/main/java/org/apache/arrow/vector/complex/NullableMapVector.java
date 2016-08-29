/**
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
 */
package org.apache.arrow.vector.complex;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseDataValueVector;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableVectorDefinitionSetter;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.holders.ComplexHolder;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.util.CallBack;

import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ArrowBuf;

public class NullableMapVector extends MapVector implements FieldVector {

  private final UInt1Vector bits;

  private final List<BufferBacked> innerVectors;

  private final Accessor accessor;
  private final Mutator mutator;

  public NullableMapVector(String name, BufferAllocator allocator, CallBack callBack) {
    super(name, checkNotNull(allocator), callBack);
    this.bits = new UInt1Vector("$bits$", allocator);
    this.innerVectors = Collections.unmodifiableList(Arrays.<BufferBacked>asList(bits));
    this.accessor = new Accessor();
    this.mutator = new Mutator();
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    BaseDataValueVector.load(getFieldInnerVectors(), ownBuffers);
    this.valueCount = fieldNode.getLength();
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return BaseDataValueVector.unload(getFieldInnerVectors());
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    return innerVectors;
  }

  @Override
  public int getValueCapacity() {
    return Math.min(bits.getValueCapacity(), super.getValueCapacity());
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    return ObjectArrays.concat(bits.getBuffers(clear), super.getBuffers(clear), ArrowBuf.class);
  }

  @Override
  public void close() {
    bits.close();
    super.close();
  }

  @Override
  public void clear() {
    bits.clear();
    super.clear();
  }

  @Override
  public int getBufferSize(){
    return super.getBufferSize() + bits.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    return super.getBufferSizeFor(valueCount)
        + bits.getBufferSizeFor(valueCount);
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    bits.setInitialCapacity(numRecords);
    super.setInitialCapacity(numRecords);
  }

  @Override
  public boolean allocateNewSafe() {
    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      success = super.allocateNewSafe() && bits.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    bits.zeroVector();
    return success;
  }
  public final class Accessor extends MapVector.Accessor  {
    final UInt1Vector.Accessor bAccessor = bits.getAccessor();

    @Override
    public Object getObject(int index) {
      if (isNull(index)) {
        return null;
      } else {
        return super.getObject(index);
      }
    }

    @Override
    public void get(int index, ComplexHolder holder) {
      holder.isSet = isSet(index);
      super.get(index, holder);
    }

    @Override
    public boolean isNull(int index) {
      return isSet(index) == 0;
    }

    public int isSet(int index){
      return bAccessor.get(index);
    }

  }

  public final class Mutator extends MapVector.Mutator implements NullableVectorDefinitionSetter {

    private Mutator(){
    }

    @Override
    public void setIndexDefined(int index){
      bits.getMutator().set(index, 1);
    }

    public void setNull(int index){
      bits.getMutator().setSafe(index, 0);
    }

    public boolean isSafe(int outIndex) {
      return outIndex < NullableMapVector.this.getValueCapacity();
    }

    @Override
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      super.setValueCount(valueCount);
      bits.getMutator().setValueCount(valueCount);
    }

  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }
}
