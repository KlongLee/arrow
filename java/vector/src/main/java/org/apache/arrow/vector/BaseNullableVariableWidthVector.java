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

package org.apache.arrow.vector;


import io.netty.buffer.ArrowBuf;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class BaseNullableVariableWidthVector extends BaseValueVector
        implements VariableWidthVector, FieldVector {
   private static final int DEFAULT_RECORD_BYTE_COUNT = 8;
   private static final int INITIAL_BYTE_COUNT = INITIAL_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT;

   private int valueAllocationSizeInBytes;
   private int validityAllocationSizeInBytes;
   private int offsetAllocationSizeInBytes;

   /* protected members */
   public static final int OFFSET_WIDTH = 4; /* 4 byte unsigned int to track offsets */
   protected static final byte[] emptyByteArray = new byte[]{};
   protected ArrowBuf validityBuffer;
   protected ArrowBuf valueBuffer;
   protected ArrowBuf offsetBuffer;
   protected int valueCount;
   protected int lastSet;
   protected final Field field;
   private boolean cleared;

   public BaseNullableVariableWidthVector(final String name, final BufferAllocator allocator,
                                          FieldType fieldType) {
      super(name, allocator);
      valueAllocationSizeInBytes = INITIAL_BYTE_COUNT;
      validityAllocationSizeInBytes = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION);
      offsetAllocationSizeInBytes = (INITIAL_VALUE_ALLOCATION) * OFFSET_WIDTH;
      field = new Field(name, fieldType, null);
      valueCount = 0;
      lastSet = -1;
      offsetBuffer = allocator.getEmpty();
      validityBuffer = allocator.getEmpty();
      valueBuffer = allocator.getEmpty();
      cleared = false;
   }

  /* TODO:
    * Determine how writerIndex and readerIndex need to be used. Right now we
    * are setting the writerIndex and readerIndex in the call to getFieldBuffers
    * using the valueCount -- this assumes that the caller of getFieldBuffers
    * on the vector has already invoked setValueCount.
    *
    * Do we need to set them during vector transfer and splitAndTransfer?
    */

   /* TODO:
    *
    * see if getNullCount() can be made faster -- O(1)
    */

  /* TODO:
    * Once the entire hierarchy has been refactored, move common functions
    * like getNullCount(), splitAndTransferValidityBuffer to top level
    * base class BaseValueVector.
    *
    * Along with this, some class members (validityBuffer) can also be
    * abstracted out to top level base class.
    *
    * Right now BaseValueVector is the top level base class for other
    * vector types in ValueVector hierarchy (non-nullable) and those
    * vectors have not yet been refactored/removed so moving things to
    * the top class as of now is not a good idea.
    */


   @Override
   @Deprecated
   public VariableWidthMutator getMutator() {
      throw new  UnsupportedOperationException("Mutator is not supported for writing into vector");
   }

   @Override
   @Deprecated
   public VariableWidthAccessor getAccessor() {
      throw new UnsupportedOperationException("Accessor is not supported for reading from vector");
   }

   @Override
   public ArrowBuf getValidityBuffer() {
      return validityBuffer;
   }

   @Override
   public ArrowBuf getDataBuffer() {
      return valueBuffer;
   }

   @Override
   public ArrowBuf getOffsetBuffer() {
      return offsetBuffer;
   }

   @Override
   public long getOffsetBufferAddress() {
      return offsetBuffer.memoryAddress();
   }

   @Override
   public long getValidityBufferAddress() {
      return validityBuffer.memoryAddress();
   }

   @Override
   public long getDataBufferAddress() {
      return valueBuffer.memoryAddress();
   }

   @Override
   public void setInitialCapacity(int valueCount) {
      final long size = (long)valueCount * DEFAULT_RECORD_BYTE_COUNT;
      if (size > MAX_ALLOCATION_SIZE) {
         throw new OversizedAllocationException("Requested amount of memory is more than max allowed");
      }
      valueAllocationSizeInBytes = (int)size;
      validityAllocationSizeInBytes = getValidityBufferSizeFromCount(valueCount);
      /* to track the end offset of last data element in vector, we need
       * an additional slot in offset buffer.
       */
      offsetAllocationSizeInBytes = (valueCount + 1) * OFFSET_WIDTH;
   }

   @Override
   public int getValueCapacity(){
      final int offsetValueCapacity = Math.max(getOffsetBufferValueCapacity() - 1, 0);
      return Math.min(offsetValueCapacity, getValidityBufferValueCapacity());
   }

   private int getValidityBufferValueCapacity() {
      return (int)(validityBuffer.capacity() * 8L);
   }

   private int getOffsetBufferValueCapacity() {
      return (int)((offsetBuffer.capacity() * 1.0)/OFFSET_WIDTH);
   }

   public void zeroVector() {
      initValidityBuffer();
      initOffsetBuffer();
   }

   private void initValidityBuffer() {
      validityBuffer.setZero(0, validityBuffer.capacity());
   }

   private void initOffsetBuffer() {
      offsetBuffer.setZero(0, offsetBuffer.capacity());
   }

   public void reset() {
      zeroVector();
      lastSet = -1;
   }

   @Override
   public void close() {
      clear();
   }

   @Override
   public void clear() {
      validityBuffer = releaseBuffer(validityBuffer);
      valueBuffer = releaseBuffer(valueBuffer);
      offsetBuffer = releaseBuffer(offsetBuffer);
      cleared = true;
      lastSet = -1;
      valueCount = 0;
   }

   @Override
   @Deprecated
   public List<BufferBacked> getFieldInnerVectors() {
      throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers");
   }

   @Override
   public void initializeChildrenFromFields(List<Field> children) {
      if (!children.isEmpty()) {
         throw new IllegalArgumentException("primitive type vector can not have children");
      }
   }

   @Override
   public List<FieldVector> getChildrenFromFields() {
      return Collections.emptyList();
   }

   @Override
   public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
      ArrowBuf bitBuffer = ownBuffers.get(0);
      ArrowBuf offBuffer = ownBuffers.get(1);
      ArrowBuf dataBuffer = ownBuffers.get(2);

      validityBuffer.release();
      validityBuffer = bitBuffer.retain(allocator);
      offsetBuffer.release();
      offsetBuffer = offBuffer.retain(allocator);
      valueBuffer.release();
      valueBuffer = dataBuffer.retain(allocator);

      lastSet = fieldNode.getLength() - 1;
      valueCount = fieldNode.getLength();
   }

   public List<ArrowBuf> getFieldBuffers() {
      List<ArrowBuf> result = new ArrayList<>(3);
      final int lastDataOffset = getstartOffset(valueCount);
      validityBuffer.readerIndex(0);
      validityBuffer.writerIndex(getValidityBufferSizeFromCount(valueCount));
      offsetBuffer.readerIndex(0);
      offsetBuffer.writerIndex((valueCount + 1) * OFFSET_WIDTH);
      valueBuffer.readerIndex(0);
      valueBuffer.writerIndex(lastDataOffset);

      result.add(validityBuffer);
      result.add(offsetBuffer);
      result.add(valueBuffer);

      return result;
   }

   @Override
   public void allocateNew() {
      if(!allocateNewSafe()){
         throw new OutOfMemoryException("Failure while allocating memory.");
      }
   }

   @Override
   public boolean allocateNewSafe() {
      long curAllocationSizeValue = valueAllocationSizeInBytes;
      long curAllocationSizeValidity = validityAllocationSizeInBytes;
      long curAllocationSizeOffset = offsetAllocationSizeInBytes;

      if (curAllocationSizeValue > MAX_ALLOCATION_SIZE ||
              curAllocationSizeOffset > MAX_ALLOCATION_SIZE) {
         throw new OversizedAllocationException("Requested amount of memory exceeds limit");
      }

      /* we are doing a new allocation -- release the current buffers */
      clear();

      try {
         allocateBytes(curAllocationSizeValue, curAllocationSizeValidity, curAllocationSizeOffset);
      } catch (Exception e) {
         e.printStackTrace();
         clear();
         return false;
      }

      return true;
   }

   @Override
   public void allocateNew(int totalBytes, int valueCount) {
      assert totalBytes >= 0;
      final int offsetBufferSize = (valueCount + 1) * OFFSET_WIDTH;
      final int validityBufferSize = getValidityBufferSizeFromCount(valueCount);

      if (totalBytes > MAX_ALLOCATION_SIZE ||
              offsetBufferSize > MAX_ALLOCATION_SIZE) {
         throw new OversizedAllocationException("Requested amount of memory exceeds limit");
      }

      /* we are doing a new allocation -- release the current buffers */
      clear();

      try {
         allocateBytes(totalBytes, validityBufferSize, offsetBufferSize);
      } catch (Exception e) {
         e.printStackTrace();
         clear();
      }
   }

   private void allocateBytes(final long valueBufferSize, final long validityBufferSize,
                              final long offsetBufferSize) {
      /* allocate data buffer */
      int curSize = (int)valueBufferSize;
      valueBuffer = allocator.buffer(curSize);
      valueBuffer.readerIndex(0);
      valueAllocationSizeInBytes = curSize;
      allocateValidityBuffer(validityBufferSize);
      allocateOffsetBuffer(offsetBufferSize);
   }

   private void allocateOffsetBuffer(final long size) {
      final int curSize = (int)size;
      offsetBuffer = allocator.buffer(curSize);
      offsetBuffer.readerIndex(0);
      offsetAllocationSizeInBytes = curSize;
      initOffsetBuffer();
   }

   private void allocateValidityBuffer(final long size) {
      final int curSize = (int)size;
      validityBuffer = allocator.buffer(curSize);
      validityBuffer.readerIndex(0);
      validityAllocationSizeInBytes = curSize;
      initValidityBuffer();
   }

   public void reAlloc() {
      reallocValueBuffer();
      reallocValidityAndOffsetBuffers();
   }

   protected void reallocValueBuffer() {
      long baseSize = valueAllocationSizeInBytes;
      final int currentBufferCapacity = valueBuffer.capacity();

      if (baseSize < (long)currentBufferCapacity) {
         baseSize = (long)currentBufferCapacity;
      }

      long newAllocationSize = baseSize * 2L;
      newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);

      if (newAllocationSize > MAX_ALLOCATION_SIZE)  {
         throw new OversizedAllocationException("Unable to expand the buffer");
      }

      final ArrowBuf newBuf = allocator.buffer((int)newAllocationSize);
      newBuf.setBytes(0, valueBuffer, 0, currentBufferCapacity);
      valueBuffer.release();
      valueBuffer = newBuf;
      valueAllocationSizeInBytes = (int)newAllocationSize;
   }

   protected void reallocValidityAndOffsetBuffers() {
      offsetBuffer = reallocBufferHelper(offsetBuffer, true);
      validityBuffer = reallocBufferHelper(validityBuffer, false);
   }

   /* need to refactor this to keep the logic in an single place and make callers
    * more intelligent. see handleSafe() for more comments on realloc
    */

   private ArrowBuf reallocBufferHelper(ArrowBuf buffer, final boolean offsetBuffer) {
      final int currentBufferCapacity = buffer.capacity();
      long baseSize  = (offsetBuffer ? offsetAllocationSizeInBytes
              : validityAllocationSizeInBytes);

      if (baseSize < (long)currentBufferCapacity) {
         baseSize = (long)currentBufferCapacity;
      }

      long newAllocationSize = baseSize * 2L;
      newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);

      if (newAllocationSize > MAX_ALLOCATION_SIZE) {
         throw new OversizedAllocationException("Unable to expand the buffer");
      }

      final ArrowBuf newBuf = allocator.buffer((int)newAllocationSize);
      newBuf.setBytes(0, buffer, 0, currentBufferCapacity);
      final int halfNewCapacity = newBuf.capacity() / 2;
      newBuf.setZero(halfNewCapacity, halfNewCapacity);
      buffer.release(1);
      buffer = newBuf;
      if (offsetBuffer) {
         offsetAllocationSizeInBytes = (int)newAllocationSize;
      }
      else {
         validityAllocationSizeInBytes = (int)newAllocationSize;
      }

      return buffer;
   }

   @Override
   public int getByteCapacity(){
      return valueBuffer.capacity();
   }

   @Override
   public int getCurrentSizeInBytes(){
      /* TODO */
      return 0;
   }

   @Override
   public int getBufferSize() {
      /* TODO */
      return 0;
   }

   @Override
   public int getBufferSizeFor(final int valueCount) {
      if (valueCount == 0) {
         return 0;
      }

      final int validityBufferSize = getValidityBufferSizeFromCount(valueCount);
      final int offsetBufferSize = (valueCount + 1) * OFFSET_WIDTH;
      /* get the end offset for this valueCount */
      final int dataBufferSize = offsetBuffer.getInt(valueCount * OFFSET_WIDTH);
      return validityBufferSize + offsetBufferSize + dataBufferSize;
   }

   @Override
   public Field getField() {
      return field;
   }

   @Override
   public ArrowBuf[] getBuffers(boolean clear) {
      final ArrowBuf[] buffers = new ArrowBuf[3];
      buffers[0] = validityBuffer;
      buffers[1] = offsetBuffer;
      buffers[1] = valueBuffer;
      if (clear) {
         for (final ArrowBuf buffer:buffers) {
            buffer.retain(1);
         }
         clear();
      }
      return buffers;
   }

   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
      return getTransferPair(ref, allocator);
   }

   @Override
   public TransferPair getTransferPair(BufferAllocator allocator){
      return getTransferPair(name, allocator);
   }

   public abstract TransferPair getTransferPair(String ref, BufferAllocator allocator);

   public void transferTo(BaseNullableVariableWidthVector target){
      compareTypes(target, "transferTo");
      target.clear();
      target.validityBuffer = validityBuffer.transferOwnership(target.allocator).buffer;
      target.valueBuffer = valueBuffer.transferOwnership(target.allocator).buffer;
      target.offsetBuffer = offsetBuffer.transferOwnership(target.allocator).buffer;
      target.valueCount = valueCount;
      target.setLastSet(lastSet);
      clear();
   }

   public void splitAndTransferTo(int startIndex, int length,
                                  BaseNullableVariableWidthVector target) {
      compareTypes(target, "splitAndTransferTo");
      target.clear();
      splitAndTransferValidityBuffer(startIndex, length, target);
      splitAndTransferOffsetBuffer(startIndex, length, target);
      target.setLastSet(length - 1);
      target.setValueCount(length);
   }

   /*
    * transfer the offsets along with data
    */
   private void splitAndTransferOffsetBuffer(int startIndex, int length, BaseNullableVariableWidthVector target) {
      final int start = offsetBuffer.getInt(startIndex * OFFSET_WIDTH);
      final int end = offsetBuffer.getInt((startIndex + length) * OFFSET_WIDTH);
      final int dataLength = end - start;
      target.allocateOffsetBuffer((length + 1) * OFFSET_WIDTH);
      for (int i = 0; i < length + 1; i++) {
         final int relativeSourceOffset = offsetBuffer.getInt((startIndex + i) * OFFSET_WIDTH) - start;
         target.offsetBuffer.setInt(i * OFFSET_WIDTH, relativeSourceOffset);
      }
      target.valueBuffer = valueBuffer.slice(start, dataLength).transferOwnership(target.allocator).buffer;
   }

   /*
    * transfer the validity.
    */
   private void splitAndTransferValidityBuffer(int startIndex, int length,
                                               BaseNullableVariableWidthVector target) {
      assert startIndex + length <= valueCount;
      int firstByteSource = BitVectorHelper.byteIndex(startIndex);
      int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
      int byteSizeTarget = getValidityBufferSizeFromCount(length);
      int offset = startIndex % 8;

      if (length > 0) {
         if (offset == 0) {
            // slice
            if (target.validityBuffer != null) {
               target.validityBuffer.release();
            }
            target.validityBuffer = validityBuffer.slice(firstByteSource, byteSizeTarget);
            target.validityBuffer.retain(1);
         }
         else {
            /* Copy data
             * When the first bit starts from the middle of a byte (offset != 0),
             * copy data from src BitVector.
             * Each byte in the target is composed by a part in i-th byte,
             * another part in (i+1)-th byte.
             */
            target.allocateValidityBuffer(byteSizeTarget);

            for (int i = 0; i < byteSizeTarget - 1; i++) {
               byte b1 = BitVectorHelper.getBitsFromCurrentByte(this.validityBuffer, firstByteSource + i, offset);
               byte b2 = BitVectorHelper.getBitsFromNextByte(this.validityBuffer, firstByteSource + i + 1, offset);

               target.validityBuffer.setByte(i, (b1 + b2));
            }

            /* Copying the last piece is done in the following manner:
             * if the source vector has 1 or more bytes remaining, we copy
             * the last piece as a byte formed by shifting data
             * from the current byte and the next byte.
             *
             * if the source vector has no more bytes remaining
             * (we are at the last byte), we copy the last piece as a byte
             * by shifting data from the current byte.
             */
            if((firstByteSource + byteSizeTarget - 1) < lastByteSource) {
               byte b1 = BitVectorHelper.getBitsFromCurrentByte(this.validityBuffer,
                       firstByteSource + byteSizeTarget - 1, offset);
               byte b2 = BitVectorHelper.getBitsFromNextByte(this.validityBuffer,
                       firstByteSource + byteSizeTarget, offset);

               target.validityBuffer.setByte(byteSizeTarget - 1, b1 + b2);
            }
            else {
               byte b1 = BitVectorHelper.getBitsFromCurrentByte(this.validityBuffer,
                       firstByteSource + byteSizeTarget - 1, offset);
               target.validityBuffer.setByte(byteSizeTarget - 1, b1);
            }
         }
      }
   }


   /******************************************************************
    *                                                                *
    *                common getters and setters                      *
    *                                                                *
    ******************************************************************/


   /**
    * Get the number of elements that are null in the vector
    *
    * @return the number of null elements.
    */
   public int getNullCount() {
      return BitVectorHelper.getNullCount(validityBuffer, valueCount);
   }

   /**
    * Check if the given index is within the current value capacity
    * of the vector
    *
    * @param index  position to check
    * @return true if index is within the current value capacity
    */
   public boolean isSafe(int index) {
      return index < getValueCapacity();
   }

   /**
    * Check if element at given index is null.
    *
    * @param index  position of element
    * @return true if element at given index is null
    */
   public boolean isNull(int index) {
      return (isSet(index) == 0);
   }

   /**
    * Same as {@link #isNull(int)}.
    *
    * @param index  position of element
    * @return 1 if element at given index is not null, 0 otherwise
    */
   public int isSet(int index) {
      final int byteIndex = index >> 3;
      final byte b = validityBuffer.getByte(byteIndex);
      final int bitIndex = index & 7;
      return Long.bitCount(b & (1L << bitIndex));
   }

   /**
    * Get the value count of vector. This will always be zero unless
    * setValueCount(int) has been called prior to calling this.
    *
    * @return valueCount for the vector
    */
   public int getValueCount(){
      return valueCount;
   }

   /**
    * Sets the value count for the vector
    *
    * @param valueCount   value count
    */
   public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      this.valueCount = valueCount;
      while (valueCount > getValueCapacity()) {
         reallocValidityAndOffsetBuffers();
      }
      fillHoles(valueCount);
      lastSet = valueCount - 1;
   }

   /**
    * Create holes in the vector upto the given index (exclusive).
    * Holes will be created from the current last set position in
    * the vector.
    *
    * @param index target index
    */
   public void fillEmpties(int index) {
      handleSafe(index, emptyByteArray.length);
      fillHoles(index);
      lastSet = index - 1;
   }

   /**
    * Set the index of last non-null element in the vector.
    * It is important to call this method with appropriate value
    * before calling {@link #setValueCount(int)}.
    *
    * @param value desired index of last non-null element.
    */
   public void setLastSet(int value) {
      lastSet = value;
   }

   /**
    * Get the index of last non-null element in the vector.
    *
    * @return index of the last non-null element
    */
   public int getLastSet() {
      return lastSet;
   }

   /**
    * Get the starting position (offset) in the data stream for a given
    * element in the vector.
    *
    * @param index position of the element in the vector
    * @return starting offset for the element
    */
   public long getStartEnd(int index) {
      return (long)offsetBuffer.getInt(index * OFFSET_WIDTH);
   }

   /**
    * Mark the particular position in the vector as non-null.
    *
    * @param index position of the element.
    */
   public void setIndexDefined(int index) {
      handleSafe(index, 0);
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
   }


   /******************************************************************
    *                                                                *
    *                helper methods for setters                      *
    *                                                                *
    ******************************************************************/


   protected final void fillHoles(int index) {
      for (int i = lastSet + 1; i < index; i++) {
         setBytes(i, emptyByteArray, 0, emptyByteArray.length);
      }
      lastSet = index - 1;
   }

   protected final void setBytes(int index, byte[] value, int start, int length) {
      /* end offset of current last element in the vector. this will
       * be the start offset of new element we are trying to store.
       */
      final int startOffset = getstartOffset(index);
      /* set new end offset */
      offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + length);
      /* store the var length data in value buffer */
      valueBuffer.setBytes(startOffset, value, start, length);
   }

   protected final int getstartOffset(int index) {
      return offsetBuffer.getInt(index * OFFSET_WIDTH);
   }

   protected final void handleSafe(int index, int dataLength) {
      /*
       * IMPORTANT:
       * value buffer for variable length vectors moves independent
       * of the companion validity and offset buffers. This is in
       * contrast to what we have for fixed width vectors.
       *
       * Here there is no concept of getValueCapacity() in the
       * data stream. getValueCapacity() is applicable only to validity
       * and offset buffers.
       *
       * So even though we may have setup an initial capacity of 1024
       * elements in the vector, it is quite possible
       * that we need to reAlloc() the data buffer when we are setting
       * the 5th element in the vector simply because previous
       * variable length elements have exhausted the buffer capacity.
       * However, we really don't need to reAlloc() validity and
       * offset buffers until we try to set the 1025th element
       * This is why we do a separate check for safe methods to
       * determine which buffer needs reallocation.
       */
      while (index >= getValueCapacity()) {
         reallocValidityAndOffsetBuffers();
      }
      final int startOffset = getstartOffset(index);
      while (valueBuffer.capacity() < (startOffset + dataLength)) {
         reallocValueBuffer();
      }
   }


   /******************************************************************
    *                                                                *
    *                helper methods currently                        *
    *                used by JsonFileReader and                      *
    *                JsonFileWriter                                  *
    *                                                                *
    ******************************************************************/


   /**
    * Method used by Json Reader to explicitly set the data of the variable
    * width vector elements. The method takes care of allocating the memory
    * for the vector if caller hasn't done so.
    *
    * External use of this method is not recommended.
    *
    * @param data ArrowBuf for storing variable width elements in the vector
    * @param offset offset of the element
    * @param allocator memory allocator
    * @param index position of the element in the vector
    * @param value array of bytes for the element
    * @param valueCount number of elements in the vector
    * @return buffer holding the variable width data.
    */
   public static ArrowBuf set(ArrowBuf data, ArrowBuf offset,
                              BufferAllocator allocator, int index, byte[] value,
                              int valueCount) {
      if (data == null) {
         data = allocator.buffer(INITIAL_BYTE_COUNT);
      }
      final int currentBufferCapacity = data.capacity();
      final int currentStartOffset = offset.getInt(index * OFFSET_WIDTH);
      while (currentBufferCapacity < currentStartOffset + value.length) {
         final ArrowBuf newBuf = allocator.buffer(currentBufferCapacity * 2);
         newBuf.setBytes(0, data, 0, currentBufferCapacity);
         data.release();
         data = newBuf;
      }
      data.setBytes(currentStartOffset, value, 0, value.length);
      if (index == (valueCount - 1)) {
         data.writerIndex(offset.getInt(valueCount * OFFSET_WIDTH));
      }
      return data;
   }

   /**
    * Method used by Json Writer to read a variable width element from
    * the variable width vector and write to Json.
    *
    * External use of this method is not recommended.
    *
    * @param data buffer storing the variable width vector elements
    * @param offset buffer storing the offsets of variable width vector elements
    * @param index position of the element in the vector
    * @return array of bytes
    */
   public static byte[] get(final ArrowBuf data, final ArrowBuf offset, int index) {
      final int currentStartOffset = offset.getInt(index * OFFSET_WIDTH);
      final int dataLength =
              offset.getInt((index + 1) * OFFSET_WIDTH) - currentStartOffset;
      final byte[] result = new byte[dataLength];
      data.getBytes(currentStartOffset, result, 0, dataLength);
      return result;
   }

   /**
    * Method used by Json Reader to explicitly set the offsets of the variable
    * width vector data. The method takes care of allocating the memory for
    * offsets if the caller hasn't done so.
    *
    * External use of this method is not recommended.
    *
    * @param buffer ArrowBuf to store offsets for variable width elements
    * @param allocator memory allocator
    * @param valueCount number of elements
    * @param index position of the element
    * @param value offset of the element
    * @return buffer holding the offsets
    */
   public static ArrowBuf set(ArrowBuf buffer, BufferAllocator allocator,
                              int valueCount, int index, int value) {
      if (buffer == null) {
         buffer = allocator.buffer(valueCount * OFFSET_WIDTH);
      }
      buffer.setInt(index * OFFSET_WIDTH, value);
      if (index == (valueCount - 1)) {
         buffer.writerIndex(valueCount * OFFSET_WIDTH);
      }

      return buffer;
   }
}
