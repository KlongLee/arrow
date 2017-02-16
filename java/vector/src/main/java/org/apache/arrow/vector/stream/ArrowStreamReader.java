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
package org.apache.arrow.vector.stream;

import com.google.common.base.Preconditions;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.file.ReadChannel;
import org.apache.arrow.vector.schema.ArrowDictionaryBatch;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * This classes reads from an input stream and produces ArrowRecordBatches.
 */
public class ArrowStreamReader implements AutoCloseable {
  private ReadChannel in;
  private final BufferAllocator allocator;
  private Schema schema;
  private Message nextMessage;

  /**
   * Constructs a streaming read, reading bytes from 'in'. Non-blocking.
   */
  public ArrowStreamReader(ReadableByteChannel in, BufferAllocator allocator) {
    super();
    this.in = new ReadChannel(in);
    this.allocator = allocator;
  }

  public ArrowStreamReader(InputStream in, BufferAllocator allocator) {
    this(Channels.newChannel(in), allocator);
  }

  /**
   * Initializes the reader. Must be called before the other APIs. This is blocking.
   */
  public void init() throws IOException {
    Preconditions.checkState(this.schema == null, "Cannot call init() more than once.");
    this.schema = readSchema();
  }

  /**
   * Returns the schema for all records in this stream.
   */
  public Schema getSchema () {
    Preconditions.checkState(this.schema != null, "Must call init() first.");
    return schema;
  }

  public long bytesRead() { return in.bytesRead(); }

  /**
   * Reads and returns the type of the next batch. Returns null if this is the end of the stream.
   *
   * @return org.apache.arrow.flatbuf.MessageHeader type
   * @throws IOException
   */
  public Byte nextBatchType() throws IOException {
    nextMessage = MessageSerializer.deserializeMessage(in);
    if (nextMessage == null) {
      return null;
    } else {
      return nextMessage.headerType();
    }
  }

  /**
   * Reads and returns the next ArrowRecordBatch. Returns null if this is the end
   * of stream.
   */
  public ArrowDictionaryBatch nextDictionaryBatch() throws IOException {
    Preconditions.checkState(this.in != null, "Cannot call after close()");
    Preconditions.checkState(this.schema != null, "Must call init() first.");
    Preconditions.checkState(this.nextMessage.headerType() == MessageHeader.DictionaryBatch,
                             "Must call nextBatchType() and receive MessageHeader.DictionaryBatch.");
    return MessageSerializer.deserializeDictionaryBatch(in, nextMessage, allocator);
  }

  /**
   * Reads and returns the next ArrowRecordBatch. Returns null if this is the end
   * of stream.
   */
  public ArrowRecordBatch nextRecordBatch() throws IOException {
    Preconditions.checkState(this.in != null, "Cannot call after close()");
    Preconditions.checkState(this.schema != null, "Must call init() first.");
    Preconditions.checkState(this.nextMessage.headerType() == MessageHeader.RecordBatch,
                             "Must call nextBatchType() and receive MessageHeader.RecordBatch.");
    return MessageSerializer.deserializeRecordBatch(in, nextMessage, allocator);
  }

  @Override
  public void close() throws IOException {
    if (this.in != null) {
      in.close();
      in = null;
    }
  }

  /**
   * Reads the schema message from the beginning of the stream.
   */
  private Schema readSchema() throws IOException {
    return MessageSerializer.deserializeSchema(in);
  }
}
