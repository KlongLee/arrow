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

package org.apache.arrow.flight;

import org.apache.arrow.flight.perf.impl.PerfOuterClass;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.protobuf.StatusProto;

public class TestErrorMetadata {
  private static final Metadata.BinaryMarshaller<Status> marshaller =
          ProtoUtils.metadataMarshaller(Status.getDefaultInstance());

  @Test
  public void testMetadata() throws Exception {
    PerfOuterClass.Perf perf = PerfOuterClass.Perf.newBuilder()
                .setStreamCount(12)
                .setRecordsPerBatch(1000)
                .setRecordsPerStream(1000000L)
                .build();
    try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final FlightServer s =
             FlightTestUtil.getStartedServer(
               (location) -> FlightServer.builder(allocator, location, new TestFlightProducer(perf)).build());
          final FlightClient client = FlightClient.builder(allocator, s.getLocation()).build()) {
      FlightStream stream = client.getStream(new Ticket("abs".getBytes()));
      stream.next();
      Assert.fail();
    } catch (FlightRuntimeException fre) {
      PerfOuterClass.Perf newPerf = null;
      FlightMetadata metadata = fre.status().metadata();
      Assert.assertNotNull(metadata);
      Assert.assertEquals(2, metadata.size());
      Assert.assertTrue(metadata.containsKey("grpc-status-details-bin"));
      Status status = marshaller.parseBytes(metadata.get("grpc-status-details-bin"));
      for (Any details : status.getDetailsList()) {
        if (details.is(PerfOuterClass.Perf.class)) {
          try {
            newPerf = details.unpack(PerfOuterClass.Perf.class);
          } catch (InvalidProtocolBufferException e) {
            Assert.fail();
          }
        }
      }
      if (newPerf == null) {
        Assert.fail();
      }
      Assert.assertEquals(perf, newPerf);
    }
  }

  private static class TestFlightProducer extends NoOpFlightProducer {
    private final PerfOuterClass.Perf perf;

    private TestFlightProducer(PerfOuterClass.Perf perf) {
      this.perf = perf;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      StatusRuntimeException sre = StatusProto.toStatusRuntimeException(Status.newBuilder()
              .setCode(1)
              .setMessage("Testing 1 2 3")
              .addDetails(Any.pack(perf, "arrow/meta/types"))
              .build());
      listener.error(sre);
    }
  }
}
