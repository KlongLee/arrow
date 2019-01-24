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

package org.apache.arrow.flight.example.integration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * An Example Flight Server that provides access to the InMemoryStore.
 */
public class IntegrationTestClient {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(IntegrationTestClient.class);
  private final Options options;

  public IntegrationTestClient() {
    this.options = new Options();
    this.options.addOption("a", "arrow", true, "arrow file");
    this.options.addOption("j", "json", true, "json file");
  }

  public static void main(String[] args) {
    try {
      new IntegrationTestClient().run(args);
    } catch (ParseException e) {
      fatalError("Invalid parameters", e);
    } catch (IOException e) {
      fatalError("Error accessing files", e);
    }
  }

  private static void fatalError(String message, Throwable e) {
    System.err.println(message);
    System.err.println(e.getMessage());
    LOGGER.error(message, e);
    System.exit(1);
  }

  void run(String[] args) throws ParseException, IOException {
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args, false);

    String fileName = cmd.getOptionValue("arrow");
    if (fileName == null) {
      throw new IllegalArgumentException("missing arrow file parameter");
    }
    File arrowFile = new File(fileName);
    if (arrowFile.exists()) {
      throw new IllegalArgumentException("arrow file already exists: " + arrowFile.getAbsolutePath());
    }

    final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    FlightClient client = new FlightClient(allocator, new Location("localhost", 31337));
    FlightInfo info = client.getInfo(FlightDescriptor.path(cmd.getOptionValue("json")));
    FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket());
    try (VectorSchemaRoot root = stream.getRoot();
         FileOutputStream fileOutputStream = new FileOutputStream(arrowFile);
         ArrowFileWriter arrowWriter = new ArrowFileWriter(root, new DictionaryProvider.MapDictionaryProvider(),
                 fileOutputStream.getChannel())) {
      while (stream.next()) {
        arrowWriter.writeBatch();
      }
    }
  }
}
