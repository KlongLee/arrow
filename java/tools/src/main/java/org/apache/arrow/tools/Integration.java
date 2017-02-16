/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.arrow.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.file.ArrowBlock;
import org.apache.arrow.vector.file.ArrowFooter;
import org.apache.arrow.vector.file.ArrowReader;
import org.apache.arrow.vector.file.ArrowWriter;
import org.apache.arrow.vector.file.json.JsonFileReader;
import org.apache.arrow.vector.file.json.JsonFileWriter;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Validator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Integration {
  private static final Logger LOGGER = LoggerFactory.getLogger(Integration.class);

  public static void main(String[] args) {
    try {
      new Integration().run(args);
    } catch (ParseException e) {
      fatalError("Invalid parameters", e);
    } catch (IOException e) {
      fatalError("Error accessing files", e);
    } catch (RuntimeException e) {
      fatalError("Incompatible files", e);
    }
  }

  private final Options options;

  enum Command {
    ARROW_TO_JSON(true, false) {
      @Override
      public void execute(File arrowFile, File jsonFile) throws IOException {
        try(
            BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
            FileInputStream fileInputStream = new FileInputStream(arrowFile);
            ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), allocator);) {
          ArrowFooter footer = arrowReader.readFooter();
          Schema schema = footer.getSchema();
          LOGGER.debug("Input file size: " + arrowFile.length());
          LOGGER.debug("Found schema: " + schema);
          try (JsonFileWriter writer = new JsonFileWriter(jsonFile, JsonFileWriter.config().pretty(true));) {
            writer.start(schema);
            List<ArrowBlock> recordBatches = footer.getRecordBatches();
            for (ArrowBlock rbBlock : recordBatches) {
              try (ArrowRecordBatch inRecordBatch = arrowReader.readRecordBatch(rbBlock);
                   VectorLoader vectorLoader = new VectorLoader(schema, allocator);) {
                VectorSchemaRoot root = vectorLoader.getVectorSchemaRoot();
                vectorLoader.load(inRecordBatch);
                writer.write(root);
              }
            }
          }
          LOGGER.debug("Output file size: " + jsonFile.length());
        }
      }
    },
    JSON_TO_ARROW(false, true) {
      @Override
      public void execute(File arrowFile, File jsonFile) throws IOException {
        try (
            BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
            JsonFileReader reader = new JsonFileReader(jsonFile, allocator);
            ) {
          Schema schema = reader.start();
          LOGGER.debug("Input file size: " + jsonFile.length());
          LOGGER.debug("Found schema: " + schema);
          try (
              FileOutputStream fileOutputStream = new FileOutputStream(arrowFile);
              ArrowWriter arrowWriter = new ArrowWriter(fileOutputStream.getChannel(), schema);
              ) {

            // initialize vectors
            VectorSchemaRoot root;
            while ((root = reader.read()) != null) {
              VectorUnloader vectorUnloader = new VectorUnloader(root);
              try (ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();) {
                arrowWriter.writeRecordBatch(recordBatch);
              }
              root.close();
            }
          }
          LOGGER.debug("Output file size: " + arrowFile.length());
        }
      }
    },
    VALIDATE(true, true) {
      @Override
      public void execute(File arrowFile, File jsonFile) throws IOException {
        try (
            BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
            JsonFileReader jsonReader = new JsonFileReader(jsonFile, allocator);
            FileInputStream fileInputStream = new FileInputStream(arrowFile);
            ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), allocator);
            ) {
          Schema jsonSchema = jsonReader.start();
          ArrowFooter footer = arrowReader.readFooter();
          Schema arrowSchema = footer.getSchema();
          LOGGER.debug("Arrow Input file size: " + arrowFile.length());
          LOGGER.debug("ARROW schema: " + arrowSchema);
          LOGGER.debug("JSON Input file size: " + jsonFile.length());
          LOGGER.debug("JSON schema: " + jsonSchema);
          Validator.compareSchemas(jsonSchema, arrowSchema);

          List<ArrowBlock> recordBatches = footer.getRecordBatches();
          Iterator<ArrowBlock> iterator = recordBatches.iterator();
          VectorSchemaRoot jsonRoot;
          while ((jsonRoot = jsonReader.read()) != null && iterator.hasNext()) {
            ArrowBlock rbBlock = iterator.next();
            try (ArrowRecordBatch inRecordBatch = arrowReader.readRecordBatch(rbBlock);
                 VectorLoader vectorLoader = new VectorLoader(arrowSchema, allocator);) {
              VectorSchemaRoot arrowRoot = vectorLoader.getVectorSchemaRoot();
              vectorLoader.load(inRecordBatch);
              Validator.compareVectorSchemaRoot(arrowRoot, jsonRoot);
            }
            jsonRoot.close();
          }
          boolean hasMoreJSON = jsonRoot != null;
          boolean hasMoreArrow = iterator.hasNext();
          if (hasMoreJSON || hasMoreArrow) {
            throw new IllegalArgumentException("Unexpected RecordBatches. J:" + hasMoreJSON + " A:" + hasMoreArrow);
          }
        }
      }
    };

    public final boolean arrowExists;
    public final boolean jsonExists;

    Command(boolean arrowExists, boolean jsonExists) {
      this.arrowExists = arrowExists;
      this.jsonExists = jsonExists;
    }

    abstract public void execute(File arrowFile, File jsonFile) throws IOException;

  }

  Integration() {
    this.options = new Options();
    this.options.addOption("a", "arrow", true, "arrow file");
    this.options.addOption("j", "json", true, "json file");
    this.options.addOption("c", "command", true, "command to execute: " + Arrays.toString(Command.values()));
  }

  private File validateFile(String type, String fileName, boolean shouldExist) {
    if (fileName == null) {
      throw new IllegalArgumentException("missing " + type + " file parameter");
    }
    File f = new File(fileName);
    if (shouldExist && (!f.exists() || f.isDirectory())) {
      throw new IllegalArgumentException(type + " file not found: " + f.getAbsolutePath());
    }
    if (!shouldExist && f.exists()) {
      throw new IllegalArgumentException(type + " file already exists: " + f.getAbsolutePath());
    }
    return f;
  }

  void run(String[] args) throws ParseException, IOException {
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args, false);


    Command command = toCommand(cmd.getOptionValue("command"));
    File arrowFile = validateFile("arrow", cmd.getOptionValue("arrow"), command.arrowExists);
    File jsonFile = validateFile("json", cmd.getOptionValue("json"), command.jsonExists);
    command.execute(arrowFile, jsonFile);
  }

  private Command toCommand(String commandName) {
    try {
      return Command.valueOf(commandName);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown command: " + commandName + " expected one of " + Arrays.toString(Command.values()));
    }
  }

  private static void fatalError(String message, Throwable e) {
    System.err.println(message);
    System.err.println(e.getMessage());
    LOGGER.error(message, e);
    System.exit(1);
  }

}
