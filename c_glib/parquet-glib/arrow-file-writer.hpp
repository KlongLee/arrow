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

#pragma once

#include <memory>

#include <parquet/arrow/writer.h>

#include <parquet-glib/arrow-file-writer.h>

GParquetArrowFileWriter *
gparquet_arrow_file_writer_new_raw(parquet::arrow::FileWriter *parquet_arrow_file_writer);
parquet::arrow::FileWriter *
gparquet_arrow_file_writer_get_raw(GParquetArrowFileWriter *arrow_file_writer);
GParquetWriterProperties *
gparquet_writer_properties_new_raw(parquet::WriterProperties::Builder *parquet_builder);
std::shared_ptr<parquet::WriterProperties>
gparquet_writer_properties_get_raw(GParquetWriterProperties *properties);
