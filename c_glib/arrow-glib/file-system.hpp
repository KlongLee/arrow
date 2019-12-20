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

#include <arrow/filesystem/api.h>

#include <arrow-glib/file-system.h>

GArrowFileStats *
garrow_file_stats_new_raw(std::shared_ptr<arrow::fs::FileStats> *arrow_file_stats);

std::shared_ptr<arrow::fs::FileStats>
garrow_file_stats_get_raw(GArrowFileStats *file_stats);

GArrowFileSelector *
garrow_file_selector_new_raw(std::shared_ptr<arrow::fs::FileSelector> *arrow_file_selector);

std::shared_ptr<arrow::fs::FileSelector>
garrow_file_selector_get_raw(GArrowFileSelector *file_selector);

GArrowFileSystem *
garrow_file_system_new_raw(std::shared_ptr<arrow::fs::FileSystem> *arrow_file_system,
                           GType type);

std::shared_ptr<arrow::fs::FileSystem>
garrow_file_system_get_raw(GArrowFileSystem *file_system);

