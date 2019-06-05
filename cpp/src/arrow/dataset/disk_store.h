// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

namespace arrow {

namespace fs {

class FileSystem;

}  // namespace fs

namespace dataset {

/// \brief Loads a previously-written collection of Arrow protocol
/// files and exposes them in a way that can be consumed as a Dataset
/// source
class ARROW_DS_EXPORT DiskStoreReader : public DatasetSource {
 public:
  DiskStoreReader(const std::string& path, fs::FileSystem* filesystem);

 private:
  class DiskStoreReaderImpl;
  std::unique_ptr<DiskStoreReaderImpl> impl_;

  std::string path_;
  fs::FileSystem* filesystem_;

  DiskStoreReader() {}
};

/// \brief
class ARROW_DS_EXPORT DiskStoreWriter {
 public:
  Status Write(const RecordBatch& batch);

 private:
  DiskStoreWriter() {}
};

}  // namespace dataset
}  // namespace arrow
