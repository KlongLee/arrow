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

#include <memory>
#include <string>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/util/macros.h"
#include "arrow/util/uri.h"

namespace Azure::Core::Credentials {
class TokenCredential;
}

namespace Azure::Storage {
class StorageSharedKeyCredential;
}

namespace Azure::Storage::Blobs {
class BlobServiceClient;
}

namespace Azure::Storage::Files::DataLake {
class DataLakeServiceClient;
}

namespace arrow::fs {

enum class AzureBackend {
  /// \brief Official Azure Remote Backend
  kAzure,
  /// \brief Local Simulated Storage
  kAzurite
};

/// Options for the AzureFileSystem implementation.
struct ARROW_EXPORT AzureOptions {
  /// \brief The backend to connect to: Azure or Azurite (for testing).
  AzureBackend backend = AzureBackend::kAzure;

  // TODO(GH-38598): Add support for more auth methods.
  // std::string connection_string;
  // std::string sas_token;

  /// \brief Default metadata for OpenOutputStream.
  ///
  /// This will be ignored if non-empty metadata is passed to OpenOutputStream.
  std::shared_ptr<const KeyValueMetadata> default_metadata;

 private:
  std::string account_blob_url_;
  std::string account_dfs_url_;

  enum class CredentialKind {
    kAnonymous,
    kStorageSharedKeyCredential,
  } credential_kind_ = CredentialKind::kAnonymous;

  std::shared_ptr<Azure::Storage::StorageSharedKeyCredential>
      storage_shared_key_credential_;

 public:
  AzureOptions();
  ~AzureOptions();

  Status ConfigureAccountKeyCredential(const std::string& account_name,
                                       const std::string& account_key);

  bool Equals(const AzureOptions& other) const;

  const std::string& AccountBlobUrl() const { return account_blob_url_; }
  const std::string& AccountDfsUrl() const { return account_dfs_url_; }

  Result<std::unique_ptr<Azure::Storage::Blobs::BlobServiceClient>>
  MakeBlobServiceClient() const;

  Result<std::unique_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient>>
  MakeDataLakeServiceClient() const;
};

/// \brief FileSystem implementation backed by Azure Blob Storage (ABS) [1] and
/// Azure Data Lake Storage Gen2 (ADLS Gen2) [2].
///
/// ADLS Gen2 isn't a dedicated service or account type. It's a set of capabilities that
/// support high throughput analytic workloads, built on Azure Blob Storage. All the data
/// ingested via the ADLS Gen2 APIs is persisted as blobs in the storage account.
/// ADLS Gen2 provides filesystem semantics, file-level security, and Hadoop
/// compatibility. ADLS Gen1 exists as a separate object that will retired on 2024-02-29
/// and new ADLS accounts use Gen2 instead.
///
/// ADLS Gen2 and Blob APIs can operate on the same data, but there are
/// some limitations [3]. The ones that are relevant to this
/// implementation are listed here:
///
/// - You can't use Blob APIs, and ADLS APIs to write to the same instance of a file. If
///   you write to a file by using ADLS APIs then that file's blocks won't be visible
///   to calls to the GetBlockList Blob API. The only exception is when you're
///   overwriting.
/// - When you use the ListBlobs operation without specifying a delimiter, the results
///   include both directories and blobs. If you choose to use a delimiter, use only a
///   forward slash (/) -- the only supported delimiter.
/// - If you use the DeleteBlob API to delete a directory, that directory is deleted only
///   if it's empty. This means that you can't use the Blob API delete directories
///   recursively.
///
/// [1]: https://azure.microsoft.com/en-us/products/storage/blobs
/// [2]: https://azure.microsoft.com/en-us/products/storage/data-lake-storage
/// [3]:
/// https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-known-issues
class ARROW_EXPORT AzureFileSystem : public FileSystem {
 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  explicit AzureFileSystem(std::unique_ptr<Impl>&& impl);

 public:
  ~AzureFileSystem() override = default;

  static Result<std::shared_ptr<AzureFileSystem>> Make(
      const AzureOptions& options, const io::IOContext& = io::default_io_context());

  std::string type_name() const override { return "abfs"; }

  /// Return the original Azure options when constructing the filesystem
  const AzureOptions& options() const;

  bool Equals(const FileSystem& other) const override;

  Result<FileInfo> GetFileInfo(const std::string& path) override;

  Result<FileInfoVector> GetFileInfo(const FileSelector& select) override;

  Status CreateDir(const std::string& path, bool recursive = true) override;

  Status DeleteDir(const std::string& path) override;

  Status DeleteDirContents(const std::string& path, bool missing_dir_ok = false) override;

  Status DeleteRootDirContents() override;

  Status DeleteFile(const std::string& path) override;

  Status Move(const std::string& src, const std::string& dest) override;

  Status CopyFile(const std::string& src, const std::string& dest) override;

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override;

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(const FileInfo& info) override;

  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;

  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const FileInfo& info) override;

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata = {}) override;

  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata = {}) override;
};

}  // namespace arrow::fs
