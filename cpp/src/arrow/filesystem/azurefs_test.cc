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

#include <algorithm>  // Missing include in boost/process

// This boost/asio/io_context.hpp include is needless for no MinGW
// build.
//
// This is for including boost/asio/detail/socket_types.hpp before any
// "#include <windows.h>". boost/asio/detail/socket_types.hpp doesn't
// work if windows.h is already included. boost/process.h ->
// boost/process/args.hpp -> boost/process/detail/basic_cmd.hpp
// includes windows.h. boost/process/args.hpp is included before
// boost/process/async.h that includes
// boost/asio/detail/socket_types.hpp implicitly is included.
#include <boost/asio/io_context.hpp>
// We need BOOST_USE_WINDOWS_H definition with MinGW when we use
// boost/process.hpp. See BOOST_USE_WINDOWS_H=1 in
// cpp/cmake_modules/ThirdpartyToolchain.cmake for details.
#include <boost/process.hpp>

#include "arrow/filesystem/azurefs.h"
#include "arrow/util/io_util.h"

#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

#include <string>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

#include <azure/identity/client_secret_credential.hpp>
#include <azure/identity/default_azure_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/storage/files/datalake.hpp>

namespace arrow {
using internal::TemporaryDir;
namespace fs {
namespace {
namespace bp = boost::process;

using ::testing::IsEmpty;
using ::testing::Not;
using ::testing::NotNull;

class AzuriteEnv : public ::testing::Environment {
 public:
  AzuriteEnv() {
    account_name_ = "devstoreaccount1";
    account_key_ =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/"
        "KBHBeksoGMGw==";
    auto exe_path = bp::search_path("azurite");
    if (exe_path.empty()) {
      auto error = std::string("Could not find Azurite emulator.");
      status_ = Status::Invalid(error);
      return;
    }
    auto temp_dir_ = *TemporaryDir::Make("azurefs-test-");
    server_process_ = bp::child(boost::this_process::environment(), exe_path, "--silent",
                                "--location", temp_dir_->path().ToString(), "--debug",
                                temp_dir_->path().ToString() + "/debug.log");
    if (!(server_process_.valid() && server_process_.running())) {
      auto error = "Could not start Azurite emulator.";
      server_process_.terminate();
      server_process_.wait();
      status_ = Status::Invalid(error);
      return;
    }
    status_ = Status::OK();
  }

  ~AzuriteEnv() override {
    server_process_.terminate();
    server_process_.wait();
  }

  const std::string& account_name() const { return account_name_; }
  const std::string& account_key() const { return account_key_; }
  const Status status() const { return status_; }

 private:
  std::string account_name_;
  std::string account_key_;
  bp::child server_process_;
  Status status_;
  std::unique_ptr<TemporaryDir> temp_dir_;
};

auto* azurite_env = ::testing::AddGlobalTestEnvironment(new AzuriteEnv);

AzuriteEnv* GetAzuriteEnv() {
  return ::arrow::internal::checked_cast<AzuriteEnv*>(azurite_env);
}

// Placeholder tests
// TODO: GH-18014 Remove once a proper test is added
TEST(AzureFileSystem, UploadThenDownload) {
  const std::string container_name = "sample-container";
  const std::string blob_name = "sample-blob.txt";
  const std::string blob_content = "Hello Azure!";

  const std::string& account_name = GetAzuriteEnv()->account_name();
  const std::string& account_key = GetAzuriteEnv()->account_key();

  auto credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
      account_name, account_key);

  auto service_client = Azure::Storage::Blobs::BlobServiceClient(
      std::string("http://127.0.0.1:10000/") + account_name, credential);
  auto container_client = service_client.GetBlobContainerClient(container_name);
  container_client.CreateIfNotExists();
  auto blob_client = container_client.GetBlockBlobClient(blob_name);

  std::vector<uint8_t> buffer(blob_content.begin(), blob_content.end());
  blob_client.UploadFrom(buffer.data(), buffer.size());

  std::vector<uint8_t> downloaded_content(blob_content.size());
  blob_client.DownloadTo(downloaded_content.data(), downloaded_content.size());

  EXPECT_EQ(std::string(downloaded_content.begin(), downloaded_content.end()),
            blob_content);
}

TEST(AzureFileSystem, InitializeCredentials) {
  auto default_credential = std::make_shared<Azure::Identity::DefaultAzureCredential>();
  auto managed_identity_credential =
      std::make_shared<Azure::Identity::ManagedIdentityCredential>();
  auto service_principal_credential =
      std::make_shared<Azure::Identity::ClientSecretCredential>("tenant_id", "client_id",
                                                                "client_secret");
}

TEST(AzureFileSystem, OptionsCompare) {
  AzureOptions options;
  EXPECT_TRUE(options.Equals(options));
}

class TestAzureFileSystem : public ::testing::Test {
 public:
  std::shared_ptr<FileSystem> fs_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient> gen2_client_;
  AzureOptions options_;

  void MakeFileSystem() {
    const std::string& account_name = GetAzuriteEnv()->account_name();
    const std::string& account_key = GetAzuriteEnv()->account_key();
    options_.backend = AzureBackend::Azurite;
    ASSERT_OK(options_.ConfigureAccountKeyCredentials(account_name, account_key));
    gen2_client_ =
        std::make_shared<Azure::Storage::Files::DataLake::DataLakeServiceClient>(
            options_.account_dfs_url, options_.storage_credentials_provider);
    ASSERT_OK_AND_ASSIGN(fs_, AzureFileSystem::Make(options_));
  }

  void SetUp() override {
    ASSERT_THAT(GetAzuriteEnv(), NotNull());
    ASSERT_OK(GetAzuriteEnv()->status());

    MakeFileSystem();
    auto file_system_client = gen2_client_->GetFileSystemClient("container");
    file_system_client.CreateIfNotExists();
    file_system_client = gen2_client_->GetFileSystemClient("empty-container");
    file_system_client.CreateIfNotExists();
    auto file_client =
        std::make_shared<Azure::Storage::Files::DataLake::DataLakeFileClient>(
            options_.account_blob_url + "container/somefile",
            options_.storage_credentials_provider);
    std::string s = "some data";
    file_client->UploadFrom(
        const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(s.data())), s.size());
  }

  void TearDown() override {
    auto containers = gen2_client_->ListFileSystems();
    for (auto container : containers.FileSystems) {
      auto file_system_client = gen2_client_->GetFileSystemClient(container.Name);
      file_system_client.DeleteIfExists();
    }
  }

  void AssertObjectContents(
      Azure::Storage::Files::DataLake::DataLakeServiceClient* client,
      const std::string& container, const std::string& path_to_file,
      const std::string& expected) {
    auto path_client =
        std::make_shared<Azure::Storage::Files::DataLake::DataLakePathClient>(
            client->GetUrl() + container + "/" + path_to_file,
            options_.storage_credentials_provider);
    auto size = path_client->GetProperties().Value.FileSize;
    if (size == 0) {
      ASSERT_EQ(expected, "");
      return;
    }
    auto buf = AllocateBuffer(size, fs_->io_context().pool());
    Azure::Storage::Blobs::DownloadBlobToOptions download_options;
    Azure::Core::Http::HttpRange range;
    range.Offset = 0;
    range.Length = size;
    download_options.Range = Azure::Nullable<Azure::Core::Http::HttpRange>(range);
    auto file_client =
        std::make_shared<Azure::Storage::Files::DataLake::DataLakeFileClient>(
            client->GetUrl() + container + "/" + path_to_file,
            options_.storage_credentials_provider);
    auto result = file_client
                      ->DownloadTo(reinterpret_cast<uint8_t*>(buf->get()->mutable_data()),
                                   size, download_options)
                      .Value;
    auto buf_data = std::move(buf->get());
    auto expected_data = std::make_shared<Buffer>(
        reinterpret_cast<const uint8_t*>(expected.data()), expected.size());
    AssertBufferEqual(*buf_data, *expected_data);
  }
};

// TEST_F(TestAzureFileSystem, FromAccountKey) {
//   auto options = AzureOptions::FromAccountKey(GetAzuriteEnv()->account_name(),
//                                               GetAzuriteEnv()->account_key())
//                      .ValueOrDie();
//   ASSERT_EQ(options.credentials_kind,
//             arrow::fs::AzureCredentialsKind::StorageCredentials);
//   ASSERT_NE(options.storage_credentials_provider, nullptr);
// }

// TEST_F(GcsIntegrationTest, OpenInputFileMixedReadVsReadAt) {
//   auto fs = GcsFileSystem::Make(TestGcsOptions());

//   // Create a file large enough to make the random access tests non-trivial.
//   auto constexpr kLineWidth = 100;
//   auto constexpr kLineCount = 4096;
//   std::vector<std::string> lines(kLineCount);
//   int lineno = 0;
//   std::generate_n(lines.begin(), lines.size(),
//                   [&] { return RandomLine(++lineno, kLineWidth); });

//   const auto path =
//       PreexistingBucketPath() + "OpenInputFileMixedReadVsReadAt/object-name";
//   std::shared_ptr<io::OutputStream> output;
//   ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
//   for (auto const& line : lines) {
//     ASSERT_OK(output->Write(line.data(), line.size()));
//   }
//   ASSERT_OK(output->Close());

//   std::shared_ptr<io::RandomAccessFile> file;
//   ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
//   for (int i = 0; i != 32; ++i) {
//     SCOPED_TRACE("Iteration " + std::to_string(i));
//     // Verify sequential reads work as expected.
//     std::array<char, kLineWidth> buffer{};
//     std::int64_t size;
//     {
//       ASSERT_OK_AND_ASSIGN(auto actual, file->Read(kLineWidth));
//       EXPECT_EQ(lines[2 * i], actual->ToString());
//     }
//     {
//       ASSERT_OK_AND_ASSIGN(size, file->Read(buffer.size(), buffer.data()));
//       EXPECT_EQ(size, kLineWidth);
//       auto actual = std::string{buffer.begin(), buffer.end()};
//       EXPECT_EQ(lines[2 * i + 1], actual);
//     }

//     // Verify random reads interleave too.
//     auto const index = RandomIndex(kLineCount);
//     auto const position = index * kLineWidth;
//     ASSERT_OK_AND_ASSIGN(size, file->ReadAt(position, buffer.size(), buffer.data()));
//     EXPECT_EQ(size, kLineWidth);
//     auto actual = std::string{buffer.begin(), buffer.end()};
//     EXPECT_EQ(lines[index], actual);

//     // Verify random reads using buffers work.
//     ASSERT_OK_AND_ASSIGN(auto b, file->ReadAt(position, kLineWidth));
//     EXPECT_EQ(lines[index], b->ToString());
//   }
// }

// TEST_F(GcsIntegrationTest, OpenInputFileRandomSeek) {
//   auto fs = GcsFileSystem::Make(TestGcsOptions());

//   // Create a file large enough to make the random access tests non-trivial.
//   auto constexpr kLineWidth = 100;
//   auto constexpr kLineCount = 4096;
//   std::vector<std::string> lines(kLineCount);
//   int lineno = 0;
//   std::generate_n(lines.begin(), lines.size(),
//                   [&] { return RandomLine(++lineno, kLineWidth); });

//   const auto path = PreexistingBucketPath() + "OpenInputFileRandomSeek/object-name";
//   std::shared_ptr<io::OutputStream> output;
//   ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
//   for (auto const& line : lines) {
//     ASSERT_OK(output->Write(line.data(), line.size()));
//   }
//   ASSERT_OK(output->Close());

//   std::shared_ptr<io::RandomAccessFile> file;
//   ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
//   for (int i = 0; i != 32; ++i) {
//     SCOPED_TRACE("Iteration " + std::to_string(i));
//     // Verify sequential reads work as expected.
//     auto const index = RandomIndex(kLineCount);
//     auto const position = index * kLineWidth;
//     ASSERT_OK(file->Seek(position));
//     ASSERT_OK_AND_ASSIGN(auto actual, file->Read(kLineWidth));
//     EXPECT_EQ(lines[index], actual->ToString());
//   }
// }

// TEST_F(GcsIntegrationTest, OpenInputFileIoContext) {
//   auto fs = GcsFileSystem::Make(TestGcsOptions());

//   // Create a test file.
//   const auto path = PreexistingBucketPath() + "OpenInputFileIoContext/object-name";
//   std::shared_ptr<io::OutputStream> output;
//   ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
//   const std::string contents = "The quick brown fox jumps over the lazy dog";
//   ASSERT_OK(output->Write(contents.data(), contents.size()));
//   ASSERT_OK(output->Close());

//   std::shared_ptr<io::RandomAccessFile> file;
//   ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
//   EXPECT_EQ(fs->io_context().external_id(), file->io_context().external_id());
// }

// TEST_F(GcsIntegrationTest, OpenInputFileInfo) {
//   auto fs = GcsFileSystem::Make(TestGcsOptions());

//   arrow::fs::FileInfo info;
//   ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(PreexistingObjectPath()));

//   std::shared_ptr<io::RandomAccessFile> file;
//   ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(info));

//   std::array<char, 1024> buffer{};
//   std::int64_t size;
//   auto constexpr kStart = 16;
//   ASSERT_OK_AND_ASSIGN(size, file->ReadAt(kStart, buffer.size(), buffer.data()));

//   auto const expected = std::string(kLoremIpsum).substr(kStart);
//   EXPECT_EQ(std::string(buffer.data(), size), expected);
// }

// TEST_F(GcsIntegrationTest, OpenInputFileNotFound) {
//   auto fs = GcsFileSystem::Make(TestGcsOptions());

//   ASSERT_RAISES(IOError, fs->OpenInputFile(NotFoundObjectPath()));
// }

// TEST_F(GcsIntegrationTest, OpenInputFileInfoInvalid) {
//   auto fs = GcsFileSystem::Make(TestGcsOptions());

//   arrow::fs::FileInfo info;
//   ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(PreexistingBucketPath()));
//   ASSERT_RAISES(IOError, fs->OpenInputFile(info));

//   ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(NotFoundObjectPath()));
//   ASSERT_RAISES(IOError, fs->OpenInputFile(info));
// }

// TEST_F(GcsIntegrationTest, OpenInputFileClosed) {
//   auto fs = GcsFileSystem::Make(TestGcsOptions());

//   ASSERT_OK_AND_ASSIGN(auto stream, fs->OpenInputFile(PreexistingObjectPath()));
//   ASSERT_OK(stream->Close());
//   std::array<char, 16> buffer{};
//   ASSERT_RAISES(Invalid, stream->Tell());
//   ASSERT_RAISES(Invalid, stream->Read(buffer.size(), buffer.data()));
//   ASSERT_RAISES(Invalid, stream->Read(buffer.size()));
//   ASSERT_RAISES(Invalid, stream->ReadAt(1, buffer.size(), buffer.data()));
//   ASSERT_RAISES(Invalid, stream->ReadAt(1, 1));
//   ASSERT_RAISES(Invalid, stream->Seek(2));
// }

}  // namespace
}  // namespace fs
}  // namespace arrow
