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

#include "arrow/adapters/orc/adapter_options.h"

#include "arrow/adapters/orc/adapter.h"
#include "orc/Common.hh"
#include "orc/Int128.hh"
#include "orc/Writer.hh"

namespace liborc = orc;

namespace arrow {

namespace adapters {

namespace orc {

std::string FileVersion::ToString() const {
  std::stringstream ss;
  ss << major() << '.' << minor();
  return ss.str();
}

const FileVersion& FileVersion::v_0_11() {
  static FileVersion version(0, 11);
  return version;
}

const FileVersion& FileVersion::v_0_12() {
  static FileVersion version(0, 12);
  return version;
}

WriteOptions::WriteOptions()
    : orc_writer_options_(std::make_shared<liborc::WriterOptions>()), batch_size_(1024) {
  // PASS
}

WriteOptions::WriteOptions(const WriteOptions& rhs)
    : orc_writer_options_(
          std::make_shared<liborc::WriterOptions>(*(rhs.orc_writer_options_.get()))),
      batch_size_(rhs.batch_size_) {
  // PASS
}

WriteOptions::WriteOptions(WriteOptions& rhs) {
  // swap orc_writer_options_ with rhs
  orc_writer_options_.swap(rhs.orc_writer_options_);
  batch_size_ = rhs.batch_size_;
}

WriteOptions& WriteOptions::operator=(const WriteOptions& rhs) {
  if (this != &rhs) {
    orc_writer_options_.reset(
        new liborc::WriterOptions(*(rhs.orc_writer_options_.get())));
    batch_size_ = rhs.batch_size_;
  }
  return *this;
}

WriteOptions::~WriteOptions() {
  // PASS
}

WriteOptions& WriteOptions::SetBatchSize(uint64_t size) {
  batch_size_ = size;
  return *this;
}

uint64_t WriteOptions::GetBatchSize() const { return batch_size_; }

RleVersion WriteOptions::GetRleVersion() const {
  return static_cast<RleVersion>(orc_writer_options_->getRleVersion());
}

WriteOptions& WriteOptions::SetStripeSize(uint64_t size) {
  orc_writer_options_->setStripeSize(size);
  return *this;
}

uint64_t WriteOptions::GetStripeSize() const {
  return orc_writer_options_->getStripeSize();
}

WriteOptions& WriteOptions::SetCompressionBlockSize(uint64_t size) {
  orc_writer_options_->setCompressionBlockSize(size);
  return *this;
}

uint64_t WriteOptions::GetCompressionBlockSize() const {
  return orc_writer_options_->getCompressionBlockSize();
}

WriteOptions& WriteOptions::SetRowIndexStride(uint64_t stride) {
  orc_writer_options_->setRowIndexStride(stride);
  return *this;
}

uint64_t WriteOptions::GetRowIndexStride() const {
  return orc_writer_options_->getRowIndexStride();
}

WriteOptions& WriteOptions::SetDictionaryKeySizeThreshold(double val) {
  orc_writer_options_->setDictionaryKeySizeThreshold(val);
  return *this;
}

double WriteOptions::GetDictionaryKeySizeThreshold() const {
  return orc_writer_options_->getDictionaryKeySizeThreshold();
}

WriteOptions& WriteOptions::SetFileVersion(const FileVersion& version) {
  // Only Hive_0_11 and Hive_0_12 version are supported currently
  uint32_t major = version.major(), minor = version.minor();
  if (major == 0 && (minor == 11 || minor == 12)) {
    orc_writer_options_->setFileVersion(liborc::FileVersion(major, minor));
    return *this;
  }
  throw std::logic_error("Unsupported file version specified.");
}

FileVersion WriteOptions::GetFileVersion() const {
  liborc::FileVersion orc_file_version_ = orc_writer_options_->getFileVersion();
  return FileVersion(orc_file_version_.getMajor(), orc_file_version_.getMinor());
}

WriteOptions& WriteOptions::SetCompression(CompressionKind comp) {
  orc_writer_options_->setCompression(static_cast<liborc::CompressionKind>(comp));
  return *this;
}

CompressionKind WriteOptions::GetCompression() const {
  return static_cast<CompressionKind>(orc_writer_options_->getCompression());
}

WriteOptions& WriteOptions::SetCompressionStrategy(CompressionStrategy strategy) {
  orc_writer_options_->setCompressionStrategy(
      static_cast<liborc::CompressionStrategy>(strategy));
  return *this;
}

CompressionStrategy WriteOptions::GetCompressionStrategy() const {
  return static_cast<CompressionStrategy>(orc_writer_options_->getCompressionStrategy());
}

bool WriteOptions::GetAlignedBitpacking() const {
  return orc_writer_options_->getAlignedBitpacking();
}

WriteOptions& WriteOptions::SetPaddingTolerance(double tolerance) {
  orc_writer_options_->setPaddingTolerance(tolerance);
  return *this;
}

double WriteOptions::GetPaddingTolerance() const {
  return orc_writer_options_->getPaddingTolerance();
}

WriteOptions& WriteOptions::SetErrorStream(std::ostream& err_stream) {
  orc_writer_options_->setErrorStream(err_stream);
  return *this;
}

std::ostream* WriteOptions::GetErrorStream() const {
  return orc_writer_options_->getErrorStream();
}

bool WriteOptions::GetEnableIndex() const {
  return orc_writer_options_->getEnableIndex();
}

bool WriteOptions::GetEnableDictionary() const {
  return orc_writer_options_->getEnableDictionary();
}

WriteOptions& WriteOptions::SetColumnsUseBloomFilter(const std::set<uint64_t>& columns) {
  orc_writer_options_->setColumnsUseBloomFilter(columns);
  return *this;
}

bool WriteOptions::IsColumnUseBloomFilter(uint64_t column) const {
  return orc_writer_options_->isColumnUseBloomFilter(column);
}

WriteOptions& WriteOptions::SetBloomFilterFpp(double fpp) {
  orc_writer_options_->setBloomFilterFPP(fpp);
  return *this;
}

double WriteOptions::GetBloomFilterFpp() const {
  return orc_writer_options_->getBloomFilterFPP();
}

// delibrately not provide setter to write bloom filter version because
// we only support UTF8 for now.
BloomFilterVersion WriteOptions::GetBloomFilterVersion() const {
  return static_cast<BloomFilterVersion>(orc_writer_options_->getBloomFilterVersion());
}

}  // namespace orc
}  // namespace adapters
}  // namespace arrow
