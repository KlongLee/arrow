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

struct WriterOptionsPrivate {
  uint64_t stripe_size_;
  uint64_t compression_block_size_;
  uint64_t row_index_stride_;
  CompressionKind compression_;
  CompressionStrategy compression_strategy_;
  liborc::MemoryPool* memory_pool_;
  double padding_tolerance_;
  std::ostream* error_stream_;
  FileVersion file_version_;
  double dictionary_key_size_threshold_;
  bool enable_index_;
  std::set<uint64_t> columns_use_bloom_filter_;
  double bloom_filter_false_positive_prob_;
  BloomFilterVersion bloom_filter_version_;

  WriterOptionsPrivate() : file_version_(FileVersion::v_0_12()) {
    stripe_size_ = 64 * 1024 * 1024;                               // 64M
    compression_block_size_ = 64 * 1024;                           // 64K
    row_index_stride_ = 10000;
    compression_ = CompressionKind_ZLIB;
    compression_strategy_ = CompressionStrategy_SPEED;
    memory_pool_ = liborc::getDefaultPool();
    padding_tolerance_ = 0.0;
    error_stream_ = &std::cerr;
    dictionary_key_size_threshold_ = 0.0;
    enable_index_ = true;
    bloom_filter_false_positive_prob_ = 0.05;
    bloom_filter_version_ = UTF8;
  }
};

WriterOptions::WriterOptions()
    : private_bits_(std::unique_ptr<WriterOptionsPrivate>(new WriterOptionsPrivate())) {
  // PASS
}

WriterOptions::WriterOptions(const WriterOptions& rhs)
    : private_bits_(std::unique_ptr<WriterOptionsPrivate>(
          new WriterOptionsPrivate(*(rhs.private_bits_.get())))) {
  // PASS
}

WriterOptions::WriterOptions(WriterOptions& rhs) {
  // swap private_bits with rhs
  private_bits_.swap(rhs.private_bits_);
}

WriterOptions& WriterOptions::operator=(const WriterOptions& rhs) {
  if (this != &rhs) {
    private_bits_.reset(new WriterOptionsPrivate(*(rhs.private_bits_.get())));
  }
  return *this;
}

WriterOptions::~WriterOptions() {
  // PASS
}
RleVersion WriterOptions::rle_version() const {
  if (private_bits_->file_version_ == FileVersion::v_0_11()) {
    return RleVersion_1;
  }

  return RleVersion_2;
}

WriterOptions& WriterOptions::set_stripe_size(uint64_t size) {
  private_bits_->stripe_size_ = size;
  return *this;
}

uint64_t WriterOptions::stripe_size() const { return private_bits_->stripe_size_; }

WriterOptions& WriterOptions::set_compression_block_size(uint64_t size) {
  private_bits_->compression_block_size_ = size;
  return *this;
}

uint64_t WriterOptions::compression_block_size() const {
  return private_bits_->compression_block_size_;
}

WriterOptions& WriterOptions::set_row_index_stride(uint64_t stride) {
  private_bits_->row_index_stride_ = stride;
  private_bits_->enable_index_ = (stride != 0);
  return *this;
}

uint64_t WriterOptions::row_index_stride() const {
  return private_bits_->row_index_stride_;
}

WriterOptions& WriterOptions::set_dictionary_key_size_threshold(double val) {
  private_bits_->dictionary_key_size_threshold_ = val;
  return *this;
}

double WriterOptions::dictionary_key_size_threshold() const {
  return private_bits_->dictionary_key_size_threshold_;
}

WriterOptions& WriterOptions::set_file_version(const FileVersion& version) {
  // Only Hive_0_11 and Hive_0_12 version are supported currently
  if (version.major() == 0 && (version.minor() == 11 || version.minor() == 12)) {
    private_bits_->file_version_ = version;
    return *this;
  }
  throw std::logic_error("Unsupported file version specified.");
}

FileVersion WriterOptions::file_version() const {
  return private_bits_->file_version_;
}

WriterOptions& WriterOptions::set_compression(CompressionKind comp) {
  private_bits_->compression_ = comp;
  return *this;
}

CompressionKind WriterOptions::compression() const {
  return private_bits_->compression_;
}

WriterOptions& WriterOptions::set_compression_strategy(CompressionStrategy strategy) {
  private_bits_->compression_strategy_ = strategy;
  return *this;
}

CompressionStrategy WriterOptions::compression_strategy() const {
  return private_bits_->compression_strategy_;
}

bool WriterOptions::aligned_bitpacking() const {
  return private_bits_->compression_strategy_ ==
         CompressionStrategy::CompressionStrategy_SPEED;
}

WriterOptions& WriterOptions::set_padding_tolerance(double tolerance) {
  private_bits_->padding_tolerance_ = tolerance;
  return *this;
}

double WriterOptions::padding_tolerance() const {
  return private_bits_->padding_tolerance_;
}

WriterOptions& WriterOptions::set_error_stream(std::ostream& err_stream) {
  private_bits_->error_stream_ = &err_stream;
  return *this;
}

std::ostream* WriterOptions::error_stream() const { return private_bits_->error_stream_; }

bool WriterOptions::enable_index() const { return private_bits_->enable_index_; }

bool WriterOptions::enable_dictionary() const {
  return private_bits_->dictionary_key_size_threshold_ > 0.0;
}

WriterOptions& WriterOptions::set_columns_use_bloom_filter(
    const std::set<uint64_t>& columns) {
  private_bits_->columns_use_bloom_filter_ = columns;
  return *this;
}

bool WriterOptions::is_column_use_bloom_filter(uint64_t column) const {
  return private_bits_->columns_use_bloom_filter_.find(column) !=
         private_bits_->columns_use_bloom_filter_.end();
}

std::set<uint64_t> WriterOptions::columns_use_bloom_filter() const {
  return private_bits_->columns_use_bloom_filter_;
}

WriterOptions& WriterOptions::set_bloom_filter_fpp(double fpp) {
  private_bits_->bloom_filter_false_positive_prob_ = fpp;
  return *this;
}

double WriterOptions::bloom_filter_fpp() const {
  return private_bits_->bloom_filter_false_positive_prob_;
}

// delibrately not provide setter to write bloom filter version because
// we only support UTF8 for now.
BloomFilterVersion WriterOptions::bloom_filter_version() const {
  return private_bits_->bloom_filter_version_;
}  // namespace

}  // namespace orc
}  // namespace adapters
}  // namespace arrow
namespace {
liborc::CompressionKind AdaptCompressionKind(
    arrow::adapters::orc::CompressionKind arrow_compression_kind) {
  return static_cast<liborc::CompressionKind>(
      static_cast<int8_t>(arrow_compression_kind));
}
liborc::CompressionStrategy AdaptCompressionStrategy(
    arrow::adapters::orc::CompressionStrategy arrow_compression_strategy) {
  return static_cast<liborc::CompressionStrategy>(
      static_cast<int8_t>(arrow_compression_strategy));
}
liborc::FileVersion AdaptFileVersion(
    arrow::adapters::orc::FileVersion arrow_file_version) {
  return liborc::FileVersion(arrow_file_version.major(), arrow_file_version.minor());
}
}  // namespace
namespace arrow {

namespace adapters {

namespace orc {

liborc::WriterOptions* AdaptWriterOptions(const WriterOptions& arrow_writer_options) {
  auto orc_writer_options = new liborc::WriterOptions();
  orc_writer_options->setStripeSize(arrow_writer_options.stripe_size());
  orc_writer_options->setCompressionBlockSize(
      arrow_writer_options.compression_block_size());
  orc_writer_options->setRowIndexStride(arrow_writer_options.row_index_stride());
  orc_writer_options->setDictionaryKeySizeThreshold(
      arrow_writer_options.dictionary_key_size_threshold());
  orc_writer_options->setFileVersion(
      AdaptFileVersion(arrow_writer_options.file_version()));
  orc_writer_options->setCompression(
      AdaptCompressionKind(arrow_writer_options.compression()));
  orc_writer_options->setCompressionStrategy(
      AdaptCompressionStrategy(arrow_writer_options.compression_strategy()));
  orc_writer_options->setPaddingTolerance(arrow_writer_options.padding_tolerance());
  orc_writer_options->setErrorStream(*(arrow_writer_options.error_stream()));
  orc_writer_options->setBloomFilterFPP(arrow_writer_options.bloom_filter_fpp());
  orc_writer_options->setColumnsUseBloomFilter(
      arrow_writer_options.columns_use_bloom_filter());
  return orc_writer_options;
}

}  // namespace orc
}  // namespace adapters
}  // namespace arrow