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

#include <chrono>
#include <unordered_map>

#include "arrow/util/concurrent_map.h"
#include "arrow/util/mutex.h"

using arrow::util::ConcurrentMap;

namespace parquet {
namespace encryption {

namespace internal {

// in miliseconds
using TimePoint = std::chrono::system_clock::time_point;

static inline TimePoint CurrentTimePoint() { return std::chrono::system_clock::now(); }

template <typename E>
class ExpiringCacheEntry {
 public:
  ExpiringCacheEntry() = default;

  ExpiringCacheEntry(const E& cached_item, uint64_t expiration_interval_millis)
      : cached_item_(cached_item) {
    expiration_timestamp_ =
        CurrentTimePoint() + std::chrono::milliseconds(expiration_interval_millis);
  }

  bool IsExpired() {
    auto now = CurrentTimePoint();
    return (now > expiration_timestamp_);
  }

  E cached_item() { return cached_item_; }

 private:
  TimePoint expiration_timestamp_;
  E cached_item_;
};

// This class is to avoid the below warning when compiling KeyToolkit class with VS2015
// warning C4503: decorated name length exceeded, name was truncated
template <typename V>
class ExpiringCacheMapEntry {
 public:
  ExpiringCacheMapEntry() = default;

  explicit ExpiringCacheMapEntry(std::shared_ptr<ConcurrentMap<V>> cached_item,
                                 uint64_t expiration_interval_millis)
      : map_cache_(cached_item, expiration_interval_millis) {}

  bool IsExpired() { return map_cache_.IsExpired(); }

  std::shared_ptr<ConcurrentMap<V>> cached_item() { return map_cache_.cached_item(); }

 private:
  ExpiringCacheEntry<std::shared_ptr<ConcurrentMap<V>>> map_cache_;
};

}  // namespace internal

// Two-level cache with expiration of internal caches according to token lifetime.
// External cache is per token, internal is per string key.
// Wrapper class around:
//    std::unordered_map<std::string,
//    internal::ExpiringCacheEntry<std::unordered_map<std::string, V>>>
// This cache is safe to be shared between threads.
template <typename V>
class TwoLevelCacheWithExpiration {
 public:
  TwoLevelCacheWithExpiration() {
    last_cache_cleanup_timestamp_ = internal::CurrentTimePoint();
  }

  std::shared_ptr<ConcurrentMap<V>> GetOrCreateInternalCache(
      const std::string& access_token, uint64_t cache_entry_lifetime_ms) {
    auto lock = mutex_.Lock();

    auto external_cache_entry = cache_.find(access_token);
    if (external_cache_entry == cache_.end() ||
        external_cache_entry->second.IsExpired()) {
      cache_.insert(
          {access_token, internal::ExpiringCacheMapEntry<V>(
                             std::shared_ptr<ConcurrentMap<V>>(new ConcurrentMap<V>()),
                             cache_entry_lifetime_ms)});
    }

    return cache_[access_token].cached_item();
  }

  void RemoveCacheEntriesForToken(const std::string& access_token) {
    auto lock = mutex_.Lock();
    cache_.erase(access_token);
  }

  void RemoveCacheEntriesForAllTokens() {
    auto lock = mutex_.Lock();
    cache_.clear();
  }

  void CheckCacheForExpiredTokens(uint64_t cache_cleanup_period_ms) {
    auto lock = mutex_.Lock();

    internal::TimePoint now = internal::CurrentTimePoint();

    if (now > (last_cache_cleanup_timestamp_ +
               std::chrono::milliseconds(cache_cleanup_period_ms))) {
      RemoveExpiredEntriesNoMutex();
      last_cache_cleanup_timestamp_ =
          now + std::chrono::milliseconds(cache_cleanup_period_ms);
    }
  }

  void RemoveExpiredEntriesFromCache() {
    auto lock = mutex_.Lock();

    RemoveExpiredEntriesNoMutex();
  }

  void Remove(const std::string& access_token) {
    auto lock = mutex_.Lock();
    cache_.erase(access_token);
  }

  void Clear() {
    auto lock = mutex_.Lock();
    cache_.clear();
  }

 private:
  void RemoveExpiredEntriesNoMutex() {
    for (auto it = cache_.begin(); it != cache_.end();) {
      if (it->second.IsExpired()) {
        it = cache_.erase(it);
      } else {
        ++it;
      }
    }
  }
  std::unordered_map<std::string, internal::ExpiringCacheMapEntry<V>> cache_;
  internal::TimePoint last_cache_cleanup_timestamp_;
  arrow::util::Mutex mutex_;
};

}  // namespace encryption
}  // namespace parquet
