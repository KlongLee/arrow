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

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

/// A wrapper around std::mutex since we can't use it directly in
/// public headers due to C++/CLI.
/// https://docs.microsoft.com/en-us/cpp/standard-library/mutex#remarks
class ARROW_EXPORT Mutex {
 public:
  Mutex();
  Mutex(Mutex&&) = default;
  Mutex& operator=(Mutex&&) = default;

  /// A Guard is falsy if a lock could not be acquired.
  class ARROW_EXPORT Guard {
   public:
    Guard() : locked_(NULLPTR, [](Mutex* mutex) {}) {}
    Guard(Guard&&) = default;
    Guard& operator=(Guard&&) = default;

    explicit operator bool() const { return bool(locked_); }

    void Unlock();

   private:
    explicit Guard(Mutex* locked);

    std::unique_ptr<Mutex, void (*)(Mutex*)> locked_;
    friend Mutex;
  };

  Guard TryLock();
  Guard Lock();

 private:
  struct Impl;
  std::unique_ptr<Impl, void (*)(Impl*)> impl_;
};

/// A trivial Mutex, T pair
template <typename T>
class Mutexed : Mutex {
 public:
  Mutexed() = default;
  Mutexed(Mutexed&&) = default;
  Mutexed& operator=(Mutexed&&) = default;
  explicit Mutexed(T obj) : obj_(std::move(obj)) {}

  using Mutex::Lock;
  using Mutex::TryLock;

  T& operator*() { return obj_; }
  const T& operator*() const { return obj_; }

  T* operator->() { return &obj_; }
  const T* operator->() const { return &obj_; }

  T& get() { return obj_; }
  const T& get() const { return obj_; }

 private:
  T obj_;
};

}  // namespace util
}  // namespace arrow
