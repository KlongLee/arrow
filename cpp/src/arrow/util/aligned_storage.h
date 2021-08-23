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

#include <type_traits>
#include <utility>

#include "arrow/util/launder.h"

namespace arrow {
namespace internal {

template <typename T>
class AlignedStorage {
 public:
  static constexpr bool can_memcpy = std::is_trivial<T>::value;

#if __cpp_constexpr >= 201304L  // non-const constexpr
  constexpr T* get() noexcept { return launder(reinterpret_cast<T*>(&data_)); }
#else
  T* get() noexcept { return launder(reinterpret_cast<T*>(&data_)); }
#endif

  constexpr const T* get() const noexcept {
    return launder(reinterpret_cast<const T*>(&data_));
  }

  void destroy() noexcept {
    if (!std::is_trivially_destructible<T>::value) {
      get()->~T();
    }
  }

  template <typename... A>
  void construct(A&&... args) noexcept {
    new (&data_) T(std::forward<A>(args)...);
  }

  template <typename V>
  void assign(V&& v) noexcept {
    *get() = std::forward<V>(v);
  }

  void move_construct(AlignedStorage* other) noexcept {
    new (&data_) T(std::move(*other->get()));
  }

  void move_assign(AlignedStorage* other) noexcept { *get() = std::move(*other->get()); }

 private:
  typename std::aligned_storage<sizeof(T), alignof(T)>::type data_;
};

}  // namespace internal
}  // namespace arrow
