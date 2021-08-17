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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <initializer_list>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>

#include "arrow/util/launder.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

template <typename T>
struct StaticVectorMixin {
  // properly aligned uninitialized storage for N T's
  using storage_type = typename std::aligned_storage<sizeof(T), alignof(T)>::type;

  static T* ptr_at(storage_type* p, size_t i) {
    return launder(reinterpret_cast<T*>(&p[i]));
  }

  static constexpr const T* ptr_at(const storage_type* p, size_t i) {
    return launder(reinterpret_cast<const T*>(&p[i]));
  }

  static void move_storage(storage_type* src, storage_type* dest, size_t n) {
    for (size_t i = 0; i < n; ++i) {
      T* src_item = ptr_at(src, i);
      T* dest_item = ptr_at(dest, i);
      new (dest_item) T(std::move(*src_item));
      src_item->~T();
    }
  }

  static void destroy_storage(storage_type* p, size_t n) {
    for (size_t i = 0; i < n; ++i) {
      ptr_at(p, i)->~T();
    }
  }
};

template <typename T, size_t N, bool NonTrivialDestructor>
struct StaticVectorStorageBase : public StaticVectorMixin<T> {
  using typename StaticVectorMixin<T>::storage_type;

  storage_type static_data_[N];
  size_t size_ = 0;

  void destroy() {}
};

template <typename T, size_t N>
struct StaticVectorStorageBase<T, N, true> : public StaticVectorMixin<T> {
  using typename StaticVectorMixin<T>::storage_type;

  storage_type static_data_[N];
  size_t size_ = 0;

  ~StaticVectorStorageBase() noexcept { destroy(); }

  void destroy() noexcept { this->destroy_storage(static_data_, size_); }
};

template <typename T, size_t N, bool D = !std::is_trivially_destructible<T>::value>
struct StaticVectorStorage : public StaticVectorStorageBase<T, N, D> {
  using Base = StaticVectorStorageBase<T, N, D>;
  using typename Base::storage_type;

  using Base::size_;
  using Base::static_data_;

  StaticVectorStorage() noexcept = default;

  T* data_ptr() { return this->ptr_at(static_data_, 0); }

  constexpr const T* const_data_ptr() const { return this->ptr_at(static_data_, 0); }

  void bump_size(size_t addend) {
    assert(size_ + addend <= N);
    size_ += addend;
  }

  void reduce_size(size_t reduce_by) {
    assert(reduce_by <= size_);
    size_ -= reduce_by;
  }

  void move_from(StaticVectorStorage&& other) noexcept {
    size_ = other.size_;
    this->move_storage(other.static_data_, static_data_, size_);
    other.size_ = 0;
  }

  constexpr size_t capacity() const { return N; }

  constexpr size_t max_size() const { return N; }

  void reserve(size_t n) {}

  void clear() {
    this->destroy_storage(static_data_, size_);
    size_ = 0;
  }
};

template <typename T, size_t N>
struct SmallVectorStorage : public StaticVectorMixin<T> {
  using typename StaticVectorMixin<T>::storage_type;

  storage_type static_data_[N];
  size_t size_ = 0;
  storage_type* data_ = static_data_;
  size_t dynamic_capacity_ = 0;

  SmallVectorStorage() noexcept = default;

  ~SmallVectorStorage() { destroy(); }

  T* data_ptr() { return this->ptr_at(data_, 0); }

  constexpr const T* const_data_ptr() const { return this->ptr_at(data_, 0); }

  void bump_size(size_t addend) {
    size_t new_size = size_ + addend;
    if (dynamic_capacity_) {
      // Grow dynamic storage if necessary
      if (new_size > dynamic_capacity_) {
        size_t new_capacity = std::max(dynamic_capacity_ * 2, new_size);
        auto new_data = new storage_type[new_capacity];
        this->move_storage(data_, new_data, size_);
        delete[] data_;
        dynamic_capacity_ = new_capacity;
        data_ = new_data;
      }
    } else if (new_size > N) {
      switch_to_dynamic(new_size);
    }
    size_ = new_size;
  }

  void reduce_size(size_t reduce_by) {
    assert(reduce_by <= size_);
    size_ -= reduce_by;
  }

  void destroy() {
    this->destroy_storage(data_, size_);
    if (dynamic_capacity_) {
      delete[] data_;
    }
  }

  void move_from(SmallVectorStorage&& other) noexcept {
    size_ = other.size_;
    dynamic_capacity_ = other.dynamic_capacity_;
    if (dynamic_capacity_) {
      data_ = other.data_;
      other.data_ = NULLPTR;
      other.dynamic_capacity_ = 0;
    } else {
      this->move_storage(other.data_, data_, size_);
    }
    other.size_ = 0;
  }

  constexpr size_t capacity() const { return dynamic_capacity_ ? dynamic_capacity_ : N; }

  constexpr size_t max_size() const { return std::numeric_limits<size_t>::max(); }

  void reserve(size_t n) {
    if (dynamic_capacity_) {
      if (n > dynamic_capacity_) {
        reallocate_dynamic(n);
      }
    } else if (n > N) {
      switch_to_dynamic(n);
    }
  }

  void clear() {
    this->destroy_storage(data_, size_);
    size_ = 0;
  }

 private:
  void switch_to_dynamic(size_t new_capacity) {
    dynamic_capacity_ = new_capacity;
    data_ = new storage_type[new_capacity];
    this->move_storage(static_data_, data_, size_);
  }

  void reallocate_dynamic(size_t new_capacity) {
    assert(new_capacity >= size_);
    auto new_data = new storage_type[new_capacity];
    this->move_storage(data_, new_data, size_);
    delete[] data_;
    dynamic_capacity_ = new_capacity;
    data_ = new_data;
  }
};

template <typename T, size_t N, typename Storage>
class StaticVectorImpl {
 private:
  Storage storage_;

  T* data_ptr() { return storage_.data_ptr(); }

  constexpr const T* const_data_ptr() const { return storage_.const_data_ptr(); }

 public:
  using size_type = size_t;
  using difference_type = ptrdiff_t;
  using value_type = T;
  using pointer = T*;
  using const_pointer = const T*;
  using reference = T&;
  using const_reference = const T&;
  using iterator = T*;
  using const_iterator = const T*;

  constexpr StaticVectorImpl() noexcept = default;

  StaticVectorImpl(StaticVectorImpl&& other) noexcept {
    storage_.move_from(std::move(other.storage_));
  }

  StaticVectorImpl& operator=(StaticVectorImpl&& other) noexcept {
    if (&other != this) {
      storage_.destroy();
      storage_.move_from(std::move(other.storage_));
    }
    return *this;
  }

  StaticVectorImpl(const StaticVectorImpl& other) {
    const size_t n = other.storage_.size_;
    storage_.bump_size(n);
    auto* src = other.const_data_ptr();
    auto* dest = data_ptr();
    for (size_t i = 0; i < n; ++i) {
      new (&dest[i]) T(src[i]);
    }
  }

  StaticVectorImpl& operator=(const StaticVectorImpl& other) noexcept {
    if (&other == this) {
      return *this;
    }
    const size_t n = other.storage_.size_;
    const size_t old_size = storage_.size_;
    auto* src = other.const_data_ptr();
    if (n > old_size) {
      storage_.bump_size(n);
      auto* dest = data_ptr();
      for (size_t i = 0; i < old_size; ++i) {
        dest[i] = src[i];
      }
      for (size_t i = old_size; i < n; ++i) {
        new (&dest[i]) T(src[i]);
      }
    } else {
      auto* dest = data_ptr();
      for (size_t i = 0; i < n; ++i) {
        dest[i] = src[i];
      }
      for (size_t i = n; i < old_size; ++i) {
        dest[i].~T();
      }
      storage_.reduce_size(n);
    }
    return *this;
  }

  explicit StaticVectorImpl(size_t count) {
    storage_.bump_size(count);
    auto* p = data_ptr();
    for (size_t i = 0; i < count; ++i) {
      new (&p[i]) T();
    }
  }

  StaticVectorImpl(size_t count, const T& value) {
    storage_.bump_size(count);
    auto* p = data_ptr();
    for (size_t i = 0; i < count; ++i) {
      new (&p[i]) T(value);
    }
  }

  StaticVectorImpl(std::initializer_list<T> values) {
    storage_.bump_size(values.size());
    auto* p = data_ptr();
    for (auto&& v : values) {
      // XXX cannot move initializer values?
      new (p++) T(v);
    }
  }

  constexpr bool empty() const { return storage_.size_ == 0; }

  constexpr size_t size() const { return storage_.size_; }

  constexpr size_t capacity() const { return storage_.capacity(); }

  constexpr size_t max_size() const { return storage_.max_size(); }

  T& operator[](size_t i) { return data_ptr()[i]; }

  constexpr const T& operator[](size_t i) const { return const_data_ptr()[i]; }

  T& front() { return data_ptr()[0]; }

  constexpr const T& front() const { return const_data_ptr()[0]; }

  T& back() { return data_ptr()[storage_.size_ - 1]; }

  constexpr const T& back() const { return const_data_ptr()[storage_.size_ - 1]; }

  T* data() { return data_ptr(); }

  constexpr const T* data() const { return const_data_ptr(); }

  iterator begin() { return iterator(data_ptr()); }

  constexpr const_iterator begin() const { return const_iterator(const_data_ptr()); }

  iterator end() { return iterator(data_ptr() + storage_.size_); }

  constexpr const_iterator end() const {
    return const_iterator(const_data_ptr() + storage_.size_);
  }

  void reserve(size_t n) { storage_.reserve(n); }

  void clear() { storage_.clear(); }

  void push_back(const T& value) {
    storage_.bump_size(1);
    new (data_ptr() + storage_.size_ - 1) T(value);
  }

  void push_back(T&& value) {
    storage_.bump_size(1);
    new (data_ptr() + storage_.size_ - 1) T(std::move(value));
  }

  template <typename... Args>
  void emplace_back(Args&&... args) {
    storage_.bump_size(1);
    new (data_ptr() + storage_.size_ - 1) T(std::forward<Args>(args)...);
  }

  void resize(size_t n) {
    const size_t old_size = storage_.size_;
    if (n > storage_.size_) {
      storage_.bump_size(n);
      auto* p = data_ptr();
      for (size_t i = old_size; i < n; ++i) {
        new (&p[i]) T{};
      }
    } else {
      auto* p = data_ptr();
      for (size_t i = n; i < old_size; ++i) {
        p[i].~T();
      }
      storage_.reduce_size(n);
    }
  }

  void resize(size_t n, const T& value) {
    const size_t old_size = storage_.size_;
    if (n > storage_.size_) {
      storage_.bump_size(n);
      auto* p = data_ptr();
      for (size_t i = old_size; i < n; ++i) {
        new (&p[i]) T(value);
      }
    } else {
      auto* p = data_ptr();
      for (size_t i = n; i < old_size; ++i) {
        p[i].~T();
      }
      storage_.reduce_size(n);
    }
  }
};

template <typename T, size_t N>
using StaticVector = StaticVectorImpl<T, N, StaticVectorStorage<T, N>>;

template <typename T, size_t N>
using SmallVector = StaticVectorImpl<T, N, SmallVectorStorage<T, N>>;

}  // namespace internal
}  // namespace arrow
