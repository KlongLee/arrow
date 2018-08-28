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

#include <RcppCommon.h>

#undef Free
#include <arrow/api.h>
#include <arrow/type.h>

template <typename T>
class static_ptr {
public:
  using element_type = T;

  static_ptr(T* ptr_) : ptr(ptr_){}
  inline T& operator*() const {
    return *ptr;
  }
  inline T* operator->() const {
    return ptr;
  }

private:
  T* ptr;
};

namespace Rcpp{
namespace traits{

struct wrap_type_shared_ptr_tag{};
struct wrap_type_static_ptr_tag{};

template <typename T>
struct wrap_type_traits<std::shared_ptr<T>>{
  using wrap_category = wrap_type_shared_ptr_tag;
};

template <typename T>
struct wrap_type_traits<static_ptr<T>>{
  using wrap_category = wrap_type_static_ptr_tag;
};

template <typename T>
class Exporter<std::shared_ptr<T>>;

template <typename T>
class Exporter<static_ptr<T>>;

}
namespace internal{

template <typename T>
inline SEXP wrap_dispatch(const T& x, Rcpp::traits::wrap_type_shared_ptr_tag) ;

template <typename T>
inline SEXP wrap_dispatch(const T& x, Rcpp::traits::wrap_type_static_ptr_tag) ;

}

}

#include <Rcpp.h>

RCPP_EXPOSED_ENUM_NODECL(arrow::Type::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::DateUnit)
RCPP_EXPOSED_ENUM_NODECL(arrow::TimeUnit::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::StatusCode)

namespace Rcpp{
namespace traits{

template <typename T>
class Exporter<std::shared_ptr<T>> {
public:
  Exporter(SEXP self) : xp(extract_xp(self)){}

  inline std::shared_ptr<T> get(){
    return *Rcpp::XPtr<std::shared_ptr<T>>(xp);
  }

private:
  SEXP xp;

  SEXP extract_xp(SEXP self){
    static SEXP symb_xp = Rf_install(".:xp:.");
    return Rf_findVarInFrame(self, symb_xp) ;
  }

};

template <typename T>
class Exporter<static_ptr<T>> {
public:
  Exporter(SEXP self) : xp(extract_xp(self)){}

  inline static_ptr<T> get(){
    return *Rcpp::XPtr<static_ptr<T>>(xp);
  }

private:
  SEXP xp;

  SEXP extract_xp(SEXP self){
    static SEXP symb_xp = Rf_install(".:xp:.");
    return Rf_findVarInFrame(self, symb_xp) ;
  }

};

}

namespace internal{

template <typename T>
inline SEXP wrap_dispatch(const T& x, Rcpp::traits::wrap_type_shared_ptr_tag){
  return Rcpp::XPtr<std::shared_ptr<typename T::element_type>>(new std::shared_ptr<typename T::element_type>(x));
}

template <typename T>
inline SEXP wrap_dispatch(const T& x, Rcpp::traits::wrap_type_static_ptr_tag){
  return Rcpp::XPtr<static_ptr<typename T::element_type>>(new static_ptr<typename T::element_type>(x));
}

}

}

