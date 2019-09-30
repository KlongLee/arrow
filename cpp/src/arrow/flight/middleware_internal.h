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

// Interfaces for defining middleware for Flight clients and
// servers. Currently experimental.

#pragma once

#include "arrow/flight/platform.h"

#include <map>
#include <string>
#include <utility>

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#else
#include <grpc++/grpc++.h>
#endif

#include "arrow/flight/middleware.h"

namespace arrow {

namespace flight {

namespace internal {

typedef std::multimap<grpc::string_ref, grpc::string_ref> GrpcMetadataMap;

class GrpcCallHeaders : public CallHeaders {
 public:
  explicit GrpcCallHeaders(const GrpcMetadataMap* metadata) : metadata_(metadata) {}
  ~GrpcCallHeaders() = default;

  std::pair<const_iterator, const_iterator> GetHeaders(
      const std::string& key) const override;
  std::size_t Count(const std::string& key) const override;
  const_iterator cbegin() const noexcept override;
  const_iterator cend() const noexcept override;

 private:
  const GrpcMetadataMap* metadata_;
};

}  // namespace internal

}  // namespace flight

}  // namespace arrow
