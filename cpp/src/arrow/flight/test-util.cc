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

#ifdef __APPLE__
#include <limits.h>
#include <mach-o/dyld.h>
#endif

#include <sstream>

#include <boost/filesystem.hpp>
#include <boost/process.hpp>

#include <gtest/gtest.h>

#include "arrow/ipc/test-common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"

#include "arrow/flight/api.h"
#include "arrow/flight/internal.h"
#include "arrow/flight/test-util.h"

namespace arrow {
namespace flight {

namespace bp = boost::process;
namespace fs = boost::filesystem;

namespace {

Status ResolveCurrentExecutable(fs::path* out) {
  // See https://stackoverflow.com/a/1024937/10194 for various
  // platform-specific recipes.

  boost::system::error_code ec;

#if defined(__linux__)
  *out = fs::canonical("/proc/self/exe", ec);
#elif defined(__APPLE__)
  char buf[PATH_MAX + 1];
  uint32_t bufsize = sizeof(buf);
  if (_NSGetExecutablePath(buf, &bufsize) < 0) {
    return Status::Invalid("Can't resolve current exe: path too large");
  }
  *out = fs::canonical(buf, ec);
#else
  ARROW_UNUSED(ec);
  return Status::NotImplemented("Not available on this system");
#endif
  if (ec) {
    // XXX fold this into the Status class?
    return Status::IOError("Can't resolve current exe: ", ec.message());
  } else {
    return Status::OK();
  }
}

}  // namespace

void TestServer::Start() {
  namespace fs = boost::filesystem;

  std::string str_port = std::to_string(port_);
  std::vector<fs::path> search_path = ::boost::this_process::path();
  // If possible, prepend current executable directory to search path,
  // since it's likely that the test server executable is located in
  // the same directory as the running unit test.
  fs::path current_exe;
  Status st = ResolveCurrentExecutable(&current_exe);
  if (st.ok()) {
    search_path.insert(search_path.begin(), current_exe.parent_path());
  } else if (st.IsNotImplemented()) {
    ARROW_CHECK(st.IsNotImplemented()) << st.ToString();
  }

  try {
    server_process_ = std::make_shared<bp::child>(
        bp::search_path(executable_name_, search_path), "-port", str_port);
  } catch (...) {
    std::stringstream ss;
    ss << "Failed to launch test server '" << executable_name_ << "', looked in ";
    for (const auto& path : search_path) {
      ss << path << " : ";
    }
    ARROW_LOG(FATAL) << ss.str();
    throw;
  }
  std::cout << "Server running with pid " << server_process_->id() << std::endl;
}

int TestServer::Stop() {
  if (server_process_ && server_process_->valid()) {
    kill(server_process_->id(), SIGTERM);
    server_process_->wait();
    return server_process_->exit_code();
  } else {
    // Presumably the server wasn't able to start
    return -1;
  }
}

bool TestServer::IsRunning() { return server_process_->running(); }

int TestServer::port() const { return port_; }

Status InProcessTestServer::Start(std::unique_ptr<ServerAuthHandler> auth_handler) {
  RETURN_NOT_OK(server_->Init(std::move(auth_handler), port_));
  thread_ = std::thread([this]() { ARROW_EXPECT_OK(server_->Serve()); });
  return Status::OK();
}

void InProcessTestServer::Stop() {
  server_->Shutdown();
  thread_.join();
}

int InProcessTestServer::port() const { return port_; }

InProcessTestServer::~InProcessTestServer() {
  // Make sure server shuts down properly
  if (thread_.joinable()) {
    Stop();
  }
}

Status MakeFlightInfo(const Schema& schema, const FlightDescriptor& descriptor,
                      const std::vector<FlightEndpoint>& endpoints, int64_t total_records,
                      int64_t total_bytes, FlightInfo::Data* out) {
  out->descriptor = descriptor;
  out->endpoints = endpoints;
  out->total_records = total_records;
  out->total_bytes = total_bytes;
  return internal::SchemaToString(schema, &out->schema);
}

std::shared_ptr<Schema> ExampleIntSchema() {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", int32());
  return ::arrow::schema({f0, f1});
}

std::shared_ptr<Schema> ExampleStringSchema() {
  auto f0 = field("f0", utf8());
  auto f1 = field("f1", binary());
  return ::arrow::schema({f0, f1});
}

std::shared_ptr<Schema> ExampleDictSchema() {
  std::shared_ptr<RecordBatch> batch;
  ABORT_NOT_OK(ipc::test::MakeDictionary(&batch));
  return batch->schema();
}

std::vector<FlightInfo> ExampleFlightInfo() {
  FlightInfo::Data flight1, flight2, flight3;

  FlightEndpoint endpoint1({{"ticket-ints-1"}, {{"foo1.bar.com", 92385}}});
  FlightEndpoint endpoint2({{"ticket-ints-2"}, {{"foo2.bar.com", 92385}}});
  FlightEndpoint endpoint3({{"ticket-cmd"}, {{"foo3.bar.com", 92385}}});
  FlightEndpoint endpoint4({{"ticket-dicts-1"}, {{"foo4.bar.com", 92385}}});

  FlightDescriptor descr1{FlightDescriptor::PATH, "", {"examples", "ints"}};
  FlightDescriptor descr2{FlightDescriptor::CMD, "my_command", {}};
  FlightDescriptor descr3{FlightDescriptor::PATH, "", {"examples", "dicts"}};

  auto schema1 = ExampleIntSchema();
  auto schema2 = ExampleStringSchema();
  auto schema3 = ExampleDictSchema();

  ARROW_EXPECT_OK(
      MakeFlightInfo(*schema1, descr1, {endpoint1, endpoint2}, 1000, 100000, &flight1));
  ARROW_EXPECT_OK(MakeFlightInfo(*schema2, descr2, {endpoint3}, 1000, 100000, &flight2));
  ARROW_EXPECT_OK(MakeFlightInfo(*schema3, descr3, {endpoint4}, -1, -1, &flight3));
  return {FlightInfo(flight1), FlightInfo(flight2), FlightInfo(flight3)};
}

Status ExampleIntBatches(BatchVector* out) {
  std::shared_ptr<RecordBatch> batch;
  for (int i = 0; i < 5; ++i) {
    // Make all different sizes, use different random seed
    RETURN_NOT_OK(ipc::test::MakeIntBatchSized(10 + i, &batch, i));
    out->push_back(batch);
  }
  return Status::OK();
}

Status ExampleDictBatches(BatchVector* out) {
  // Just the same batch, repeated a few times
  std::shared_ptr<RecordBatch> batch;
  for (int i = 0; i < 3; ++i) {
    RETURN_NOT_OK(ipc::test::MakeDictionary(&batch));
    out->push_back(batch);
  }
  return Status::OK();
}

std::vector<ActionType> ExampleActionTypes() {
  return {{"drop", "drop a dataset"}, {"cache", "cache a dataset"}};
}

TestServerAuthHandler::TestServerAuthHandler(const std::string& username,
                                             const std::string& password)
    : username_(username), password_(password) {}
TestServerAuthHandler::~TestServerAuthHandler() {}
Status TestServerAuthHandler::Authenticate(const ServerAuthSender& outgoing,
                                           const ServerAuthReader& incoming) {
  std::string token;
  RETURN_NOT_OK(incoming.Read(&token));
  if (token != password_) {
    return Status::Invalid("Invalid password");
  }
  RETURN_NOT_OK(outgoing.Write(username_));
  return Status::OK();
}

Status TestServerAuthHandler::IsValid(const std::string& token,
                                      std::string* peer_identity) {
  if (token != password_) {
    return Status::Invalid("Invalid token");
  }
  *peer_identity = username_;
  return Status::OK();
}

TestClientAuthHandler::TestClientAuthHandler(const std::string& username,
                                             const std::string& password)
    : username_(username), password_(password) {}
TestClientAuthHandler::~TestClientAuthHandler() {}
Status TestClientAuthHandler::Authenticate(const ClientAuthSender& outgoing,
                                           const ClientAuthReader& incoming) {
  RETURN_NOT_OK(outgoing.Write(password_));
  std::string username;
  RETURN_NOT_OK(incoming.Read(&username));
  if (username != username_) {
    return Status::Invalid("Invalid username");
  }
  return Status::OK();
}

Status TestClientAuthHandler::GetToken(std::string* token) {
  *token = password_;
  return Status::OK();
}

}  // namespace flight
}  // namespace arrow
