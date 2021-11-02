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

#include <arrow/buffer.h>
#include <arrow/flight/flight_sql/FlightSql.pb.h>
#include <arrow/flight/flight_sql/client.h>
#include <arrow/flight/flight_sql/client_internal.h>
#include <arrow/flight/types.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/result.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/logging.h>
#include <google/protobuf/any.pb.h>

namespace pb = arrow::flight::protocol;

namespace arrow {
namespace flight {
namespace sql {

FlightSqlClient::FlightSqlClient(std::shared_ptr<internal::FlightClientImpl> client)
    : impl_(std::move(client)) {}

FlightSqlClient::FlightSqlClient(std::unique_ptr<FlightClient> client)
    : impl_(internal::FlightClientImpl_Create(std::move(client))) {}

FlightSqlClient::PreparedStatement::PreparedStatement(
    std::shared_ptr<internal::FlightClientImpl> client, std::string handle,
    std::shared_ptr<Schema> dataset_schema, std::shared_ptr<Schema> parameter_schema,
    FlightCallOptions options)
    : client_(std::move(client)),
      options_(std::move(options)),
      handle_(std::move(handle)),
      dataset_schema_(std::move(dataset_schema)),
      parameter_schema_(std::move(parameter_schema)),
      is_closed_(false) {}

FlightSqlClient::~FlightSqlClient() = default;

FlightSqlClient::PreparedStatement::~PreparedStatement() {
  if (IsClosed()) return;

  const Status status = Close();
  if (!status.ok()) {
    ARROW_LOG(ERROR) << "Failed to delete PreparedStatement: " << status.ToString();
  }
}

inline FlightDescriptor GetFlightDescriptorForCommand(
    const google::protobuf::Message& command) {
  google::protobuf::Any any;
  any.PackFrom(command);

  const std::string& string = any.SerializeAsString();
  return FlightDescriptor::Command(string);
}

arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoForCommand(
    internal::FlightClientImpl& client, const FlightCallOptions& options,
    const google::protobuf::Message& command) {
  const FlightDescriptor& descriptor = GetFlightDescriptorForCommand(command);

  std::unique_ptr<FlightInfo> flight_info;
  ARROW_RETURN_NOT_OK(internal::FlightClientImpl_GetFlightInfo(client, options,
                                                               descriptor, &flight_info));

  return std::move(flight_info);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::Execute(
    const FlightCallOptions& options, const std::string& query) {
  pb::sql::CommandStatementQuery command;
  command.set_query(query);

  return GetFlightInfoForCommand(*impl_, options, command);
}

arrow::Result<int64_t> FlightSqlClient::ExecuteUpdate(const FlightCallOptions& options,
                                                      const std::string& query) {
  pb::sql::CommandStatementUpdate command;
  command.set_query(query);

  const FlightDescriptor& descriptor = GetFlightDescriptorForCommand(command);

  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;

  ARROW_RETURN_NOT_OK(internal::FlightClientImpl_DoPut(*impl_, options, descriptor,
                                                       NULLPTR, &writer, &reader));

  std::shared_ptr<Buffer> metadata;

  ARROW_RETURN_NOT_OK(reader->ReadMetadata(&metadata));

  pb::sql::DoPutUpdateResult doPutUpdateResult;

  pb::sql::DoPutUpdateResult result;
  if (!result.ParseFromArray(metadata->data(), static_cast<int>(metadata->size()))) {
    return Status::Invalid("Unable to parse DoPutUpdateResult object.");
  }

  return result.record_count();
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetCatalogs(
    const FlightCallOptions& options) {
  pb::sql::CommandGetCatalogs command;

  return GetFlightInfoForCommand(*impl_, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetSchemas(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema_filter_pattern) {
  pb::sql::CommandGetSchemas command;
  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }
  if (schema_filter_pattern != NULLPTR) {
    command.set_schema_filter_pattern(*schema_filter_pattern);
  }

  return GetFlightInfoForCommand(*impl_, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetTables(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema_filter_pattern, const std::string* table_filter_pattern,
    bool include_schema, std::vector<std::string>& table_types) {
  pb::sql::CommandGetTables command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (schema_filter_pattern != NULLPTR) {
    command.set_schema_filter_pattern(*schema_filter_pattern);
  }

  if (table_filter_pattern != NULLPTR) {
    command.set_table_name_filter_pattern(*table_filter_pattern);
  }

  command.set_include_schema(include_schema);

  for (const std::string& table_type : table_types) {
    command.add_table_types(table_type);
  }

  return GetFlightInfoForCommand(*impl_, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetPrimaryKeys(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema, const std::string& table) {
  pb::sql::CommandGetPrimaryKeys command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (schema != NULLPTR) {
    command.set_schema(*schema);
  }

  command.set_table(table);

  return GetFlightInfoForCommand(*impl_, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetExportedKeys(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema, const std::string& table) {
  pb::sql::CommandGetExportedKeys command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (schema != NULLPTR) {
    command.set_schema(*schema);
  }

  command.set_table(table);

  return GetFlightInfoForCommand(*impl_, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetImportedKeys(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema, const std::string& table) {
  pb::sql::CommandGetImportedKeys command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (schema != NULLPTR) {
    command.set_schema(*schema);
  }

  command.set_table(table);

  return GetFlightInfoForCommand(*impl_, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetCrossReference(
    const FlightCallOptions& options, const std::string* pk_catalog,
    const std::string* pk_schema, const std::string& pk_table,
    const std::string* fk_catalog, const std::string* fk_schema,
    const std::string& fk_table) {
  pb::sql::CommandGetCrossReference command;

  if (pk_catalog != NULLPTR) {
    command.set_pk_catalog(*pk_catalog);
  }
  if (pk_schema != NULLPTR) {
    command.set_pk_schema(*pk_schema);
  }
  command.set_pk_table(pk_table);

  if (fk_catalog != NULLPTR) {
    command.set_fk_catalog(*fk_catalog);
  }
  if (fk_schema != NULLPTR) {
    command.set_fk_schema(*fk_schema);
  }
  command.set_fk_table(fk_table);

  return GetFlightInfoForCommand(*impl_, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetTableTypes(
    const FlightCallOptions& options) {
  pb::sql::CommandGetTableTypes command;

  return GetFlightInfoForCommand(*impl_, options, command);
}

arrow::Result<std::unique_ptr<FlightStreamReader>> FlightSqlClient::DoGet(
    const FlightCallOptions& options, const Ticket& ticket) {
  std::unique_ptr<FlightStreamReader> stream;
  ARROW_RETURN_NOT_OK(internal::FlightClientImpl_DoGet(*impl_, options, ticket, &stream));

  return std::move(stream);
}

arrow::Result<std::shared_ptr<FlightSqlClient::PreparedStatement>>
FlightSqlClient::Prepare(const FlightCallOptions& options, const std::string& query) {
  google::protobuf::Any command;
  pb::sql::ActionCreatePreparedStatementRequest request;
  request.set_query(query);
  command.PackFrom(request);

  Action action;
  action.type = "CreatePreparedStatement";
  action.body = Buffer::FromString(command.SerializeAsString());

  std::unique_ptr<ResultStream> results;

  ARROW_RETURN_NOT_OK(
      internal::FlightClientImpl_DoAction(*impl_, options, action, &results));

  std::unique_ptr<Result> result;
  ARROW_RETURN_NOT_OK(results->Next(&result));

  google::protobuf::Any prepared_result;

  std::shared_ptr<Buffer> message = std::move(result->body);
  if (!prepared_result.ParseFromArray(message->data(),
                                      static_cast<int>(message->size()))) {
    return Status::Invalid("Unable to parse packed ActionCreatePreparedStatementResult");
  }

  pb::sql::ActionCreatePreparedStatementResult prepared_statement_result;

  if (!prepared_result.UnpackTo(&prepared_statement_result)) {
    return Status::Invalid("Unable to unpack ActionCreatePreparedStatementResult");
  }

  const std::string& serialized_dataset_schema =
      prepared_statement_result.dataset_schema();
  const std::string& serialized_parameter_schema =
      prepared_statement_result.parameter_schema();

  std::shared_ptr<Schema> dataset_schema;
  if (!serialized_dataset_schema.empty()) {
    io::BufferReader dataset_schema_reader(serialized_dataset_schema);
    ipc::DictionaryMemo in_memo;
    ARROW_ASSIGN_OR_RAISE(dataset_schema, ReadSchema(&dataset_schema_reader, &in_memo));
  }
  std::shared_ptr<Schema> parameter_schema;
  if (!serialized_parameter_schema.empty()) {
    io::BufferReader parameter_schema_reader(serialized_parameter_schema);
    ipc::DictionaryMemo in_memo;
    ARROW_ASSIGN_OR_RAISE(parameter_schema,
                          ReadSchema(&parameter_schema_reader, &in_memo));
  }
  auto handle = prepared_statement_result.prepared_statement_handle();

  return std::make_shared<PreparedStatement>(impl_, handle, dataset_schema,
                                             parameter_schema, options);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::PreparedStatement::Execute() {
  if (is_closed_) {
    return Status::Invalid("Statement already closed.");
  }

  pb::sql::CommandPreparedStatementQuery execute_query_command;

  execute_query_command.set_prepared_statement_handle(handle_);

  google::protobuf::Any any;
  any.PackFrom(execute_query_command);

  const std::string& string = any.SerializeAsString();
  const FlightDescriptor descriptor = FlightDescriptor::Command(string);

  if (parameter_binding_ && parameter_binding_->num_rows() > 0) {
    std::unique_ptr<FlightStreamWriter> writer;
    std::unique_ptr<FlightMetadataReader> reader;
    ARROW_RETURN_NOT_OK(internal::FlightClientImpl_DoPut(
        *client_, options_, descriptor, parameter_binding_->schema(), &writer, &reader));

    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*parameter_binding_));
    ARROW_RETURN_NOT_OK(writer->DoneWriting());
    // Wait for the server to ack the result
    std::shared_ptr<Buffer> buffer;
    ARROW_RETURN_NOT_OK(reader->ReadMetadata(&buffer));
  }

  std::unique_ptr<FlightInfo> info;
  ARROW_RETURN_NOT_OK(
      internal::FlightClientImpl_GetFlightInfo(*client_, options_, descriptor, &info));

  return std::move(info);
}

arrow::Result<int64_t> FlightSqlClient::PreparedStatement::ExecuteUpdate() {
  if (is_closed_) {
    return Status::Invalid("Statement already closed.");
  }

  pb::sql::CommandPreparedStatementUpdate command;
  command.set_prepared_statement_handle(handle_);
  const FlightDescriptor& descriptor = GetFlightDescriptorForCommand(command);
  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;

  if (parameter_binding_ && parameter_binding_->num_rows() > 0) {
    ARROW_RETURN_NOT_OK(internal::FlightClientImpl_DoPut(
        *client_, options_, descriptor, parameter_binding_->schema(), &writer, &reader));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*parameter_binding_));
  } else {
    const std::shared_ptr<Schema> schema = arrow::schema({});
    ARROW_RETURN_NOT_OK(internal::FlightClientImpl_DoPut(*client_, options_, descriptor,
                                                         schema, &writer, &reader));
    const auto& record_batch =
        arrow::RecordBatch::Make(schema, 0, (std::vector<std::shared_ptr<Array>>){});
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*record_batch));
  }

  ARROW_RETURN_NOT_OK(writer->DoneWriting());
  std::shared_ptr<Buffer> metadata;
  ARROW_RETURN_NOT_OK(reader->ReadMetadata(&metadata));
  ARROW_RETURN_NOT_OK(writer->Close());

  pb::sql::DoPutUpdateResult result;
  if (!result.ParseFromArray(metadata->data(), static_cast<int>(metadata->size()))) {
    return Status::Invalid("Unable to parse DoPutUpdateResult object.");
  }

  return result.record_count();
}

Status FlightSqlClient::PreparedStatement::SetParameters(
    std::shared_ptr<RecordBatch> parameter_binding) {
  parameter_binding_ = std::move(parameter_binding);

  return Status::OK();
}

bool FlightSqlClient::PreparedStatement::IsClosed() { return is_closed_; }

std::shared_ptr<Schema> FlightSqlClient::PreparedStatement::dataset_schema() const {
  return dataset_schema_;
}

std::shared_ptr<Schema> FlightSqlClient::PreparedStatement::parameter_schema() const {
  return parameter_schema_;
}

Status FlightSqlClient::PreparedStatement::Close() {
  if (is_closed_) {
    return Status::Invalid("Statement already closed.");
  }
  google::protobuf::Any command;
  pb::sql::ActionClosePreparedStatementRequest request;
  request.set_prepared_statement_handle(handle_);

  command.PackFrom(request);

  Action action;
  action.type = "ClosePreparedStatement";
  action.body = Buffer::FromString(command.SerializeAsString());

  std::unique_ptr<ResultStream> results;

  ARROW_RETURN_NOT_OK(FlightClientImpl_DoAction(*client_, options_, action, &results));

  is_closed_ = true;

  return Status::OK();
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetSqlInfo(
    const FlightCallOptions& options, const std::vector<int>& sql_info) {
  pb::sql::CommandGetSqlInfo command;
  for (const int& info : sql_info) command.add_info(info);

  return GetFlightInfoForCommand(*impl_, options, command);
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
