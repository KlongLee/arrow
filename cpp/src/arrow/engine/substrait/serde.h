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

// This API is EXPERIMENTAL.

#pragma once

#include <functional>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/dataset/file_base.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/visibility.h"
#include "arrow/result.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace engine {

/// Factory function type for generating the node that consumes the batches produced by
/// each toplevel Substrait relation when deserializing a Substrait Plan.
using ConsumerFactory = std::function<std::shared_ptr<compute::SinkNodeConsumer>()>;

/// \brief Deserializes a Substrait Plan message to a list of ExecNode declarations
///
/// \param[in] buf a buffer containing the protobuf serialization of a Substrait Plan
/// message
/// \param[in] consumer_factory factory function for generating the node that consumes
/// the batches produced by each toplevel Substrait relation
/// \param[out] ext_set_out if non-null, the extension mapping used by the Substrait
/// Plan is returned here.
/// \return a vector of ExecNode declarations, one for each toplevel relation in the
/// Substrait Plan
ARROW_ENGINE_EXPORT Result<std::vector<compute::Declaration>> DeserializePlans(
    const Buffer& buf, const ConsumerFactory& consumer_factory,
    ExtensionSet* ext_set_out = NULLPTR, const ExtensionIdRegistry* registry = NULLPTR);

/// \brief Deserializes a single-relation Substrait Plan message to an execution plan
///
/// \param[in] buf a buffer containing the protobuf serialization of a Substrait Plan
/// message
/// \param[in] consumer_factory factory function for generating the node that consumes
/// the batches produced by each toplevel Substrait relation
/// \param[out] ext_set_out if non-null, the extension mapping used by the Substrait
/// Plan is returned here.
/// \return an ExecNode corresponding to the single toplevel relation in the Substrait
/// Plan
Result<compute::ExecPlan> DeserializePlan(const Buffer& buf,
                                          const ConsumerFactory& consumer_factory,
                                          ExtensionSet* ext_set_out = NULLPTR,
                                          const ExtensionIdRegistry* registry = NULLPTR);

/// Factory function type for generating the write options of a node consuming the batches
/// produced by each toplevel Substrait relation when deserializing a Substrait Plan.
using WriteOptionsFactory = std::function<std::shared_ptr<dataset::WriteNodeOptions>()>;

/// \brief Deserializes a Substrait Plan message to a list of ExecNode declarations
///
/// \param[in] buf a buffer containing the protobuf serialization of a Substrait Plan
/// message
/// \param[in] write_options_factory factory function for generating the write options of
/// a node consuming the batches produced by each toplevel Substrait relation
/// \param[out] ext_set_out if non-null, the extension mapping used by the Substrait
/// Plan is returned here.
/// \return a vector of ExecNode declarations, one for each toplevel relation in the
/// Substrait Plan
ARROW_ENGINE_EXPORT Result<std::vector<compute::Declaration>> DeserializePlans(
    const Buffer& buf, const WriteOptionsFactory& write_options_factory,
    ExtensionSet* ext_set = NULLPTR, const ExtensionIdRegistry* registry = NULLPTR);

struct ARROW_ENGINE_EXPORT UdfDeclaration {
  std::string name;
  std::string code;
  std::string summary;
  std::string description;
  std::vector<std::pair<std::shared_ptr<DataType>, bool>> input_types;
  std::pair<std::shared_ptr<DataType>, bool> output_type;
};

ARROW_ENGINE_EXPORT Result<std::vector<UdfDeclaration>> DeserializePlanUdfs(
    const Buffer& buf, const ExtensionIdRegistry* registry);

/// \brief Deserializes a Substrait Type message to the corresponding Arrow type
///
/// \param[in] buf a buffer containing the protobuf serialization of a Substrait Type
/// message
/// \param[in] ext_set the extension mapping to use, normally provided by the
/// surrounding Plan message
/// \return the corresponding Arrow data type
ARROW_ENGINE_EXPORT
Result<std::shared_ptr<DataType>> DeserializeType(const Buffer& buf,
                                                  const ExtensionSet& ext_set);

/// \brief Serializes an Arrow type to a Substrait Type message
///
/// \param[in] type the Arrow data type to serialize
/// \param[in,out] ext_set the extension mapping to use; may be updated to add a
/// mapping for the given type
/// \return a buffer containing the protobuf serialization of the corresponding Substrait
/// Type message
ARROW_ENGINE_EXPORT
Result<std::shared_ptr<Buffer>> SerializeType(const DataType& type,
                                              ExtensionSet* ext_set);

/// \brief Deserializes a Substrait NamedStruct message to an Arrow schema
///
/// \param[in] buf a buffer containing the protobuf serialization of a Substrait
/// NamedStruct message
/// \param[in] ext_set the extension mapping to use, normally provided by the
/// surrounding Plan message
/// \return the corresponding Arrow schema
ARROW_ENGINE_EXPORT
Result<std::shared_ptr<Schema>> DeserializeSchema(const Buffer& buf,
                                                  const ExtensionSet& ext_set);

/// \brief Serializes an Arrow schema to a Substrait NamedStruct message
///
/// \param[in] schema the Arrow schema to serialize
/// \param[in,out] ext_set the extension mapping to use; may be updated to add
/// mappings for the types used in the schema
/// \return a buffer containing the protobuf serialization of the corresponding Substrait
/// NamedStruct message
ARROW_ENGINE_EXPORT
Result<std::shared_ptr<Buffer>> SerializeSchema(const Schema& schema,
                                                ExtensionSet* ext_set);

/// \brief Deserializes a Substrait Expression message to a compute expression
///
/// \param[in] buf a buffer containing the protobuf serialization of a Substrait
/// Expression message
/// \param[in] ext_set the extension mapping to use, normally provided by the
/// surrounding Plan message
/// \return the corresponding Arrow compute expression
ARROW_ENGINE_EXPORT
Result<compute::Expression> DeserializeExpression(const Buffer& buf,
                                                  const ExtensionSet& ext_set);

/// \brief Serializes an Arrow compute expression to a Substrait Expression message
///
/// \param[in] expr the Arrow compute expression to serialize
/// \param[in,out] ext_set the extension mapping to use; may be updated to add
/// mappings for the types used in the expression
/// \return a buffer containing the protobuf serialization of the corresponding Substrait
/// Expression message
ARROW_ENGINE_EXPORT
Result<std::shared_ptr<Buffer>> SerializeExpression(const compute::Expression& expr,
                                                    ExtensionSet* ext_set);

/// \brief Deserializes a Substrait Rel (relation) message to an ExecNode declaration
///
/// \param[in] buf a buffer containing the protobuf serialization of a Substrait
/// Rel message
/// \param[in] ext_set the extension mapping to use, normally provided by the
/// surrounding Plan message
/// \return the corresponding ExecNode declaration
ARROW_ENGINE_EXPORT Result<compute::Declaration> DeserializeRelation(
    const Buffer& buf, const ExtensionSet& ext_set);

namespace internal {

/// \brief Checks whether two protobuf serializations of a particular Substrait message
/// type are equivalent
///
/// Note that a binary comparison of the two buffers is insufficient. One reason for this
/// is that the fields of a message can be specified in any order in the serialization.
///
/// \param[in] message_name the name of the Substrait message type to check
/// \param[in] l_buf buffer containing the first protobuf serialization to compare
/// \param[in] r_buf buffer containing the second protobuf serialization to compare
/// \return success if equivalent, failure if not
ARROW_ENGINE_EXPORT
Status CheckMessagesEquivalent(util::string_view message_name, const Buffer& l_buf,
                               const Buffer& r_buf);

/// \brief Utility function to convert a JSON serialization of a Substrait message to
/// its binary serialization
///
/// \param[in] type_name the name of the Substrait message type to convert
/// \param[in] json the JSON string to convert
/// \return a buffer filled with the binary protobuf serialization of message
ARROW_ENGINE_EXPORT
Result<std::shared_ptr<Buffer>> SubstraitFromJSON(util::string_view type_name,
                                                  util::string_view json);

/// \brief Utility function to convert a binary protobuf serialization of a Substrait
/// message to JSON
///
/// \param[in] type_name the name of the Substrait message type to convert
/// \param[in] buf the buffer containing the binary protobuf serialization of the message
/// \return a JSON string representing the message
ARROW_ENGINE_EXPORT
Result<std::string> SubstraitToJSON(util::string_view type_name, const Buffer& buf);

}  // namespace internal
}  // namespace engine
}  // namespace arrow
