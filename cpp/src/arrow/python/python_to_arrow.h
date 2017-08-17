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

#ifndef ARROW_PYTHON_PYTHON_TO_ARROW_H
#define ARROW_PYTHON_PYTHON_TO_ARROW_H

#include <Python.h>

#include "arrow/api.h"
#include "arrow/io/interfaces.h"
#include "arrow/python/numpy_interop.h"
#include "arrow/python/sequence.h"

#include <vector>

extern "C" {
extern PyObject* pyarrow_serialize_callback;
extern PyObject* pyarrow_deserialize_callback;
}

namespace arrow {
namespace py {

Status SerializePythonSequence(PyObject* sequence,
                               std::shared_ptr<RecordBatch>* batch_out,
                               std::vector<std::shared_ptr<Tensor>>* tensors_out);

Status WriteSerializedPythonSequence(std::shared_ptr<RecordBatch> batch,
                                     std::vector<std::shared_ptr<Tensor>> tensors,
                                     io::OutputStream* dst);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_PYTHON_TO_ARROW_H
