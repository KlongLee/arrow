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

// Functions for converting between pandas's NumPy-based data representation
// and Arrow data structures

#ifndef ARROW_PYTHON_ADAPTERS_PANDAS_H
#define ARROW_PYTHON_ADAPTERS_PANDAS_H

#include "arrow/python/platform.h"

#include <memory>
#include <string>
#include <unordered_set>

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class ChunkedArray;
class Column;
class DataType;
class MemoryPool;
class Status;
class Table;

namespace py {

struct PandasOptions {
  /// If true, we will convert all string columns to categoricals
  bool strings_to_categorical;
  bool zero_copy_only;
  bool integer_object_nulls;
  bool use_threads;

  PandasOptions()
      : strings_to_categorical(false),
        zero_copy_only(false),
        integer_object_nulls(false),
        use_threads(false) {}
};

ARROW_EXPORT
Status ConvertArrayToPandas(PandasOptions options, const std::shared_ptr<Array>& arr,
                            PyObject* py_ref, PyObject** out);

ARROW_EXPORT
Status ConvertChunkedArrayToPandas(PandasOptions options,
                                   const std::shared_ptr<ChunkedArray>& col,
                                   PyObject* py_ref, PyObject** out);

ARROW_EXPORT
Status ConvertColumnToPandas(PandasOptions options, const std::shared_ptr<Column>& col,
                             PyObject* py_ref, PyObject** out);

// Convert a whole table as efficiently as possible to a pandas.DataFrame.
//
// The returned Python object is a list of tuples consisting of the exact 2D
// BlockManager structure of the pandas.DataFrame used as of pandas 0.19.x.
//
// tuple item: (indices: ndarray[int32], block: ndarray[TYPE, ndim=2])
ARROW_EXPORT
Status ConvertTableToPandas(PandasOptions options, const std::shared_ptr<Table>& table,
                            MemoryPool* pool, PyObject** out);

/// Convert a whole table as efficiently as possible to a pandas.DataFrame.
///
/// Explicitly name columns that should be a categorical
/// This option is only used on conversions that are applied to a table.
ARROW_EXPORT
Status ConvertTableToPandas(PandasOptions options,
                            const std::unordered_set<std::string>& categorical_columns,
                            const std::shared_ptr<Table>& table, MemoryPool* pool,
                            PyObject** out);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_ADAPTERS_PANDAS_H
