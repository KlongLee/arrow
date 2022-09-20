# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# distutils: language = c++
# cython: language_level = 3

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport (c_string, CStatus)

cdef extern from "arrow/python/decimal.h" namespace "arrow::py::internal" nogil:

    PyObject* DecimalFromString(
        PyObject* decimal_constructor,
        const c_string& decimal_string)

    CStatus PythonDecimalToString(
        PyObject* python_decimal,
        c_string* out)

    cdef cppclass DecimalMetadata:
        DecimalMetadata()
        DecimalMetadata(int32_t precision, int32_t scale)

        CStatus Update(int32_t suggested_precision, int32_t suggested_scale)
        CStatus Update(PyObject* object)

        int32_t precision()
        int32_t scale()

cdef extern from "arrow/python/python_test.h" namespace "arrow::py" nogil:

    CStatus TestOwnedRefMoves()
    CStatus TestOwnedRefNoGILMoves()
    CStatus TestCheckPyErrorStatus()
    CStatus TestCheckPyErrorStatusNoGIL()
    CStatus TestRestorePyErrorBasics()
    CStatus TestPyBufferInvalidInputObject()

    #ifdef _WIN32
    CStatus TestPyBufferNumpyArray()
    CStatus TestNumPyBufferNumpyArray()
    #endif