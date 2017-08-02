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

from libcpp cimport bool as c_bool, nullptr
from libcpp.vector cimport vector as c_vector
from cpython.ref cimport PyObject
from cython.operator cimport dereference as deref

from pyarrow.lib cimport Buffer, NativeFile, check_status, _RecordBatchFileWriter

cdef extern from "arrow/python/python_to_arrow.h":

    cdef CStatus SerializeSequences(c_vector[PyObject*] sequences,
        int32_t recursion_depth, shared_ptr[CArray]* array_out,
        c_vector[PyObject*]& tensors_out)

    cdef shared_ptr[CRecordBatch] MakeBatch(shared_ptr[CArray] data)

cdef extern from "arrow/python/arrow_to_python.h":

    cdef CStatus DeserializeList(shared_ptr[CArray] array, int32_t start_idx,
        int32_t stop_idx, PyObject* base,
        const c_vector[shared_ptr[CTensor]]& tensors, PyObject** out)

cdef class PythonObject:

    cdef:
        shared_ptr[CRecordBatch] batch
        c_vector[shared_ptr[CTensor]] tensors

    def __cinit__(self):
        pass

# Main entry point for serialization
def serialize_sequence(object value):
    cdef int32_t recursion_depth = 0
    cdef PythonObject result = PythonObject()
    cdef c_vector[PyObject*] sequences
    cdef shared_ptr[CArray] array
    cdef c_vector[PyObject*] tensors
    cdef PyObject* tensor
    cdef shared_ptr[CTensor] out
    sequences.push_back(<PyObject*> value)
    check_status(SerializeSequences(sequences, recursion_depth, &array, tensors))
    result.batch = MakeBatch(array)
    for tensor in tensors:
        check_status(NdarrayToTensor(c_default_memory_pool(), <object> tensor, &out))
        result.tensors.push_back(out)
    return result

# Main entry point for deserialization
def deserialize_sequence(PythonObject value, object base):
    cdef PyObject* result
    check_status(DeserializeList(deref(value.batch).column(0), 0, deref(value.batch).num_rows(), <PyObject*> base, value.tensors, &result))
    return <object> result

def write_python_object(PythonObject value, NativeFile sink):
    cdef shared_ptr[OutputStream] stream
    sink.write_handle(&stream)
    cdef shared_ptr[CRecordBatchStreamWriter] writer
    cdef shared_ptr[CSchema] schema = deref(value.batch).schema()
    cdef shared_ptr[CRecordBatch] batch = value.batch
    cdef shared_ptr[CTensor] tensor
    cdef int32_t metadata_length
    cdef int64_t body_length

    with nogil:
        check_status(CRecordBatchStreamWriter.Open(stream.get(), schema, &writer))
        check_status(deref(writer).WriteRecordBatch(deref(batch)))
        check_status(deref(writer).Close())

    for tensor in value.tensors:
        check_status(WriteTensor(deref(tensor), stream.get(), &metadata_length, &body_length))

def read_python_object(NativeFile source):
    cdef PythonObject result = PythonObject()
    cdef shared_ptr[RandomAccessFile] stream
    source.read_handle(&stream)
    cdef shared_ptr[CRecordBatchStreamReader] reader
    cdef shared_ptr[CTensor] tensor
    cdef int64_t offset
    
    with nogil:
        check_status(CRecordBatchStreamReader.Open(<shared_ptr[InputStream]> stream, &reader))
        check_status(reader.get().ReadNextRecordBatch(&result.batch))

        check_status(deref(stream).Tell(&offset))

        while True:
            s = ReadTensor(offset, stream.get(), &tensor)
            result.tensors.push_back(tensor)
            if not s.ok():
                break
            check_status(deref(stream).Tell(&offset))

    return result
