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

from pyarrow.lib cimport *
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_gpu cimport *


cdef class CudaDeviceManager:
    cdef:
        CCudaDeviceManager * manager


cdef class CudaContext:
    cdef:
        shared_ptr[CCudaContext] context

    cdef void init(self, const shared_ptr[CCudaContext] & ctx)


cdef class CudaIpcMemHandle:
    cdef:
        shared_ptr[CCudaIpcMemHandle] handle

    cdef void init(self, shared_ptr[CCudaIpcMemHandle] h)


cdef class CudaBuffer(Buffer):
    cdef:
        shared_ptr[CCudaBuffer] cuda_buffer

    cdef void init_cuda(self, const shared_ptr[CCudaBuffer] & buffer)


cdef class CudaHostBuffer(Buffer):
    cdef:
        shared_ptr[CCudaHostBuffer] host_buffer
        c_bool _freed

    cdef void init_host(self, const shared_ptr[CCudaHostBuffer] & buffer)


cdef class CudaBufferReader(NativeFile):
    cdef:
        CCudaBufferReader * reader
        CudaBuffer buffer


cdef class CudaBufferWriter(NativeFile):
    cdef:
        CCudaBufferWriter * writer
        CudaBuffer buffer
