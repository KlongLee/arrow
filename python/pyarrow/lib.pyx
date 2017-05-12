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

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from cython.operator cimport dereference as deref
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.common cimport PyObject_to_object
cimport pyarrow.includes.libarrow as libarrow
cimport cpython as cp


import datetime
import decimal as _pydecimal
import numpy as np
import six
import pyarrow._config
from pyarrow.compat import frombytes, tobytes, PandasSeries, Categorical


cdef _pandas():
    import pandas as pd
    return pd

# Exception types
include "error.pxi"

# Memory pools and allocation
include "memory.pxi"

# Array types
include "array.pxi"

# Column, Table, Record Batch
include "table.pxi"

# File IO, IPC
include "io.pxi"

#----------------------------------------------------------------------
# Public API

include "public-api.pxi"
