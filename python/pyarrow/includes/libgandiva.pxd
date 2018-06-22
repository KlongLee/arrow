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

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *

cdef extern from "gandiva/status.h" namespace "gandiva" nogil:
    # We can later add more of the common status factory methods as needed
    cdef GStatus GStatus_OK "Status::OK"()
    cdef GStatus GStatus_Invalid "Status::Invalid"()

    cdef cppclass GStatus "gandiva::Status":
        GStatus()

        c_string ToString()
        c_string message()

        c_bool ok()

cdef extern from "gandiva/gandiva_aliases.h" namespace "gandiva" nogil:

    cdef cppclass CNode" gandiva::Node":
        pass

    cdef cppclass CExpression" gandiva::Expression":
        pass

    ctypedef vector[shared_ptr[CNode]] CNodeVector" gandiva::NodeVector"

    ctypedef vector[shared_ptr[CExpression]] CExpressionVector" gandiva::ExpressionVector"


cdef extern from "gandiva/arrow.h" namespace "gandiva" nogil:

    ctypedef vector[shared_ptr[CArray]] CArrayVector" gandiva::ArrayVector"


cdef extern from "gandiva/tree_expr_builder.h" namespace "gandiva" nogil:

    cdef shared_ptr[CNode] TreeExprBuilder_MakeLiteral "gandiva::TreeExprBuilder::MakeLiteral"(c_bool value)

    cdef shared_ptr[CExpression] TreeExprBuilder_MakeExpression "gandiva::TreeExprBuilder::MakeExpression"(shared_ptr[CNode] root_node, shared_ptr[CField] result_field)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeFunction "gandiva::TreeExprBuilder::MakeFunction"(const c_string& name, const CNodeVector& children, shared_ptr[CDataType] return_type)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeField "gandiva::TreeExprBuilder::MakeField"(shared_ptr[CField] field)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeIf "gandiva::TreeExprBuilder::MakeIf"(shared_ptr[CNode] condition, shared_ptr[CNode] this_node, shared_ptr[CNode] else_node, shared_ptr[CDataType] return_type)

    cdef GStatus Projector_Make "gandiva::Projector::Make"(shared_ptr[CSchema] schema, const CExpressionVector& children, CMemoryPool* pool, shared_ptr[CProjector]* projector)

cdef extern from "gandiva/projector.h" namespace "gandiva" nogil:

    cdef cppclass CProjector" gandiva::Projector":

         GStatus Evaluate(const CRecordBatch& batch, CArrayVector* output)
