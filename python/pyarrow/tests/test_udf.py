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


import pytest

import pyarrow as pa
from pyarrow import compute as pc


unary_doc = {"summary": "add function",
             "description": "test add function"}


def unary_function(ctx, scalar1):
    return pc.call_function("add", [scalar1, 1])


binary_doc = {"summary": "y=mx",
              "description": "find y from y = mx"}


def binary_function(ctx, m, x):
    return pc.call_function("multiply", [m, x])


ternary_doc = {"summary": "y=mx+c",
               "description": "find y from y = mx + c"}


def ternary_function(ctx, m, x, c):
    mx = pc.call_function("multiply", [m, x])
    return pc.call_function("add", [mx, c])


varargs_doc = {"summary": "z=ax+by+c",
               "description": "find z from z = ax + by + c"
               }


def varargs_function(ctx, *values):
    base_val = values[:2]
    res = pc.call_function("add", base_val)
    for other_val in values[2:]:
        res = pc.call_function("add", [res, other_val])
    return res


def check_scalar_function(name,
                          in_types,
                          out_type,
                          doc,
                          function,
                          input):
    expected_output = function(None, *input)
    pc.register_scalar_function(function,
                                name, doc, in_types, out_type)

    func = pc.get_function(name)
    assert func.name == name

    result = pc.call_function(name, input)
    assert result == expected_output


def test_scalar_udf_function_with_scalar_valued_functions():
    check_scalar_function("scalar_y=x+k",
                          {"scalar": pc.InputType.scalar(pa.int64()), },
                          pa.int64(),
                          unary_doc,
                          unary_function,
                          [pa.scalar(10, pa.int64())]
                          )

    check_scalar_function("scalar_y=mx",
                          {
                              "scalar1": pc.InputType.scalar(pa.int64()),
                              "scalar2": pc.InputType.scalar(pa.int64()),
                          },
                          pa.int64(),
                          binary_doc,
                          binary_function,
                          [
                              pa.scalar(10, pa.int64()),
                              pa.scalar(2, pa.int64())
                          ]
                          )

    check_scalar_function("scalar_y=mx+c",
                          {
                              "scalar1": pc.InputType.scalar(pa.int64()),
                              "scalar2": pc.InputType.scalar(pa.int64()),
                              "scalar3": pc.InputType.scalar(pa.int64()),
                          },
                          pa.int64(),
                          ternary_doc,
                          ternary_function,
                          [
                              pa.scalar(10, pa.int64()),
                              pa.scalar(2, pa.int64()),
                              pa.scalar(5, pa.int64())
                          ]
                          )

    check_scalar_function("scalar_z=ax+by+c",
                          {
                              "scalar1": pc.InputType.scalar(pa.int64()),
                              "scalar2": pc.InputType.scalar(pa.int64()),
                              "scalar3": pc.InputType.scalar(pa.int64()),
                              "scalar4": pc.InputType.scalar(pa.int64()),
                              "scalar5": pc.InputType.scalar(pa.int64()),
                          },
                          pa.int64(),
                          varargs_doc,
                          varargs_function,
                          [
                              pa.scalar(2, pa.int64()),
                              pa.scalar(10, pa.int64()),
                              pa.scalar(3, pa.int64()),
                              pa.scalar(20, pa.int64()),
                              pa.scalar(5, pa.int64())
                          ]
                          )


def test_scalar_udf_with_array_data_functions():
    check_scalar_function("array_y=x+k",
                          {"array": pc.InputType.array(pa.int64()), },
                          pa.int64(),
                          unary_doc,
                          unary_function,
                          [
                              pa.array([10, 20], pa.int64())
                          ]
                          )

    check_scalar_function("array_y=mx",
                          {
                              "array1": pc.InputType.array(pa.int64()),
                              "array2": pc.InputType.array(pa.int64()),
                          },
                          pa.int64(),
                          binary_doc,
                          binary_function,
                          [
                              pa.array([10, 20], pa.int64()),
                              pa.array([2, 4], pa.int64())
                          ]
                          )

    check_scalar_function("array_y=mx+c",
                          {
                              "array1": pc.InputType.array(pa.int64()),
                              "array2": pc.InputType.array(pa.int64()),
                              "array3": pc.InputType.array(pa.int64()),
                          },
                          pa.int64(),
                          ternary_doc,
                          ternary_function,
                          [
                              pa.array([10, 20], pa.int64()),
                              pa.array([2, 4], pa.int64()),
                              pa.array([5, 10], pa.int64())
                          ]
                          )

    check_scalar_function("array_z=ax+by+c",
                          {
                              "array1": pc.InputType.array(pa.int64()),
                              "array2": pc.InputType.array(pa.int64()),
                              "array3": pc.InputType.array(pa.int64()),
                              "array4": pc.InputType.array(pa.int64()),
                              "array5": pc.InputType.array(pa.int64()),
                          },
                          pa.int64(),
                          varargs_doc,
                          varargs_function,
                          [
                              pa.array([2, 3], pa.int64()),
                              pa.array([10, 20], pa.int64()),
                              pa.array([3, 7], pa.int64()),
                              pa.array([20, 30], pa.int64()),
                              pa.array([5, 10], pa.int64())
                          ]
                          )


def test_udf_input():
    def unary_scalar_function(ctx, scalar):
        return pc.call_function("add", [scalar, 1])

    # validate function name
    doc = {
        "summary": "test udf input",
        "description": "parameters are validated"
    }
    in_types = {"scalar": pc.InputType.scalar(pa.int64())}
    out_type = pa.int64()
    with pytest.raises(TypeError):
        pc.register_scalar_function(unary_scalar_function, None, doc, in_types,
                                    out_type)

    # validate function
    with pytest.raises(TypeError, match="Object must be a callable"):
        pc.register_scalar_function(None, "none_function", doc, in_types,
                                    out_type)

    # validate output type
    expected_expr = "DataType expected, got <class 'NoneType'>"
    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(unary_scalar_function,
                                    "output_function", doc, in_types,
                                    None)

    # validate input type
    expected_expr = r'in_types must be a dictionary of InputType'
    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(unary_scalar_function,
                                    "input_function", doc, None,
                                    out_type)


def test_varargs_function_validation():
    def n_add(ctx, *values):
        base_val = values[:2]
        res = pc.call_function("add", base_val)
        for other_val in values[2:]:
            res = pc.call_function("add", [res, other_val])
        return res

    in_types = {"array1": pc.InputType.array(pa.int64()),
                "array2": pc.InputType.array(pa.int64())
                }
    doc = {"summary": "n add function",
           "description": "add N number of arrays"
           }
    pc.register_scalar_function(n_add, "n_add", doc,
                                in_types, pa.int64())

    func = pc.get_function("n_add")

    assert func.name == "n_add"
    error_msg = "VarArgs function 'n_add' needs at least 2 arguments"
    with pytest.raises(pa.lib.ArrowInvalid, match=error_msg):
        pc.call_function("n_add", [pa.array([1, 10]),
                                   ])


def test_function_doc_validation():

    def unary_scalar_function(ctx, scalar):
        return pc.call_function("add", [scalar, 1])

    # validate arity
    in_types = {"scalar": pc.InputType.scalar(pa.int64())}
    out_type = pa.int64()

    # doc with no summary
    func_doc = {
        "description": "desc"
    }

    expected_expr = "Function doc must contain a summary"

    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(unary_scalar_function, "no_summary",
                                    func_doc, in_types,
                                    out_type)

    # doc with no decription
    func_doc = {
        "summary": "test summary"
    }

    expected_expr = "Function doc must contain a description"

    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(unary_scalar_function, "no_desc",
                                    func_doc, in_types,
                                    out_type)

    # doc with empty dictionary
    func_doc = {}
    expected_expr = "Function doc must contain a summary"
    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(unary_scalar_function, "empty_dictionary",
                                    func_doc, in_types,
                                    out_type)


def test_non_uniform_input_udfs():

    def unary_scalar_function(ctx, scalar1, array1, scalar2):
        coeff = pc.call_function("add", [scalar1, scalar2])
        return pc.call_function("multiply", [coeff, array1])

    in_types = {"scalar1": pc.InputType.scalar(pa.int64()),
                "scalar2": pc.InputType.array(pa.int64()),
                "scalar3": pc.InputType.scalar(pa.int64()),
                }
    func_doc = {
        "summary": "multi type udf",
        "description": "desc"
    }
    pc.register_scalar_function(unary_scalar_function,
                                "multi_type_udf", func_doc,
                                in_types,
                                pa.int64())

    res = pc.call_function("multi_type_udf",
                           [pa.scalar(10), pa.array([1, 2, 3]), pa.scalar(20)])
    assert res == pa.array([30, 60, 90])


def test_nullary_functions():

    def gen_random(ctx):
        import random
        val = random.randint(0, 10)
        return pa.scalar(val)

    func_doc = {
        "summary": "random function",
        "description": "generates a random value"
    }

    pc.register_scalar_function(gen_random, "random_func", func_doc,
                                {},
                                pa.int64())

    res = pc.call_function("random_func", [])
    assert res.as_py() >= 0 and res.as_py() <= 10


def test_output_datatype():
    def add_one(ctx, array):
        ar = pc.call_function("add", [array, 1])
        ar = ar.cast(pa.int32())
        return ar

    func_name = "py_add_to_scalar"
    in_types = {"array": pc.InputType.array(pa.int64())}
    out_type = pa.int64()
    doc = {
        "summary": "add function scalar",
        "description": "add function"
    }
    pc.register_scalar_function(add_one, func_name, doc,
                                in_types, out_type)

    func = pc.get_function(func_name)

    assert func.name == func_name

    expected_expr = "Expected output type, int64," \
        + " but function returned type int32"

    with pytest.raises(pa.lib.ArrowTypeError, match=expected_expr):
        pc.call_function(func_name, [pa.array([20, 30])])


def test_output_value():
    def add_one(ctx, array):
        ar = pc.call_function("add", [array, 1])
        ar = ar.cast(pa.int32())
        return ar

    func_name = "test_output_value"
    in_types = {"array": pc.InputType.array(pa.int64())}
    out_type = {}
    doc = {
        "summary": "test output value",
        "description": "test output"
    }

    expected_expr = "DataType expected, got <class 'dict'>"

    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(add_one, func_name, doc,
                                    in_types, out_type)


def test_output_type():
    def add_one(ctx, array):
        return 42

    func_name = "add_to_scalar_as_py"
    in_types = {"array": pc.InputType.array(pa.int64())}
    out_type = pa.int64()
    doc = {
        "summary": "add function scalar",
        "description": "add function"
    }
    pc.register_scalar_function(add_one, func_name, doc,
                                in_types, out_type)

    func = pc.get_function(func_name)

    assert func.name == func_name

    expected_expr = "Unexpected output type: int"

    with pytest.raises(pa.lib.ArrowTypeError, match=expected_expr):
        pc.call_function(func_name, [pa.array([20, 30])])


def test_input_type():
    def add_one(ctx, array):
        return 42

    func_name = "test_input_type"
    in_types = {"array": None}
    out_type = pa.int64()
    doc = {
        "summary": "test invalid input type",
        "description": "invalid input function"
    }
    expected_expr = "in_types must be of type InputType"

    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(add_one, func_name, doc,
                                    in_types, out_type)


def test_udf_context():

    def random(context, one, two):
        return pc.add(one, two, memory_pool=context.memory_pool)

    in_types = {"one": pc.InputType.scalar(pa.int64()),
                "two": pc.InputType.scalar(pa.int64())
                }
    func_doc = {
        "summary": "test udf context",
        "description": "udf context test"
    }
    pc.register_scalar_function(random,
                                "test_udf_context", func_doc,
                                in_types,
                                pa.int64())

    res = pc.call_function("test_udf_context", [pa.scalar(10), pa.scalar(20)])

    assert res.as_py() == 30
