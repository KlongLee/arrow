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

from pathlib import Path
import os

from pyarrow._pyarrow_cpp_tests import get_cpp_tests


def inject_cpp_tests(ns):
    """
    Inject C++ tests as Python functions into namespace `ns` (a dict).
    """
    for case in get_cpp_tests():
        def wrapper(case=case):
            case()
        wrapper.__name__ = wrapper.__qualname__ = case.name
        wrapper.__module__ = ns['__name__']
        ns[case.name] = wrapper


inject_cpp_tests(globals())


def test_pyarrow_include():
    # We need to make sure that pyarrow/include is always
    # created. Either with PyArrow C++ header files or with
    # Arrow C++ and PyArrow C++ header files together

    cwd = os.getcwd()
    pyarrow_include = os.path.join(cwd, 'pyarrow', 'include')
    pyarrow_cpp_include = os.path.join(pyarrow_include, 'arrow', 'python')

    obj_include = Path(pyarrow_include)
    obj_python_include = Path(pyarrow_cpp_include)

    assert obj_include.exists()
    assert obj_python_include.exists()
