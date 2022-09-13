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

# import importlib
# import sys

# # list of Cython modules containing tests
# cython_test_modules = ["pyarrow._pyarrow_cpp_tests"]

# for mod in cython_test_modules:
#     # For each callable in `mod` with name `test_*`,
#     # set the result as an attribute of this module.
#     mod = importlib.import_module(mod)
#     for name in dir(mod):
#         item = getattr(mod, name)
#         if callable(item) and name.startswith("test_"):
#             setattr(sys.modules[__name__], name, item)

from pyarrow._pyarrow_cpp_tests import (test_PythonDecimalToString)  # noqa

def test_python_decimal_to_string():
    test_PythonDecimalToString()
