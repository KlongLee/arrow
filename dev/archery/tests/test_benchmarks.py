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

from archery.benchmark.core import Benchmark, median
from archery.benchmark.compare import BenchmarkComparator
from archery.benchmark.google import add_unit_to_name, extend_payload


def test_benchmark_comparator():
    unit = "micros"

    assert not BenchmarkComparator(
        Benchmark("contender", unit, True, [10]),
        Benchmark("baseline", unit, True, [20])).regression

    assert BenchmarkComparator(
        Benchmark("contender", unit, False, [10]),
        Benchmark("baseline", unit, False, [20])).regression

    assert BenchmarkComparator(
        Benchmark("contender", unit, True, [20]),
        Benchmark("baseline", unit, True, [10])).regression

    assert not BenchmarkComparator(
        Benchmark("contender", unit, False, [20]),
        Benchmark("baseline", unit, False, [10])).regression


def test_benchmark_median():
    assert median([10]) == 10
    assert median([1, 2, 3]) == 2
    assert median([1, 2]) == 1.5
    assert median([1, 2, 3, 4]) == 2.5
    assert median([1, 1, 1, 1]) == 1
    try:
        median([])
        assert False
    except ValueError:
        pass


def test_add_unit_to_name_no_params():
    name = add_unit_to_name("Something", "Unit")
    assert name == "SomethingUnit"


def test_add_unit_to_name_params():
    name = add_unit_to_name("Something/1000", "Unit")
    assert name == "SomethingUnit/1000"


def test_add_unit_to_name_many_params():
    name = add_unit_to_name("Something/1000/33", "Unit")
    assert name == "SomethingUnit/1000/33"


def test_add_unit_to_name_kind():
    name = add_unit_to_name("Something<Repetition::OPTIONAL>", "Unit")
    assert name == "SomethingUnit<Repetition::OPTIONAL>"


def test_add_unit_to_name_kind_and_params():
    name = add_unit_to_name("Something<Repetition::OPTIONAL>/1000/33", "Unit")
    assert name == "SomethingUnit<Repetition::OPTIONAL>/1000/33"


def test_extend_payload():
    payload = [
        {
            "bytes_per_second": 281772039.9844759,
            "cpu_time": 116292.58886653671,
            "items_per_second": 281772039.9844759,
            "iterations": 5964,
            "name": "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0",
            "null_percent": 0.0,
            "real_time": 119811.77313729875,
            "repetition_index": 0,
            "repetitions": 0,
            "run_name": "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0",
            "run_type": "iteration",
            "size": 32768.0,
            "threads": 1,
            "time_unit": "ns",
        },
    ]
    assert extend_payload(payload) == [
        {
            "bytes_per_second": 281772039.9844759,
            "cpu_time": 116292.58886653671,
            "iterations": 5964,
            "name": "ArrayArrayKernelBytes<AddChecked, UInt8Type>/32768/0",
            "null_percent": 0.0,
            "real_time": 119811.77313729875,
            "repetition_index": 0,
            "repetitions": 0,
            "run_name": "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0",
            "run_type": "iteration",
            "size": 32768.0,
            "threads": 1,
            "time_unit": "ns",
        },
        {
            "cpu_time": 116292.58886653671,
            "items_per_second": 281772039.9844759,
            "iterations": 5964,
            "name": "ArrayArrayKernelItems<AddChecked, UInt8Type>/32768/0",
            "null_percent": 0.0,
            "real_time": 119811.77313729875,
            "repetition_index": 0,
            "repetitions": 0,
            "run_name": "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0",
            "run_type": "iteration",
            "size": 32768.0,
            "threads": 1,
            "time_unit": "ns",
        },
    ]


def test_munge_payload():
    payload = [
        {
            "cpu_time": 116292.58886653671,
            "items_per_second": 281772039.9844759,
            "iterations": 5964,
            "name": "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0",
            "null_percent": 0.0,
            "real_time": 119811.77313729875,
            "repetition_index": 0,
            "repetitions": 0,
            "run_name": "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0",
            "run_type": "iteration",
            "size": 32768.0,
            "threads": 1,
            "time_unit": "ns",
        },
        {
            "bytes_per_second": 281772039.9844759,
            "cpu_time": 116292.58886653671,
            "iterations": 5964,
            "name": "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0",
            "null_percent": 0.0,
            "real_time": 119811.77313729875,
            "repetition_index": 0,
            "repetitions": 0,
            "run_name": "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0",
            "run_type": "iteration",
            "size": 32768.0,
            "threads": 1,
            "time_unit": "ns",
        },
    ]
    assert extend_payload(payload) == [
        {
            "cpu_time": 116292.58886653671,
            "items_per_second": 281772039.9844759,
            "iterations": 5964,
            "name": "ArrayArrayKernelItems<AddChecked, UInt8Type>/32768/0",
            "null_percent": 0.0,
            "real_time": 119811.77313729875,
            "repetition_index": 0,
            "repetitions": 0,
            "run_name": "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0",
            "run_type": "iteration",
            "size": 32768.0,
            "threads": 1,
            "time_unit": "ns",
        },
        {
            "bytes_per_second": 281772039.9844759,
            "cpu_time": 116292.58886653671,
            "iterations": 5964,
            "name": "ArrayArrayKernelBytes<AddChecked, UInt8Type>/32768/0",
            "null_percent": 0.0,
            "real_time": 119811.77313729875,
            "repetition_index": 0,
            "repetitions": 0,
            "run_name": "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0",
            "run_type": "iteration",
            "size": 32768.0,
            "threads": 1,
            "time_unit": "ns",
        },
    ]
