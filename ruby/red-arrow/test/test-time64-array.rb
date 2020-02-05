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

class Time64ArrayTest < Test::Unit::TestCase
  sub_test_case(".new") do
    sub_test_case("unit") do
      test("Arrow::TimeUnit") do
        values = [1000 * 10, nil]
        array = Arrow::Time64Array.new(Arrow::TimeUnit::NANO, values)
        assert_equal([
                       "time64[ns]",
                       [
                         Arrow::Time.new(Arrow::TimeUnit::NANO,
                                         1000 * 10),
                         nil,
                       ],
                     ],
                     [
                       array.value_data_type.to_s,
                       array.to_a,
                     ])
      end

      test("Symbol") do
        values = [1000 * 10, nil]
        array = Arrow::Time64Array.new(:micro, values)
        assert_equal([
                       "time64[us]",
                       [
                         Arrow::Time.new(Arrow::TimeUnit::MICRO,
                                         1000 * 10),
                         nil,
                       ],
                     ],
                     [
                       array.value_data_type.to_s,
                       array.to_a,
                     ])
      end
    end

    sub_test_case("values") do
      test("Arrow::Time") do
        data_type = Arrow::Time64DataType.new(:nano)
        values = [
          Arrow::Time.new(Arrow::TimeUnit::NANO,
                          1000 * 10),
          nil,
        ]
        array = Arrow::Time64Array.new(data_type, values)
        assert_equal(values, array.to_a)
      end

      test("Integer") do
        data_type = Arrow::Time64DataType.new(:nano)
        values = [1000 * 10, nil]
        array = Arrow::Time64Array.new(data_type, values)
        assert_equal([
                       Arrow::Time.new(Arrow::TimeUnit::NANO,
                                       1000 * 10),
                       nil,
                     ],
                     array.to_a)
      end
    end
  end
end
