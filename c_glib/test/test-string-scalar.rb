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

class TestStringScalar < Test::Unit::TestCase
  def setup
    @buffer = Arrow::Buffer.new("Hello")
    @scalar = Arrow::StringScalar.new(@buffer)
  end

  def test_data_type
    assert_equal(Arrow::StringDataType.new,
                 @scalar.data_type)
  end

  def test_valid?
    assert do
      @scalar.valid?
    end
  end

  def test_equal
    assert_equal(Arrow::StringScalar.new(@buffer),
                 @scalar)
  end

  def test_to_s
    assert_equal("Hello", @scalar.to_s)
  end

  def test_value
    assert_equal(@buffer,
                 @scalar.value)
  end

  def test_cast
    buffer = Arrow::Buffer.new("-10")
    scalar = Arrow::StringScalar.new(buffer)
    assert_equal(Arrow::Int8Scalar.new(-10),
                 scalar.cast(Arrow::Int8DataType.new))
  end
end
