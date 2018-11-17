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

class TestExpression < Test::Unit::TestCase
  def setup
    omit("Gandiva is required") unless defined?(::Gandiva)
  end

  def test_to_s
    augend = Arrow::Field.new("augend", Arrow::Int32DataType.new)
    addend = Arrow::Field.new("addend", Arrow::Int32DataType.new)
    sum = Arrow::Field.new("sum", Arrow::Int32DataType.new)
    expression = Gandiva::Expression.new("add", [augend, addend], sum)
    assert_equal("int32 add((int32) augend, (int32) addend)", expression.to_s)
  end
end
