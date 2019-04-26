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

class RawRecordsTableMultipleArraysTest < Test::Unit::TestCase
  test("2 arrays") do
    records = [
      [true, nil, "Ruby"],
      [nil, 0, "GLib"],
      [false, 2 ** 8 - 1, nil],

      [nil, 10, "A"],
      [true, 20, "B"],
      [false, nil, "C"],
      [nil, 40, nil],
    ]
    fields = [
               {name: :column0, type: :boolean},
               {name: :column1, type: :uint8},
               {name: :column2, type: :string},
             ]
    record_batches = [
                       Arrow::RecordBatch.new(fields, records[0...3]),
                       Arrow::RecordBatch.new(fields, records[3..-1]),
                     ]
    table = Arrow::Table.new(record_batches[0].schema, record_batches)
    assert_equal(records, table.raw_records)
  end
end
