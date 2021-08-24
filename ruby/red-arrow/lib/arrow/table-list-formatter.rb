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

module Arrow
  # TODO: Almost codes should be implemented in Apache Arrow C++.
  class TableListFormatter < TableFormatter
    private
    def format_header(text, columns)
    end

    def format_rows(text, column_formatters, rows, n_digits, start_offset)
      rows.each_with_index do |row, nth_row|
        text << ("=" * 20 + " #{start_offset + nth_row} " + "=" * 20 + "\n")
        row.each_with_index do |column_value, nth_column|
          column_formatter = column_formatters[nth_column]
          formatted_name = column_formatter.name
          formatted_value = column_formatter.format_value(column_value)
          text << "#{formatted_name}: #{formatted_value}\n"
        end
      end
    end

    def format_ellipsis(text)
      text << "...\n"
    end
  end
end
