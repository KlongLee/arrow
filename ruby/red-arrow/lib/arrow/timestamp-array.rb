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
  class TimestampArray
    class << self
      def new(unit, values)
        data_type = TimestampDataType.new(unit)
        builder = TimestampArrayBuilder.new(data_type)
        builder.build(values)
      end
    end

    def get_value(i)
      to_time(get_raw_value(i))
    end

    def unit
      @unit ||= value_data_type.unit
    end

    private
    def unit_id
      @unit_id ||= unit.nick.to_sym
    end

    def to_time(raw_value)
      case unit_id
      when :second
        Time.at(raw_value)
      when :milli
        Time.at(*raw_value.divmod(1_000))
      when :micro
        Time.at(*raw_value.divmod(1_000_000))
      else
        Time.at(raw_value / 1_000_000_000.0)
      end
    end
  end
end
