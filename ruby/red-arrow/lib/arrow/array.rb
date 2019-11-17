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

require "arrow/filtable"
require "arrow/takeable"

module Arrow
  class Array
    include Enumerable
    include Filtable
    include Takeable

    class << self
      def new(*args)
        builder_class_name = "#{name}Builder"
        if const_defined?(builder_class_name)
          builder_class = const_get(builder_class_name)
          if builder_class.buildable?(args)
            builder_class.build(*args)
          else
            super
          end
        else
          super
        end
      end
    end

    # @param i [Integer]
    #   The index of the value to be gotten.
    #
    #   You can specify negative index like for `::Array#[]`.
    #
    # @return [Object, nil]
    #   The `i`-th value.
    #
    #   `nil` for NULL value or out of range `i`.
    def [](i)
      i += length if i < 0
      return nil if i < 0 or i >= length
      if null?(i)
        nil
      else
        get_value(i)
      end
    end

    def each
      return to_enum(__method__) unless block_given?

      length.times do |i|
        yield(self[i])
      end
    end

    def reverse_each
      return to_enum(__method__) unless block_given?

      (length - 1).downto(0) do |i|
        yield(self[i])
      end
    end

    def to_arrow
      self
    end

    alias_method :value_data_type_raw, :value_data_type
    def value_data_type
      @value_data_type ||= value_data_type_raw
    end

    def to_a
      values
    end

    alias_method :filter_raw, :filter
    alias_method :filter, :filter_array

    alias_method :take_raw, :take
    alias_method :take, :take_array

    alias_method :is_in_raw, :is_in
    def is_in(array)
      case array
      when ::Array
        is_in_raw(self.class.new(array))
      when ChunkedArray
        is_in_chunked_array(array)
      else
        is_in_raw(array)
      end
    end
  end
end
