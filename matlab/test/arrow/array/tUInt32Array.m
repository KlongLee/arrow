% Licensed to the Apache Software Foundation (ASF) under one or more
% contributor license agreements.  See the NOTICE file distributed with
% this work for additional information regarding copyright ownership.
% The ASF licenses this file to you under the Apache License, Version
% 2.0 (the "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
% implied.  See the License for the specific language governing
% permissions and limitations under the License.

classdef tUInt32Array < hNumericArray
% Tests for arrow.array.UInt32Array

    properties
        ArrowArrayClassName = "arrow.array.UInt32Array"
        ArrowArrayConstructor = @arrow.array.UInt32Array
        MatlabConversionFcn = @uint32 % uint32 method on class
        MatlabArrayFcn = @uint32 % uint32 function
        MaxValue = intmax("uint32")
        MinValue = intmin("uint32")
        NullSubstitutionValue = uint32(0)
        ArrowType = arrow.type.uint32
    end
end
