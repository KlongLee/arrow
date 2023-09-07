%FLOAT64TYPE Type class for float64 data. 

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

classdef Float64Type < arrow.type.NumericType
    
    methods 
        function obj = Float64Type(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.type.proxy.Float64Type")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.type.NumericType(proxy);
        end
    end

    methods (Access=protected)
        function groups = getDisplayPropertyGroups(~)
            targets = "ID";
            groups = matlab.mixin.util.PropertyGroup(targets);
        end
    end
end