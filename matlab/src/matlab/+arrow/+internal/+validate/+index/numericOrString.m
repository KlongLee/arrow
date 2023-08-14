%NUMERICORSTRING Validates index is a valid numeric or string index.

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

function index = numericOrString(index, numericIndexType)
    index = convertCharsToStrings(index);
    if isnumeric(index)
        index = arrow.internal.validate.index.numeric(index, numericIndexType);
    elseif isstring(index)
        index = arrow.internal.validate.index.string(index);
    else
        errid = "arrow:badSubscript:UnsupportedIndexType";
        msg = "Indices must be positive integers or nonmissing strings.";
        error(errid, msg);
    end
end
