classdef tfeathermex < matlab.unittest.TestCase
    % Tests for MATLAB featherreadmex and featherwritemex.
    
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
    
    methods(TestClassSetup)
        
        function addFeatherFunctionsToMATLABPath(testCase)
            import matlab.unittest.fixtures.PathFixture
            % Add Feather test utilities to the MATLAB path.
            testCase.applyFixture(PathFixture('util'));
            % Add featherread and featherwrite to the MATLAB path.
            testCase.applyFixture(PathFixture(fullfile('..', 'src')));
            % Add Feather source utilities to the MATLAB path.
            testCase.applyFixture(PathFixture(fullfile('..', 'src', 'util')));
            % featherreadmex must be on the MATLAB path.
            testCase.assertTrue(~isempty(which('featherreadmex')), ...
                '''featherreadmex'' must be on the MATLAB path. Use ''addpath'' to add folders to the MATLAB path.');
            % featherwritemex must be on the MATLAB path.
            testCase.assertTrue(~isempty(which('featherwritemex')), ...
                '''featherwritemex'' must be on to the MATLAB path. Use ''addpath'' to add folders to the MATLAB path.');
        end
        
    end
    
    methods(TestMethodSetup)
    
        function setupTempWorkingDirectory(testCase)
            import matlab.unittest.fixtures.WorkingFolderFixture;
            testCase.applyFixture(WorkingFolderFixture);
        end
        
    end
    
    methods(Test)
        
        function NumericDatatypesNulls(testCase)
            filename = fullfile(pwd, 'temp.feather');
            
            [expectedVariables, expectedMetadata] = createVariablesAndMetadataStructs();
            [actualVariables, ~] = featherMEXRoundTrip(filename, expectedVariables, expectedMetadata);
            testCase.verifyEqual([actualVariables.Valid], [expectedVariables.Valid]);
        end
        
    end

end
