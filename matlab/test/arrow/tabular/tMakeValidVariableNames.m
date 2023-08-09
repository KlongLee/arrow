classdef tMakeValidVariableNames < matlab.unittest.TestCase

    methods(Test)

        function Colon(testCase)
            % Verify that ":" becomes ":_1".
            import arrow.tabular.internal.*

            original = ":";
            expected = ":_1";

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
        end

        function RowNames(testCase)
            % Verify that "RowNames" becomes "RowNames_1".
            import arrow.tabular.internal.*

            original = "RowNames";
            expected = "RowNames_1";
            actual = makeValidVariableNames(original);
            testCase.verifyEqual(actual, expected);
        end

        function Properties(testCase)
            % Verify that "Properties" becomes "Properties_1".
            import arrow.tabular.internal.*

            original = "Properties";
            expected = "Properties_1";

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
        end

        function VariableNames(testCase)
            % Verify that "VariableNames" becomes VariableNames_1.
            import arrow.tabular.internal.*

            original = "VariableNames";
            expected = "VariableNames_1";

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
        end

        function ValidVariableNames(testCase)
            % Verify that when all of the input strings
            % are valid table variable names, that none of them
            % are modified.
            import arrow.tabular.internal.*

            original = ["A", "B", "C"];
            expected = original;

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
        end

        function ValidVariableNamesUnicode(testCase)
            % Verify that when all of the input strings are valid Unicode
            % table variable names, that none of them are modified.
            import arrow.tabular.internal.*

            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";

            original = [smiley, tree, mango];
            expected = original;

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
        end

        function PropertiesWithConflictingNumericSuffix(testCase)
            % Verify that conflicting numeric suffixes (e.g. "Properties"
            % and "Properties_1") are resolved as expected.
            import arrow.tabular.internal.*

            original = ["Properties", "Properties_1"];
            expected = ["Properties_2", "Properties_1"];

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);

            original = ["Properties_1", "Properties", "Properties_4"];
            expected = ["Properties_1", "Properties_2", "Properties_4"];

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
        end

        function RowNamesWithConflictingNumericSuffix(testCase)
            % Verify that conflicting numeric suffixes (e.g. "RowNames"
            % and "RowNames_1") are resolved as expected.
            import arrow.tabular.internal.*

            original = ["RowNames", "RowNames_1"];
            expected = ["RowNames_2", "RowNames_1"];

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);

            original = ["RowNames_1", "RowNames", "RowNames_4"];
            expected = ["RowNames_1", "RowNames_2", "RowNames_4"];

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
        end

        function VariableNamesWithConflictingNumericSuffix(testCase)
            % Verify that conflicting numeric suffixes (e.g. "VariableNames"
            % and "VariableNames_1") are resolved as expected.
            import arrow.tabular.internal.*

            original = ["VariableNames", "VariableNames_1"];
            expected = ["VariableNames_2", "VariableNames_1"];

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);

            original = ["VariableNames_1", "VariableNames", "VariableNames_4"];
            expected = ["VariableNames_1", "VariableNames_2", "VariableNames_4"];

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
        end

        function ColonWithConflictingSuffix(testCase)
            % Verify that conflicting suffixes (e.g. ":"
            % and "x_") are resolved as expected.
            import arrow.tabular.internal.*

            original = [":", ":_1"];
            expected = [":_2", ":_1"];

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);

            original = [":_1", ":", ":_4"];
            expected = [":_1", ":_2", ":_4"];

            actual = makeValidVariableNames(original);

            testCase.verifyEqual(actual, expected);
        end

    end
    
end