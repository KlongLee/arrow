.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

=========
Substrait
=========

The ``arrow-dataset`` module can execute Substrait_ plans via the :doc:`Acero <../cpp/streaming_execution>`
query engine.

.. contents::

Executing Queries Using Substrait Plans
=======================================

Plans can reference data in files via URIs, or "named tables" that must be provided along with the plan.

Here is an example of a Java program that queries a Parquet file using Java Substrait
(this example use `Substrait Java`_ project to compile a SQL query to a Substrait plan):

.. code-block:: Java

    import com.google.common.collect.ImmutableList;
    import io.substrait.isthmus.SqlToSubstrait;
    import io.substrait.proto.Plan;
    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.dataset.substrait.AceroSubstraitConsumer;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowReader;
    import org.apache.calcite.sql.parser.SqlParseException;

    import java.nio.ByteBuffer;
    import java.util.HashMap;
    import java.util.Map;

    public class ClientSubstrait {
        public static void main(String[] args) {
            String uri = "file:///data/tpch_parquet/nation.parquet";
            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
            try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(),
                        FileFormat.PARQUET, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options);
                ArrowReader reader = scanner.scanBatches()
            ) {
                // map table to reader
                Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
                mapTableToArrowReader.put("NATION", reader);
                // get binary plan
                Plan plan = getPlan();
                ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.toByteArray().length);
                substraitPlan.put(plan.toByteArray());
                // run query
                try (ArrowReader arrowReader = new AceroSubstraitConsumer(allocator).runQuery(
                        substraitPlan,
                        mapTableToArrowReader
                )) {
                    while (arrowReader.loadNextBatch()) {
                        System.out.println(arrowReader.getVectorSchemaRoot().contentToTSVString());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        static Plan getPlan() throws SqlParseException {
            String sql = "SELECT * from nation";
            String nation = "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), " +
                    "N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))";
            SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
            Plan plan = sqlToSubstrait.execute(sql, ImmutableList.of(nation));
            return plan;
        }
    }

.. code-block:: text

    // Results example:
    FieldPath(0)	FieldPath(1)	FieldPath(2)	FieldPath(3)
    0	ALGERIA	0	 haggle. carefully final deposits detect slyly agai
    1	ARGENTINA	1	al foxes promise slyly according to the regular accounts. bold requests alon

Executing Projections and Filters Using Extended Expressions
============================================================

Dataset also supports projections and filters with Substrait's extended expressions.
This requires the substrait-java library.

This Java program:

- Loads a Parquet file containing the "nation" table from the TPC-H benchmark.
- Projects two new columns:
    - ``N_NAME || ' - ' || N_COMMENT``
    - ``N_REGIONKEY + 10``
- Applies a filter: ``N_NATIONKEY > 18``

.. code-block:: Java

    import java.nio.ByteBuffer;
    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.Base64;
    import java.util.HashMap;
    import java.util.List;
    import java.util.Optional;

    import org.apache.arrow.dataset.file.FileFormat;
    import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
    import org.apache.arrow.dataset.jni.NativeMemoryPool;
    import org.apache.arrow.dataset.scanner.ScanOptions;
    import org.apache.arrow.dataset.scanner.Scanner;
    import org.apache.arrow.dataset.source.Dataset;
    import org.apache.arrow.dataset.source.DatasetFactory;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.ipc.ArrowReader;

    import com.google.protobuf.InvalidProtocolBufferException;
    import com.google.protobuf.util.JsonFormat;

    import io.substrait.proto.Expression;
    import io.substrait.proto.ExpressionReference;
    import io.substrait.proto.ExtendedExpression;
    import io.substrait.proto.FunctionArgument;
    import io.substrait.proto.SimpleExtensionDeclaration;
    import io.substrait.proto.SimpleExtensionURI;
    import io.substrait.type.NamedStruct;
    import io.substrait.type.Type;
    import io.substrait.type.TypeCreator;
    import io.substrait.type.proto.TypeProtoConverter;

    public class ClientSubstraitExtendedExpressions {
      public static void main(String[] args) throws Exception {
        String binaryExtendedExpressions = createExtendedExpresionMessageUsingPOJOClasses();
        projectAndFilterDataset(binaryExtendedExpressions);
      }

      public static void projectAndFilterDataset(String binaryExtendedExpressions) {
        String uri = "file:///data/tpch_parquet/nation.parquet";
        byte[] extendedExpressions = Base64.getDecoder().decode(
            binaryExtendedExpressions);
        ByteBuffer substraitExtendedExpressions = ByteBuffer.allocateDirect(
            extendedExpressions.length);
        substraitExtendedExpressions.put(extendedExpressions);
        ScanOptions options = new ScanOptions(/*batchSize*/ 32768,
            Optional.empty(),
            Optional.of(substraitExtendedExpressions));
        try (
            BufferAllocator allocator = new RootAllocator();
            DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(),
                FileFormat.PARQUET, uri);
            Dataset dataset = datasetFactory.finish();
            Scanner scanner = dataset.newScan(options);
            ArrowReader reader = scanner.scanBatches()
        ) {
          while (reader.loadNextBatch()) {
            System.out.println(
                reader.getVectorSchemaRoot().contentToTSVString());
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      private static String createExtendedExpresionMessageUsingPOJOClasses() throws InvalidProtocolBufferException {
        // Expression: N_REGIONKEY + 10 = col 3 + 10
        Expression.Builder selectionBuilderProjectOne = Expression.newBuilder().
            setSelection(
                Expression.FieldReference.newBuilder().
                    setDirectReference(
                        Expression.ReferenceSegment.newBuilder().
                            setStructField(
                                Expression.ReferenceSegment.StructField.newBuilder().setField(
                                    2)
                            )
                    )
            );
        Expression.Builder literalBuilderProjectOne = Expression.newBuilder()
            .setLiteral(
                Expression.Literal.newBuilder().setI32(10)
            );
        io.substrait.proto.Type outputProjectOne = TypeCreator.NULLABLE.I32.accept(
            new TypeProtoConverter());
        Expression.Builder expressionBuilderProjectOne = Expression.
            newBuilder().
            setScalarFunction(
                Expression.
                    ScalarFunction.
                    newBuilder().
                    setFunctionReference(0).
                    setOutputType(outputProjectOne).
                    addArguments(
                        0,
                        FunctionArgument.newBuilder().setValue(
                            selectionBuilderProjectOne)
                    ).
                    addArguments(
                        1,
                        FunctionArgument.newBuilder().setValue(
                            literalBuilderProjectOne)
                    )
            );
        ExpressionReference.Builder expressionReferenceBuilderProjectOne = ExpressionReference.newBuilder().
            setExpression(expressionBuilderProjectOne)
            .addOutputNames("ADD_TEN_TO_COLUMN_N_REGIONKEY");

        // Expression: name || name = N_NAME || "-" || N_COMMENT = col 1 || col 3
        Expression.Builder selectionBuilderProjectTwo = Expression.newBuilder().
            setSelection(
                Expression.FieldReference.newBuilder().
                    setDirectReference(
                        Expression.ReferenceSegment.newBuilder().
                            setStructField(
                                Expression.ReferenceSegment.StructField.newBuilder().setField(
                                    1)
                            )
                    )
            );
        Expression.Builder selectionBuilderProjectTwoConcatLiteral = Expression.newBuilder()
            .setLiteral(
                Expression.Literal.newBuilder().setString(" - ")
            );
        Expression.Builder selectionBuilderProjectOneToConcat = Expression.newBuilder().
            setSelection(
                Expression.FieldReference.newBuilder().
                    setDirectReference(
                        Expression.ReferenceSegment.newBuilder().
                            setStructField(
                                Expression.ReferenceSegment.StructField.newBuilder().setField(
                                    3)
                            )
                    )
            );
        io.substrait.proto.Type outputProjectTwo = TypeCreator.NULLABLE.STRING.accept(
            new TypeProtoConverter());
        Expression.Builder expressionBuilderProjectTwo = Expression.
            newBuilder().
            setScalarFunction(
                Expression.
                    ScalarFunction.
                    newBuilder().
                    setFunctionReference(1).
                    setOutputType(outputProjectTwo).
                    addArguments(
                        0,
                        FunctionArgument.newBuilder().setValue(
                            selectionBuilderProjectTwo)
                    ).
                    addArguments(
                        1,
                        FunctionArgument.newBuilder().setValue(
                            selectionBuilderProjectTwoConcatLiteral)
                    ).
                    addArguments(
                        2,
                        FunctionArgument.newBuilder().setValue(
                            selectionBuilderProjectOneToConcat)
                    )
            );
        ExpressionReference.Builder expressionReferenceBuilderProjectTwo = ExpressionReference.newBuilder().
            setExpression(expressionBuilderProjectTwo)
            .addOutputNames("CONCAT_COLUMNS_N_NAME_AND_N_COMMENT");

        // Expression: Filter: N_NATIONKEY > 18 = col 1 > 18
        Expression.Builder selectionBuilderFilterOne = Expression.newBuilder().
            setSelection(
                Expression.FieldReference.newBuilder().
                    setDirectReference(
                        Expression.ReferenceSegment.newBuilder().
                            setStructField(
                                Expression.ReferenceSegment.StructField.newBuilder().setField(
                                    0)
                            )
                    )
            );
        Expression.Builder literalBuilderFilterOne = Expression.newBuilder()
            .setLiteral(
                Expression.Literal.newBuilder().setI32(18)
            );
        io.substrait.proto.Type outputFilterOne = TypeCreator.NULLABLE.BOOLEAN.accept(
            new TypeProtoConverter());
        Expression.Builder expressionBuilderFilterOne = Expression.
            newBuilder().
            setScalarFunction(
                Expression.
                    ScalarFunction.
                    newBuilder().
                    setFunctionReference(2).
                    setOutputType(outputFilterOne).
                    addArguments(
                        0,
                        FunctionArgument.newBuilder().setValue(
                            selectionBuilderFilterOne)
                    ).
                    addArguments(
                        1,
                        FunctionArgument.newBuilder().setValue(
                            literalBuilderFilterOne)
                    )
            );
        ExpressionReference.Builder expressionReferenceBuilderFilterOne = ExpressionReference.newBuilder().
            setExpression(expressionBuilderFilterOne)
            .addOutputNames("COLUMN_N_NATIONKEY_GREATER_THAN_18");

        List<String> columnNames = Arrays.asList("N_NATIONKEY", "N_NAME",
            "N_REGIONKEY", "N_COMMENT");
        List<Type> dataTypes = Arrays.asList(
            TypeCreator.NULLABLE.I32,
            TypeCreator.NULLABLE.STRING,
            TypeCreator.NULLABLE.I32,
            TypeCreator.NULLABLE.STRING
        );
        //
        NamedStruct of = NamedStruct.of(
            columnNames,
            Type.Struct.builder().fields(dataTypes).nullable(false).build()
        );

        // Extensions URI
        HashMap<String, SimpleExtensionURI> extensionUris = new HashMap<>();
        extensionUris.put(
            "key-001",
            SimpleExtensionURI.newBuilder()
                .setExtensionUriAnchor(1)
                .setUri("/functions_arithmetic.yaml")
                .build()
        );
        extensionUris.put(
            "key-002",
            SimpleExtensionURI.newBuilder()
                .setExtensionUriAnchor(2)
                .setUri("/functions_comparison.yaml")
                .build()
        );

        // Extensions
        ArrayList<SimpleExtensionDeclaration> extensions = new ArrayList<>();
        SimpleExtensionDeclaration extensionFunctionAdd = SimpleExtensionDeclaration.newBuilder()
            .setExtensionFunction(
                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                    .setFunctionAnchor(0)
                    .setName("add:i32_i32")
                    .setExtensionUriReference(1))
            .build();
        SimpleExtensionDeclaration extensionFunctionGreaterThan = SimpleExtensionDeclaration.newBuilder()
            .setExtensionFunction(
                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                    .setFunctionAnchor(1)
                    .setName("concat:vchar")
                    .setExtensionUriReference(2))
            .build();
        SimpleExtensionDeclaration extensionFunctionLowerThan = SimpleExtensionDeclaration.newBuilder()
            .setExtensionFunction(
                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                    .setFunctionAnchor(2)
                    .setName("gt:any_any")
                    .setExtensionUriReference(2))
            .build();
        extensions.add(extensionFunctionAdd);
        extensions.add(extensionFunctionGreaterThan);
        extensions.add(extensionFunctionLowerThan);

        // Extended Expression
        ExtendedExpression.Builder extendedExpressionBuilder =
            ExtendedExpression.newBuilder().
                addReferredExpr(0,
                    expressionReferenceBuilderProjectOne).
                addReferredExpr(1,
                    expressionReferenceBuilderProjectTwo).
                addReferredExpr(2,
                    expressionReferenceBuilderFilterOne).
                setBaseSchema(of.toProto());
        extendedExpressionBuilder.addAllExtensionUris(extensionUris.values());
        extendedExpressionBuilder.addAllExtensions(extensions);

        ExtendedExpression extendedExpression = extendedExpressionBuilder.build();

        // Print JSON
        System.out.println(
            JsonFormat.printer().includingDefaultValueFields().print(
                extendedExpression));
        // Print binary representation
        System.out.println(Base64.getEncoder().encodeToString(
            extendedExpression.toByteArray()));

        return Base64.getEncoder().encodeToString(
            extendedExpression.toByteArray());
      }
    }

.. code-block:: text

    ADD_TEN_TO_COLUMN_N_REGIONKEY	CONCAT_COLUMNS_N_NAME_AND_N_COMMENT
    13	ROMANIA - ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account
    14	SAUDI ARABIA - ts. silent requests haggle. closely express packages sleep across the blithely
    12	VIETNAM - hely enticingly express accounts. even, final
    13	RUSSIA -  requests against the platelets use never according to the quickly regular pint
    13	UNITED KINGDOM - eans boost carefully special requests. accounts are. carefull
    11	UNITED STATES - y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be

.. _`Substrait`: https://substrait.io/
.. _`Substrait Java`: https://github.com/substrait-io/substrait-java
.. _`Acero`: https://arrow.apache.org/docs/cpp/streaming_execution.html
.. _`Extended Expression`: https://github.com/substrait-io/substrait/blob/main/site/docs/expressions/extended_expression.md