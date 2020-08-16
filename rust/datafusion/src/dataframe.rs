// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! DataFrame API for building and executing query plans.

use crate::arrow::record_batch::RecordBatch;
use crate::error::Result;
use crate::logicalplan::{Expr, LogicalPlan};
use arrow::datatypes::Schema;
use std::sync::Arc;

/// DataFrame represents a logical set of rows with the same named columns.
/// Similar to a [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) or
/// [Spark DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html)
///
/// DataFrames are typically created by the `read_csv` and `read_parquet` methods on the
/// [ExecutionContext](../execution/context/struct.ExecutionContext.html) and can then be modified
/// by calling the transformation methods, such as `filter`, `select`, `aggregate`, and `limit`
/// to build up a query definition.
///
/// The query can be executed by calling the `collect` method.
///
/// ```
/// use datafusion::ExecutionContext;
/// use datafusion::execution::physical_plan::csv::CsvReadOptions;
/// use datafusion::logicalplan::col;
///
/// let mut ctx = ExecutionContext::new();
/// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).unwrap();
/// let df = df.filter(col("a").lt_eq(col("b"))).unwrap()
///            .aggregate(vec![col("a")], vec![df.min(col("b")).unwrap()]).unwrap()
///            .limit(100).unwrap();
/// let results = df.collect(4096);
/// ```
pub trait DataFrame {
    /// Filter the DataFrame by column. Returns a new DataFrame only containing the
    /// specified columns.
    ///
    /// ```
    /// use datafusion::ExecutionContext;
    /// use datafusion::execution::physical_plan::csv::CsvReadOptions;
    /// let mut ctx = ExecutionContext::new();
    ///
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).unwrap();
    /// let df = df.select_columns(vec!["a", "b"]).unwrap();
    /// ```
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<dyn DataFrame>>;

    /// Create a projection based on arbitrary expressions.
    ///
    /// ```
    /// use datafusion::ExecutionContext;
    /// use datafusion::execution::physical_plan::csv::CsvReadOptions;
    /// use datafusion::logicalplan::col;
    ///
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).unwrap();
    /// let df = df.select(vec![col("a").multiply(col("b")), col("c")]).unwrap();
    /// ```
    fn select(&self, expr: Vec<Expr>) -> Result<Arc<dyn DataFrame>>;

    /// Filter a DataFrame to only include rows that match the specified filter expression.
    ///
    /// ```
    /// use datafusion::ExecutionContext;
    /// use datafusion::execution::physical_plan::csv::CsvReadOptions;
    /// use datafusion::logicalplan::col;
    ///
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).unwrap();
    /// let df = df.filter(col("a").lt_eq(col("b"))).unwrap();
    /// ```
    fn filter(&self, expr: Expr) -> Result<Arc<dyn DataFrame>>;

    /// Perform an aggregate query with optional grouping expressions.
    ///
    /// ```
    /// use datafusion::ExecutionContext;
    /// use datafusion::execution::physical_plan::csv::CsvReadOptions;
    /// use datafusion::logicalplan::col;
    ///
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).unwrap();
    ///
    /// // The following use is the equivalent of "SELECT MIN(b) GROUP BY a"
    /// let _ = df.aggregate(vec![col("a")], vec![df.min(col("b")).unwrap()]).unwrap();
    ///
    /// // The following use is the equivalent of "SELECT MIN(b)"
    /// let _ = df.aggregate(vec![], vec![df.min(col("b")).unwrap()]).unwrap();
    /// ```
    fn aggregate(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Arc<dyn DataFrame>>;

    /// limit the number of rows returned from this DataFrame.
    ///
    /// ```
    /// use datafusion::ExecutionContext;
    /// use datafusion::execution::physical_plan::csv::CsvReadOptions;
    /// use datafusion::logicalplan::col;
    ///
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).unwrap();
    /// let df = df.limit(100).unwrap();
    /// ```
    fn limit(&self, n: usize) -> Result<Arc<dyn DataFrame>>;

    /// Executes this DataFrame and collects all results into a vector of RecordBatch.
    ///
    /// ```
    /// use datafusion::ExecutionContext;
    /// use datafusion::execution::physical_plan::csv::CsvReadOptions;
    /// use datafusion::logicalplan::col;
    ///
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).unwrap();
    /// let batches = df.collect(4096).unwrap();
    /// ```
    fn collect(&self, batch_size: usize) -> Result<Vec<RecordBatch>>;

    /// Returns the schema describing the output of this DataFrame in terms of columns returned,
    /// where each column has a name, data type, and nullability attribute.

    /// ```
    /// use datafusion::ExecutionContext;
    /// use datafusion::execution::physical_plan::csv::CsvReadOptions;
    /// use datafusion::logicalplan::col;
    ///
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).unwrap();
    /// let schema = df.schema();
    /// ```
    fn schema(&self) -> &Schema;

    /// Return the logical plan represented by this DataFrame.
    fn to_logical_plan(&self) -> LogicalPlan;

    /// Create an expression to represent the min() aggregate function
    fn min(&self, expr: Expr) -> Result<Expr>;

    /// Create an expression to represent the max() aggregate function
    fn max(&self, expr: Expr) -> Result<Expr>;

    /// Create an expression to represent the sum() aggregate function
    fn sum(&self, expr: Expr) -> Result<Expr>;

    /// Create an expression to represent the avg() aggregate function
    fn avg(&self, expr: Expr) -> Result<Expr>;

    /// Create an expression to represent the count() aggregate function
    fn count(&self, expr: Expr) -> Result<Expr>;
}
