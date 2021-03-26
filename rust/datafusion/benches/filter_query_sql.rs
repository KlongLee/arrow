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

use arrow::{
    array::{Float32Array, Float64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::ExecutionContext;
use datafusion::{datasource::MemTable, error::Result};
use futures::executor::block_on;
use std::sync::Arc;

async fn query(ctx: &mut ExecutionContext, sql: &str) {
    // execute the query
    let df = ctx.sql(&sql).unwrap();
    let results = df.collect().await.unwrap();

    // display the relation
    for _batch in results {
        // println!("num_rows: {}", _batch.num_rows());
    }
}

fn create_context(array_len: usize, batch_size: usize) -> Result<ExecutionContext> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
    ]));

    // define data.
    let batches = (0..array_len / batch_size)
        .map(|i| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Float32Array::from(vec![i as f32; batch_size])),
                    Arc::new(Float64Array::from(vec![i as f64; batch_size])),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    let mut ctx = ExecutionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("t", Arc::new(provider))?;

    Ok(ctx)
}

fn criterion_benchmark(c: &mut Criterion) {
    let array_len = 524_288; // 2^19
    let batch_size = 4096; // 2^12

    c.bench_function("filter_array", |b| {
        let mut ctx = create_context(array_len, batch_size).unwrap();
        b.iter(|| block_on(query(&mut ctx, "select f32, f64 from t where f32 >= f64")))
    });

    c.bench_function("filter_scalar", |b| {
        let mut ctx = create_context(array_len, batch_size).unwrap();
        b.iter(|| {
            block_on(query(
                &mut ctx,
                "select f32, f64 from t where f32 >= 250 and f64 > 250",
            ))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
