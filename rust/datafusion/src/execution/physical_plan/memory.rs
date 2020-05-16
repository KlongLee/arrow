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

//! Execution plan for reading in-memory batches of data

use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::execution::physical_plan::{BatchIterator, ExecutionPlan, Partition};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

/// Execution plan for scanning a Memory file
pub struct MemoryExec {
    /// The data to query
    partitions: Vec<Vec<RecordBatch>>,
    /// Schema representing the Memory files after the optional projection is applied
    schema: Arc<Schema>,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
}

impl ExecutionPlan for MemoryExec {
    /// Get the schema for this execution plan
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Get the partitions for this execution plan. Each partition can be executed in parallel.
    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        let partitions = self
            .partitions
            .iter()
            .map(|vec| {
                Arc::new(MemoryPartition::new(
                    vec.clone(),
                    self.schema.clone(),
                    self.projection.clone(),
                )) as Arc<dyn Partition>
            })
            .collect();
        Ok(partitions)
    }
}

impl MemoryExec {
    /// Create a new execution plan for reading in-memory record batches
    pub fn try_new(
        partitions: &Vec<Vec<RecordBatch>>,
        schema: Arc<Schema>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            partitions: partitions.clone(),
            schema,
            projection,
        })
    }
}

/// Memory Partition
struct MemoryPartition {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Schema representing the data
    schema: Arc<Schema>,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
}

impl MemoryPartition {
    fn new(
        data: Vec<RecordBatch>,
        schema: Arc<Schema>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            data: data.clone(),
            schema,
            projection,
        }
    }
}

impl Partition for MemoryPartition {
    /// Execute this partition and return an iterator over RecordBatch
    fn execute(&self) -> Result<Arc<Mutex<dyn BatchIterator>>> {
        Ok(Arc::new(Mutex::new(MemoryIterator::try_new(
            self.data.clone(),
            self.schema.clone(),
            self.projection.clone(),
        )?)))
    }
}

/// Iterator over batches
struct MemoryIterator {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Schema representing the data
    schema: Arc<Schema>,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Index into the data
    index: usize,
}

impl MemoryIterator {
    /// Create an iterator for a Memory file
    pub fn try_new(
        data: Vec<RecordBatch>,
        schema: Arc<Schema>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data: data.clone(),
            schema: schema.clone(),
            projection,
            index: 0,
        })
    }
}

impl BatchIterator for MemoryIterator {
    /// Get the schema
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Get the next RecordBatch
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.index < self.data.len() {
            self.index += 1;
            let batch = &self.data[self.index - 1];
            // apply projection
            let batch = match &self.projection {
                Some(p) => unimplemented!(),
                None => batch,
            };
            Ok(Some(batch.clone()))
        } else {
            Ok(None)
        }
    }
}
