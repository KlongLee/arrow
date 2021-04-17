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

//! Support for distributed schedulers, such as Kubernetes

pub mod api;
pub mod planner;
pub mod state;

#[cfg(test)]
pub mod test_utils;

use std::fmt;
use std::{convert::TryInto, sync::Arc};

use ballista_core::serde::protobuf::{
    execute_query_params::Query, job_status, scheduler_grpc_server::SchedulerGrpc,
    ExecuteQueryParams, ExecuteQueryResult, FailedJob, FilePartitionMetadata, FileType,
    GetExecutorMetadataParams, GetExecutorMetadataResult, GetFileMetadataParams,
    GetFileMetadataResult, GetJobStatusParams, GetJobStatusResult, JobStatus,
    PartitionId, PollWorkParams, PollWorkResult, QueuedJob, RunningJob, TaskDefinition,
    TaskStatus,
};
use ballista_core::serde::scheduler::ExecutorMeta;

use clap::arg_enum;
use datafusion::physical_plan::ExecutionPlan;
#[cfg(feature = "sled")]
extern crate sled_package as sled;

// an enum used to configure the backend
// needs to be visible to code generated by configure_me
arg_enum! {
    #[derive(Debug, serde::Deserialize)]
    pub enum ConfigBackend {
        Etcd,
        Standalone
    }
}

impl parse_arg::ParseArgFromStr for ConfigBackend {
    fn describe_type<W: fmt::Write>(mut writer: W) -> fmt::Result {
        write!(writer, "The configuration backend for the scheduler")
    }
}

use crate::planner::DistributedPlanner;

use datafusion::execution::context::ExecutionContext;
use log::{debug, error, info, warn};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tonic::{Request, Response};

use self::state::{ConfigBackendClient, SchedulerState};
use datafusion::physical_plan::parquet::ParquetExec;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct SchedulerServer {
    state: Arc<SchedulerState>,
    start_time: u128,
    version: String,
}

impl SchedulerServer {
    pub fn new(config: Arc<dyn ConfigBackendClient>, namespace: String) -> Self {
        const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");
        let state = Arc::new(SchedulerState::new(config, namespace));
        let state_clone = state.clone();

        // TODO: we should elect a leader in the scheduler cluster and run this only in the leader
        tokio::spawn(async move { state_clone.synchronize_job_status_loop().await });

        Self {
            state,
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            version: VERSION.unwrap_or("Unknown").to_string(),
        }
    }
}

#[tonic::async_trait]
impl SchedulerGrpc for SchedulerServer {
    async fn get_executors_metadata(
        &self,
        _request: Request<GetExecutorMetadataParams>,
    ) -> std::result::Result<Response<GetExecutorMetadataResult>, tonic::Status> {
        info!("Received get_executors_metadata request");
        let result = self
            .state
            .get_executors_metadata()
            .await
            .map_err(|e| {
                let msg = format!("Error reading executors metadata: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?
            .into_iter()
            .map(|meta| meta.into())
            .collect();
        Ok(Response::new(GetExecutorMetadataResult {
            metadata: result,
        }))
    }

    async fn poll_work(
        &self,
        request: Request<PollWorkParams>,
    ) -> std::result::Result<Response<PollWorkResult>, tonic::Status> {
        if let PollWorkParams {
            metadata: Some(metadata),
            can_accept_task,
            task_status,
        } = request.into_inner()
        {
            debug!("Received poll_work request for {:?}", metadata);
            let metadata: ExecutorMeta = metadata.into();
            let mut lock = self.state.lock().await.map_err(|e| {
                let msg = format!("Could not lock the state: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;
            self.state
                .save_executor_metadata(metadata.clone())
                .await
                .map_err(|e| {
                    let msg = format!("Could not save executor metadata: {}", e);
                    error!("{}", msg);
                    tonic::Status::internal(msg)
                })?;
            for task_status in task_status {
                self.state
                    .save_task_status(&task_status)
                    .await
                    .map_err(|e| {
                        let msg = format!("Could not save task status: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    })?;
            }
            let task = if can_accept_task {
                let plan = self
                    .state
                    .assign_next_schedulable_task(&metadata.id)
                    .await
                    .map_err(|e| {
                        let msg = format!("Error finding next assignable task: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    })?;
                if let Some((task, _plan)) = &plan {
                    let partition_id = task.partition_id.as_ref().unwrap();
                    info!(
                        "Sending new task to {}: {}/{}/{}",
                        metadata.id,
                        partition_id.job_id,
                        partition_id.stage_id,
                        partition_id.partition_id
                    );
                }
                plan.map(|(status, plan)| TaskDefinition {
                    plan: Some(plan.try_into().unwrap()),
                    task_id: status.partition_id,
                })
            } else {
                None
            };
            lock.unlock().await;
            Ok(Response::new(PollWorkResult { task }))
        } else {
            warn!("Received invalid executor poll_work request");
            Err(tonic::Status::invalid_argument(
                "Missing metadata in request",
            ))
        }
    }

    async fn get_file_metadata(
        &self,
        request: Request<GetFileMetadataParams>,
    ) -> std::result::Result<Response<GetFileMetadataResult>, tonic::Status> {
        let GetFileMetadataParams { path, file_type } = request.into_inner();

        let file_type: FileType = file_type.try_into().map_err(|e| {
            let msg = format!("Error reading request: {}", e);
            error!("{}", msg);
            tonic::Status::internal(msg)
        })?;

        match file_type {
            FileType::Parquet => {
                let parquet_exec =
                    ParquetExec::try_from_path(&path, None, None, 1024, 1, None)
                        .map_err(|e| {
                            let msg = format!("Error opening parquet files: {}", e);
                            error!("{}", msg);
                            tonic::Status::internal(msg)
                        })?;

                //TODO include statistics and any other info needed to reconstruct ParquetExec
                Ok(Response::new(GetFileMetadataResult {
                    schema: Some(parquet_exec.schema().as_ref().into()),
                    partitions: parquet_exec
                        .partitions()
                        .iter()
                        .map(|part| FilePartitionMetadata {
                            filename: part.filenames().to_vec(),
                        })
                        .collect(),
                }))
            }
            //TODO implement for CSV
            _ => Err(tonic::Status::unimplemented(
                "get_file_metadata unsupported file type",
            )),
        }
    }

    async fn execute_query(
        &self,
        request: Request<ExecuteQueryParams>,
    ) -> std::result::Result<Response<ExecuteQueryResult>, tonic::Status> {
        if let ExecuteQueryParams { query: Some(query) } = request.into_inner() {
            let plan = match query {
                Query::LogicalPlan(logical_plan) => {
                    // parse protobuf
                    (&logical_plan).try_into().map_err(|e| {
                        let msg = format!("Could not parse logical plan protobuf: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    })?
                }
                Query::Sql(sql) => {
                    //TODO we can't just create a new context because we need a context that has
                    // tables registered from previous SQL statements that have been executed
                    let mut ctx = ExecutionContext::new();
                    let df = ctx.sql(&sql).map_err(|e| {
                        let msg = format!("Error parsing SQL: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    })?;
                    df.to_logical_plan()
                }
            };
            debug!("Received plan for execution: {:?}", plan);
            let executors = self.state.get_executors_metadata().await.map_err(|e| {
                let msg = format!("Error reading executors metadata: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;
            debug!("Found executors: {:?}", executors);

            let job_id: String = {
                let mut rng = thread_rng();
                std::iter::repeat(())
                    .map(|()| rng.sample(Alphanumeric))
                    .map(char::from)
                    .take(7)
                    .collect()
            };

            // Save placeholder job metadata
            self.state
                .save_job_metadata(
                    &job_id,
                    &JobStatus {
                        status: Some(job_status::Status::Queued(QueuedJob {})),
                    },
                )
                .await
                .map_err(|e| {
                    tonic::Status::internal(format!("Could not save job metadata: {}", e))
                })?;

            let state = self.state.clone();
            let job_id_spawn = job_id.clone();
            tokio::spawn(async move {
                // create physical plan using DataFusion
                let datafusion_ctx = ExecutionContext::new();
                macro_rules! fail_job {
                    ($code :expr) => {{
                        match $code {
                            Err(error) => {
                                warn!("Job {} failed with {}", job_id_spawn, error);
                                state
                                    .save_job_metadata(
                                        &job_id_spawn,
                                        &JobStatus {
                                            status: Some(job_status::Status::Failed(
                                                FailedJob {
                                                    error: format!("{}", error),
                                                },
                                            )),
                                        },
                                    )
                                    .await
                                    .unwrap();
                                return;
                            }
                            Ok(value) => value,
                        }
                    }};
                }

                let start = Instant::now();

                let optimized_plan =
                    fail_job!(datafusion_ctx.optimize(&plan).map_err(|e| {
                        let msg =
                            format!("Could not create optimized logical plan: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    }));

                debug!("Calculated optimized plan: {:?}", optimized_plan);

                let plan = fail_job!(datafusion_ctx
                    .create_physical_plan(&optimized_plan)
                    .map_err(|e| {
                        let msg = format!("Could not create physical plan: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    }));

                info!(
                    "DataFusion created physical plan in {} milliseconds",
                    start.elapsed().as_millis(),
                );

                // create distributed physical plan using Ballista
                if let Err(e) = state
                    .save_job_metadata(
                        &job_id_spawn,
                        &JobStatus {
                            status: Some(job_status::Status::Running(RunningJob {})),
                        },
                    )
                    .await
                {
                    warn!(
                        "Could not update job {} status to running: {}",
                        job_id_spawn, e
                    );
                }
                let mut planner = fail_job!(DistributedPlanner::try_new(executors)
                    .map_err(|e| {
                        let msg = format!("Could not create distributed planner: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    }));
                let stages = fail_job!(planner
                    .plan_query_stages(&job_id_spawn, plan)
                    .map_err(|e| {
                        let msg = format!("Could not plan query stages: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    }));

                // save stages into state
                for stage in stages {
                    fail_job!(state
                        .save_stage_plan(
                            &job_id_spawn,
                            stage.stage_id,
                            stage.child.clone()
                        )
                        .await
                        .map_err(|e| {
                            let msg = format!("Could not save stage plan: {}", e);
                            error!("{}", msg);
                            tonic::Status::internal(msg)
                        }));
                    let num_partitions = stage.output_partitioning().partition_count();
                    for partition_id in 0..num_partitions {
                        let pending_status = TaskStatus {
                            partition_id: Some(PartitionId {
                                job_id: job_id_spawn.clone(),
                                stage_id: stage.stage_id as u32,
                                partition_id: partition_id as u32,
                            }),
                            status: None,
                        };
                        fail_job!(state.save_task_status(&pending_status).await.map_err(
                            |e| {
                                let msg = format!("Could not save task status: {}", e);
                                error!("{}", msg);
                                tonic::Status::internal(msg)
                            }
                        ));
                    }
                }
            });

            Ok(Response::new(ExecuteQueryResult { job_id }))
        } else {
            Err(tonic::Status::internal("Error parsing request"))
        }
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusParams>,
    ) -> std::result::Result<Response<GetJobStatusResult>, tonic::Status> {
        let job_id = request.into_inner().job_id;
        debug!("Received get_job_status request for job {}", job_id);
        let job_meta = self.state.get_job_metadata(&job_id).await.map_err(|e| {
            let msg = format!("Error reading job metadata: {}", e);
            error!("{}", msg);
            tonic::Status::internal(msg)
        })?;
        Ok(Response::new(GetJobStatusResult {
            status: Some(job_meta),
        }))
    }
}

#[cfg(all(test, feature = "sled"))]
mod test {
    use std::sync::Arc;

    use tonic::Request;

    use ballista_core::error::BallistaError;
    use ballista_core::serde::protobuf::{ExecutorMetadata, PollWorkParams};

    use super::{
        state::{SchedulerState, StandaloneClient},
        SchedulerGrpc, SchedulerServer,
    };

    #[tokio::test]
    async fn test_poll_work() -> Result<(), BallistaError> {
        let state = Arc::new(StandaloneClient::try_new_temporary()?);
        let namespace = "default";
        let scheduler = SchedulerServer::new(state.clone(), namespace.to_owned());
        let state = SchedulerState::new(state, namespace.to_string());
        let exec_meta = ExecutorMetadata {
            id: "abc".to_owned(),
            host: "".to_owned(),
            port: 0,
        };
        let request: Request<PollWorkParams> = Request::new(PollWorkParams {
            metadata: Some(exec_meta.clone()),
            can_accept_task: false,
            task_status: vec![],
        });
        let response = scheduler
            .poll_work(request)
            .await
            .expect("Received error response")
            .into_inner();
        // no response task since we told the scheduler we didn't want to accept one
        assert!(response.task.is_none());
        // executor should be registered
        assert_eq!(state.get_executors_metadata().await.unwrap().len(), 1);

        let request: Request<PollWorkParams> = Request::new(PollWorkParams {
            metadata: Some(exec_meta.clone()),
            can_accept_task: true,
            task_status: vec![],
        });
        let response = scheduler
            .poll_work(request)
            .await
            .expect("Received error response")
            .into_inner();
        // still no response task since there are no tasks in the scheduelr
        assert!(response.task.is_none());
        // executor should be registered
        assert_eq!(state.get_executors_metadata().await.unwrap().len(), 1);
        Ok(())
    }
}
