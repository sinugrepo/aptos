// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_indexer_grpc_utils::{
    config::IndexerGrpcFileStoreConfig,
    file_store_operator::{FileStoreOperator, GcsFileStoreOperator},
};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tracing::{error, info};

const MAX_VERSION_IN_TESTNET: u64 = 875906185;
const LEGACY_BUCKET_NAME: &str = "aptos-indexer-grpc-testnet";
const NEW_BUCKET_NAME: &str = "aptos-indexer-grpc-testnet2";
const NUM_OF_PROCESSING_THREADS: usize = 8;
const UPDATE_INTERVAL_IN_MILLISECONDS: u64 = 10000;

pub struct Processor {
    legacy_file_store_operator: GcsFileStoreOperator,
    new_file_store_operator: GcsFileStoreOperator,
}

impl Processor {
    pub fn new(file_store_config: IndexerGrpcFileStoreConfig) -> Self {
        let service_account_path = match file_store_config {
            IndexerGrpcFileStoreConfig::GcsFileStore(config) => {
                config.gcs_file_store_service_account_key_path
            },
            _ => panic!("Invalid file store config"),
        };
        let legacy_file_store_operator = GcsFileStoreOperator::new(
            LEGACY_BUCKET_NAME.to_string(),
            service_account_path.clone(),
            false,
        );
        let new_file_store_operator = GcsFileStoreOperator::new(
            NEW_BUCKET_NAME.to_string(),
            service_account_path.clone(),
            true,
        );
        Self {
            legacy_file_store_operator,
            new_file_store_operator,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("Start to run gcs migration processor");
        self.legacy_file_store_operator
            .verify_storage_bucket_existence()
            .await;
        self.new_file_store_operator
            .verify_storage_bucket_existence()
            .await;

        let new_file_store_metadata = self
            .new_file_store_operator
            .get_file_store_metadata()
            .await
            .unwrap();
        let next_version_to_process = new_file_store_metadata.version;
        info!("Start to process from version: {}", next_version_to_process);
        let task_allocation = Arc::new(Mutex::new(next_version_to_process));
        let running_tasks: Arc<Mutex<BTreeSet<u64>>> = Arc::new(Mutex::new(BTreeSet::new()));
        let mut task_handlers = Vec::new();
        for _ in 0..NUM_OF_PROCESSING_THREADS {
            let legacy_file_store_operator = self.legacy_file_store_operator.clone();
            let new_file_store_operator = self.new_file_store_operator.clone();
            let task_allocation = task_allocation.clone();
            let running_tasks = running_tasks.clone();
            let t = tokio::spawn(async move {
                loop {
                    let version_to_process = {
                        let mut task_allocation = task_allocation.lock().unwrap();
                        let ret = *task_allocation;
                        *task_allocation += 1000;
                        ret
                    };
                    if version_to_process > MAX_VERSION_IN_TESTNET {
                        break;
                    }
                    {
                        let running_tasks = running_tasks.lock().unwrap();
                        if running_tasks.contains(&version_to_process) {
                            panic!("Duplicated version to process: {}", version_to_process);
                        }
                    }
                    // Insert into running tasks.
                    {
                        let mut running_tasks = running_tasks.lock().unwrap();
                        running_tasks.insert(version_to_process);
                    }
                    let legacy_file_store_operator = legacy_file_store_operator.clone();
                    let mut new_file_store_operator = new_file_store_operator.clone();

                    // Simple retry to process the version.
                    loop {
                        // download files from legacy bucket.
                        let transactions = match legacy_file_store_operator
                            .get_transactions(version_to_process)
                            .await
                        {
                            Ok(transactions) => transactions,
                            Err(e) => {
                                error!("Failed to download transactions from legacy bucket: {}", e);
                                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                                continue;
                            },
                        };

                        // upload files to new bucket.
                        match new_file_store_operator
                            .upload_transaction_batch(2, transactions)
                            .await
                        {
                            Ok(_) => {
                                break;
                            },
                            Err(e) => {
                                error!("Failed to upload transactions to new bucket: {}", e);
                                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                                continue;
                            },
                        }
                    }
                    // Remove from running tasks.
                    {
                        let mut running_tasks = running_tasks.lock().unwrap();
                        running_tasks.remove(&version_to_process);
                    }
                }
            });
            task_handlers.push(t);
        }

        // sleep for 10 seconds.
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        // watchdog thread.
        let new_file_store_operator = self.new_file_store_operator.clone();
        let t = tokio::spawn(async move {
            let mut failure_count = 0;
            loop {
                if failure_count >= 100 {
                    panic!("Failed to update file store metadata for 100 times");
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    UPDATE_INTERVAL_IN_MILLISECONDS,
                ))
                .await;
                let new_metadata_version = {
                    let running_tasks = running_tasks.lock().unwrap();
                    if running_tasks.is_empty() {
                        panic!("All tasks are done");
                    }
                    // get first running task.
                    let first_running_task = *running_tasks.iter().next().unwrap();
                    first_running_task
                };
                let mut new_file_store_operator = new_file_store_operator.clone();
                let current_metadata = new_file_store_operator
                    .get_file_store_metadata()
                    .await
                    .unwrap();
                if current_metadata.version >= new_metadata_version {
                    continue;
                }
                match new_file_store_operator
                    .update_file_store_metadata_internal(2, new_metadata_version)
                    .await
                {
                    Ok(_) => {
                        failure_count = 0;
                        info!(
                            "Updated file store metadata to version: {}",
                            new_metadata_version
                        );
                    },
                    Err(_) => {
                        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                        failure_count += 1;
                    },
                }
            }
        });
        task_handlers.push(t);
        // join all.
        for t in task_handlers {
            t.await?;
        }
        Ok(())
    }
}
