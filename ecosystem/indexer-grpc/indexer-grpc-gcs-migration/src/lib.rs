// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

pub mod processor;

use anyhow::Result;
use aptos_indexer_grpc_server_framework::RunnableConfig;
use aptos_indexer_grpc_utils::config::IndexerGrpcFileStoreConfig;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerGrpcGcsMigrationConfig {
    pub file_store_config: IndexerGrpcFileStoreConfig,
}

impl IndexerGrpcGcsMigrationConfig {
    pub fn new(file_store_config: IndexerGrpcFileStoreConfig) -> Self {
        Self { file_store_config }
    }
}

#[async_trait::async_trait]
impl RunnableConfig for IndexerGrpcGcsMigrationConfig {
    async fn run(&self) -> Result<()> {
        let mut processor = processor::Processor::new(self.file_store_config.clone());
        processor
            .run()
            .await
            .expect("File store processor exited unexpectedly");
        Ok(())
    }

    fn get_server_name(&self) -> String {
        "idxgcsmgrt".to_string()
    }
}
