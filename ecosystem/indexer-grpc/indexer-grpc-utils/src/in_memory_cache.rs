// Copyright © Aptos Foundation

use crate::compression_util::{CacheEntry, StorageFormat};
use anyhow::Context;
use aptos_protos::transaction::v1::Transaction;
use itertools::Itertools;
use mini_moka::sync::Cache;
use prost::Message;
use redis::AsyncCommands;
use std::sync::Arc;
use tokio::sync::RwLock;

// Internal lookup retry interval for in-memory cache.
const IN_MEMORY_CACHE_LOOKUP_RETRY_INTERVAL_MS: u64 = 10;
// Max cache size in bytes: 1 GB.
const MAX_IN_MEMORY_CACHE_CAPACITY_IN_BYTES: u64 = 1_000_000_000;
// Warm-up cache entries. Pre-fetch the cache entries to warm up the cache.
const WARM_UP_CACHE_ENTRIES: u64 = 20_000;
const MAX_REDIS_FETCH_BATCH_SIZE: usize = 500;

/// InMemoryCache is a simple in-memory cache that stores the protobuf Transaction.
pub struct InMemoryCache {
    /// Cache maps the cache key to the deserialized Transaction.
    cache: Cache<String, Arc<Transaction>>,
    latest_version: Arc<RwLock<u64>>,
    storage_format: StorageFormat,
    _cancellation_token_drop_guard: tokio_util::sync::DropGuard,
}

impl InMemoryCache {
    pub async fn new_with_redis_connection<C>(
        conn: C,
        storage_format: StorageFormat,
    ) -> anyhow::Result<Self>
    where
        C: redis::aio::ConnectionLike + Send + Sync + Clone + 'static,
    {
        let cache = Cache::builder()
            .weigher(|_k, v: &Arc<Transaction>| v.encoded_len() as u32)
            .max_capacity(MAX_IN_MEMORY_CACHE_CAPACITY_IN_BYTES)
            .build();
        let in_memory_latest_version =
            warm_up_the_cache(conn.clone(), cache.clone(), storage_format).await?;

        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();
        let latest_version = Arc::new(RwLock::new(in_memory_latest_version));
        create_update_task(
            conn,
            cache.clone(),
            latest_version.clone(),
            storage_format,
            cancellation_token_clone,
        )
        .await;
        Ok(Self {
            cache,
            latest_version,
            _cancellation_token_drop_guard: cancellation_token.drop_guard(),
            storage_format,
        })
    }

    // This returns the transaction if it exists in the cache.
    // If requested version is not in the cache, it blocks until the version is available.
    // Otherwise, empty.
    pub async fn get_transactions(&self, starting_version: u64) -> Vec<Transaction> {
        let versions_to_fetch = loop {
            let latest_version = *self.latest_version.read().await;
            if starting_version >= latest_version {
                tokio::time::sleep(std::time::Duration::from_millis(
                    IN_MEMORY_CACHE_LOOKUP_RETRY_INTERVAL_MS,
                ))
                .await;
                continue;
            }
            break (starting_version..latest_version).collect::<Vec<u64>>();
        };
        let keys = versions_to_fetch
            .into_iter()
            .map(|version| CacheEntry::build_key(version, self.storage_format))
            .collect::<Vec<String>>();

        let mut arc_transactions = Vec::new();
        for key in keys {
            if let Some(transaction) = self.cache.get(&key) {
                arc_transactions.push(transaction.clone());
            } else {
                break;
            }
        }
        // Actual clone.
        arc_transactions
            .into_iter()
            .map(|t| t.as_ref().clone())
            .collect()
    }
}

/// Warm up the cache with the latest transactions.
async fn warm_up_the_cache<C>(
    conn: C,
    cache: Cache<String, Arc<Transaction>>,
    storage_format: StorageFormat,
) -> anyhow::Result<u64>
where
    C: redis::aio::ConnectionLike + Send + Sync + Clone + 'static,
{
    let mut conn = conn.clone();
    let latest_version = get_config_by_key(&mut conn, "latest_versions")
        .await
        .context("Failed to fetch the latest version from redis")?
        .context("Latest version doesn't exist in Redis")?;
    let versions_to_fetch =
        (latest_version.saturating_sub(WARM_UP_CACHE_ENTRIES)..latest_version).collect();
    let transactions = batch_get_transactions(&mut conn, versions_to_fetch, storage_format).await?;
    for transaction in transactions {
        cache.insert(
            CacheEntry::build_key(transaction.version, storage_format),
            Arc::new(transaction),
        );
    }
    Ok(latest_version)
}

async fn create_update_task<C>(
    conn: C,
    cache: Cache<String, Arc<Transaction>>,
    latest_version: Arc<RwLock<u64>>,
    storage_format: StorageFormat,
    cancellation_token: tokio_util::sync::CancellationToken,
) where
    C: redis::aio::ConnectionLike + Send + Sync + Clone + 'static,
{
    tokio::spawn(async move {
        let mut conn = conn.clone();
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(
            IN_MEMORY_CACHE_LOOKUP_RETRY_INTERVAL_MS,
        ));
        loop {
            let current_latest_version = get_config_by_key(&mut conn, "latest_versions")
                .await
                .context("Failed to fetch the latest version from redis")
                .unwrap()
                .context("Latest version doesn't exist in Redis")
                .unwrap();
            let in_cache_latest_version = { *latest_version.read().await };
            if current_latest_version == in_cache_latest_version {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        tracing::info!("In-memory cache update task is cancelled.");
                        return;
                    },
                    _ = interval.tick() => {
                        continue;
                    },
                }
            }

            let versions_to_fetch = (in_cache_latest_version..current_latest_version).collect();
            let transactions = batch_get_transactions(&mut conn, versions_to_fetch, storage_format)
                .await
                .unwrap();
            for transaction in transactions {
                cache.insert(
                    CacheEntry::build_key(transaction.version, storage_format),
                    Arc::new(transaction),
                );
            }
            *latest_version.write().await = current_latest_version;
        }
    });
}

// TODO: move the following functions to cache operator.
async fn get_config_by_key<C>(conn: &mut C, key: &str) -> anyhow::Result<Option<u64>>
where
    C: redis::aio::ConnectionLike + Send + Sync + Clone + 'static,
{
    let value = redis::cmd("GET").arg(key).query_async(conn).await?;
    Ok(value)
}

async fn batch_get_transactions<C>(
    conn: &mut C,
    versions: Vec<u64>,
    storage_format: StorageFormat,
) -> anyhow::Result<Vec<Transaction>>
where
    C: redis::aio::ConnectionLike + Send + Sync + Clone + 'static,
{
    let keys: Vec<String> = versions
        .into_iter()
        .map(|version| CacheEntry::build_key(version, storage_format))
        .collect();
    let mut tasks: Vec<tokio::task::JoinHandle<anyhow::Result<Vec<Transaction>>>> = Vec::new();
    for chunk in &keys.into_iter().chunks(MAX_REDIS_FETCH_BATCH_SIZE) {
        let keys: Vec<String> = chunk.collect();
        let mut conn = conn.clone();
        tasks.push(tokio::spawn(async move {
            let values = conn.mget::<Vec<String>, Vec<Vec<u8>>>(keys).await?;
            // If any of the values are empty, we return an error.
            if values.iter().any(|v| v.is_empty()) {
                return Err(anyhow::anyhow!("Failed to fetch all the keys"));
            }
            let transactions = values
                .into_iter()
                .map(|v| {
                    let cache_entry = CacheEntry::new(v, storage_format);
                    cache_entry.into_transaction()
                })
                .collect();
            Ok(transactions)
        }));
    }
    // join all.
    let results = futures::future::join_all(tasks).await;
    let mut transactions = Vec::new();
    for result in results {
        transactions.extend(result??);
    }
    anyhow::Result::Ok(transactions)
}
