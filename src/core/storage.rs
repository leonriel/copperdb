use std::io;
use std::ops::Bound;

use async_trait::async_trait;

/// Errors surfaced through the public `StorageEngine` trait. The HTTP layer
/// maps every variant to a response code without looking inside, so the set
/// is intentionally small.
#[derive(thiserror::Error, Debug)]
pub enum EngineError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// The public contract between the HTTP layer and the underlying storage
/// engine. Async because the server wraps blocking engine calls in
/// `tokio::task::spawn_blocking` so WAL fsyncs and SSTable reads don't stall
/// a runtime worker.
///
/// Implementors must be cheap to `Arc::clone` — the router hands out one
/// `Arc<dyn StorageEngine>` that is cloned per request.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), EngineError>;
    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, EngineError>;
    async fn delete(&self, key: String) -> Result<(), EngineError>;

    /// Range scan over `[start, end)` (Bound flexibility on both ends),
    /// returning up to `limit` live (key, value) pairs in ascending key order.
    /// Tombstones suppress their key from the output; only the newest version
    /// of each key is returned.
    async fn scan(
        &self,
        start: Bound<String>,
        end: Bound<String>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, EngineError>;
}
