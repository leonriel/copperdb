mod core;
mod db;
mod memtable;
mod sstable;
mod wal;
mod compaction;
mod manifest;
mod flusher;
mod server;

use std::path::PathBuf;
use std::sync::Arc;

use crate::core::StorageEngine;
use crate::db::{LsmEngine, LsmHandle};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir: PathBuf = std::env::var("COPPERDB_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./copperdb-data"));

    let addr = std::env::var("COPPERDB_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());

    let engine: Arc<LsmEngine> = LsmEngine::open(&data_dir)?;
    let handle: Arc<dyn StorageEngine> = LsmHandle::new(engine);
    let app = server::build_router(handle);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    eprintln!("[copperdb] listening on http://{} (data: {})", addr, data_dir.display());

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("[copperdb] shutting down");
}
