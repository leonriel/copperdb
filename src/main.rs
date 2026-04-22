mod core;
mod engine;
mod memtable;
mod sstable;
mod wal;
mod compaction;
mod manifest;
mod flusher;
mod server;

#[cfg(test)]
mod property_tests;

use std::path::PathBuf;
use std::sync::Arc;

use crate::core::StorageEngine;
use crate::engine::{LsmEngine, LsmHandle};

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn tmp_dir() -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir()
            .join(format!("copperdb_main_{}_{}", std::process::id(), id));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[tokio::test]
    async fn server_starts_and_shuts_down_gracefully() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();
        let handle: Arc<dyn StorageEngine> = LsmHandle::new(engine);
        let app = server::build_router(handle);

        // Bind to port 0 so the OS assigns a free port.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // A notify that acts as the shutdown signal.
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let shutdown_rx = Arc::clone(&shutdown);

        let server_handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { shutdown_rx.notified().await })
                .await
                .unwrap();
        });

        // Verify the server is responding with a raw HTTP/1.1 request.
        let response = tokio::task::spawn_blocking(move || {
            use std::io::{Read, Write};
            let mut stream = std::net::TcpStream::connect(addr).unwrap();
            stream.write_all(
                b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            ).unwrap();
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).unwrap();
            String::from_utf8_lossy(&buf).to_string()
        })
        .await
        .unwrap();

        assert!(
            response.starts_with("HTTP/1.1 200"),
            "expected 200 OK, got: {}",
            response.lines().next().unwrap_or("(empty)"),
        );

        // Signal shutdown and wait for the server task to finish.
        shutdown.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), server_handle)
            .await
            .expect("server did not shut down within 5 seconds")
            .expect("server task panicked");
    }

    /// Verify that sending SIGINT (Ctrl-C) triggers `shutdown_signal()` and
    /// causes the server to exit cleanly.
    ///
    /// Tokio's `ctrl_c()` installs a signal handler that catches SIGINT instead
    /// of terminating the process, so raising SIGINT here is safe.
    #[tokio::test]
    async fn ctrl_c_triggers_graceful_shutdown() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();
        let handle: Arc<dyn StorageEngine> = LsmHandle::new(engine);
        let app = server::build_router(handle);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .unwrap();
        });

        // Give the server a moment to start accepting connections.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Verify the server is responding.
        let response = tokio::task::spawn_blocking(move || {
            use std::io::{Read, Write};
            let mut stream = std::net::TcpStream::connect(addr).unwrap();
            stream.write_all(
                b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            ).unwrap();
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).unwrap();
            String::from_utf8_lossy(&buf).to_string()
        })
        .await
        .unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));

        // Send SIGINT to our own process. Tokio's signal handler catches it
        // and resolves the ctrl_c() future without killing the process.
        let pid = std::process::id().to_string();
        std::process::Command::new("kill")
            .args(["-INT", &pid])
            .status()
            .expect("failed to send SIGINT");

        // The server should shut down promptly.
        tokio::time::timeout(std::time::Duration::from_secs(5), server_handle)
            .await
            .expect("server did not shut down within 5 seconds after SIGINT")
            .expect("server task panicked");
    }
}
