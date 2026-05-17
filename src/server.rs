use std::ops::Bound;
use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::core::{EngineError, StorageEngine};

/// Shared application state: a type-erased handle to the storage engine.
/// `server.rs` depends only on the `StorageEngine` trait, never on
/// `LsmEngine` — the concrete engine is plugged in by `main.rs`.
pub type SharedEngine = Arc<dyn StorageEngine>;

/// Build the router. Callers pass any `Arc<dyn StorageEngine>` — a real
/// `LsmEngine` in production or a mock in tests.
pub fn build_router(engine: SharedEngine) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/kv", get(scan_kv))
        .route(
            "/kv/{key}",
            get(get_kv).put(put_kv).delete(delete_kv),
        )
        .with_state(engine)
}

async fn health() -> &'static str {
    "ok"
}

async fn get_kv(
    State(engine): State<SharedEngine>,
    Path(key): Path<String>,
) -> Result<(StatusCode, Bytes), StatusCode> {
    match engine.get(key).await {
        Ok(Some(value)) => Ok((StatusCode::OK, Bytes::from(value))),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => Err(log_and_500("get", e)),
    }
}

async fn put_kv(
    State(engine): State<SharedEngine>,
    Path(key): Path<String>,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    match engine.put(key, body.to_vec()).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(e) => Err(log_and_500("put", e)),
    }
}

async fn delete_kv(
    State(engine): State<SharedEngine>,
    Path(key): Path<String>,
) -> Result<StatusCode, StatusCode> {
    match engine.delete(key).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(e) => Err(log_and_500("delete", e)),
    }
}

fn log_and_500(op: &str, err: EngineError) -> StatusCode {
    eprintln!("[server] {} failed: {}", op, err);
    StatusCode::INTERNAL_SERVER_ERROR
}

// ---------------------------------------------------------------------------
// Range scan: GET /kv?start=…&end=…&limit=…
//
// Half-open `[start, end)` per the LevelDB/RocksDB/YCSB convention. Both
// bounds and `limit` are optional. Response is `{"results": [...]}` with
// values base64-encoded — JSON has no native binary type and the engine
// stores arbitrary bytes, so any serialisation that packs multiple values
// into one response needs *some* encoding here. The single-key endpoints
// don't have that problem (their body *is* the value), so they remain
// raw-bytes.
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ScanParams {
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    end: Option<String>,
    #[serde(default = "default_scan_limit")]
    limit: usize,
}

fn default_scan_limit() -> usize {
    1000
}

#[derive(Serialize)]
struct ScanResponse {
    results: Vec<ScanEntry>,
}

#[derive(Serialize)]
struct ScanEntry {
    key: String,
    /// Base64-encoded bytes.
    value: String,
}

async fn scan_kv(
    State(engine): State<SharedEngine>,
    Query(params): Query<ScanParams>,
) -> Result<Json<ScanResponse>, StatusCode> {
    let start = params
        .start
        .map_or(Bound::Unbounded, Bound::Included);
    let end = params
        .end
        .map_or(Bound::Unbounded, Bound::Excluded);

    let pairs = engine
        .scan(start, end, params.limit)
        .await
        .map_err(|e| log_and_500("scan", e))?;

    let b64 = base64::engine::general_purpose::STANDARD;
    let results = pairs
        .into_iter()
        .map(|(key, value)| ScanEntry {
            key,
            value: b64.encode(&value),
        })
        .collect();
    Ok(Json(ScanResponse { results }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    // -----------------------------------------------------------------------
    // Handler-level tests with an in-memory mock — proves the HTTP wiring is
    // independent of any real engine.
    // -----------------------------------------------------------------------

    struct MockEngine {
        inner: Mutex<HashMap<String, Vec<u8>>>,
        fail_next: Mutex<bool>,
    }

    impl MockEngine {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: Mutex::new(HashMap::new()),
                fail_next: Mutex::new(false),
            })
        }

        fn arm_failure(&self) {
            *self.fail_next.lock().unwrap() = true;
        }

        fn take_failure(&self) -> bool {
            let mut guard = self.fail_next.lock().unwrap();
            let armed = *guard;
            *guard = false;
            armed
        }
    }

    #[async_trait]
    impl StorageEngine for MockEngine {
        async fn put(&self, key: String, value: Vec<u8>) -> Result<(), EngineError> {
            if self.take_failure() {
                return Err(EngineError::Io(std::io::Error::other("armed failure")));
            }
            self.inner.lock().unwrap().insert(key, value);
            Ok(())
        }

        async fn get(&self, key: String) -> Result<Option<Vec<u8>>, EngineError> {
            if self.take_failure() {
                return Err(EngineError::Io(std::io::Error::other("armed failure")));
            }
            Ok(self.inner.lock().unwrap().get(&key).cloned())
        }

        async fn delete(&self, key: String) -> Result<(), EngineError> {
            if self.take_failure() {
                return Err(EngineError::Io(std::io::Error::other("armed failure")));
            }
            self.inner.lock().unwrap().remove(&key);
            Ok(())
        }

        async fn scan(
            &self,
            start: std::ops::Bound<String>,
            end: std::ops::Bound<String>,
            limit: usize,
        ) -> Result<Vec<(String, Vec<u8>)>, EngineError> {
            if self.take_failure() {
                return Err(EngineError::Io(std::io::Error::other("armed failure")));
            }
            let mut entries: Vec<(String, Vec<u8>)> = self
                .inner
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| in_range(k.as_str(), &start, &end))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            entries.truncate(limit);
            Ok(entries)
        }
    }

    /// `[start, end)` membership check used by `MockEngine::scan`.
    fn in_range(key: &str, start: &std::ops::Bound<String>, end: &std::ops::Bound<String>) -> bool {
        use std::ops::Bound::*;
        let after_start = match start {
            Included(s) => key >= s.as_str(),
            Excluded(s) => key > s.as_str(),
            Unbounded => true,
        };
        let before_end = match end {
            Included(e) => key <= e.as_str(),
            Excluded(e) => key < e.as_str(),
            Unbounded => true,
        };
        after_start && before_end
    }

    fn router_with(engine: Arc<MockEngine>) -> Router {
        build_router(engine as Arc<dyn StorageEngine>)
    }

    async fn read_body(body: Body) -> Vec<u8> {
        body.collect().await.unwrap().to_bytes().to_vec()
    }

    fn req(method: Method, uri: &str, body: Vec<u8>) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::from(body))
            .unwrap()
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let app = router_with(MockEngine::new());
        let resp = app
            .oneshot(req(Method::GET, "/health", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(read_body(resp.into_body()).await, b"ok");
    }

    #[tokio::test]
    async fn get_missing_key_returns_404() {
        let app = router_with(MockEngine::new());
        let resp = app
            .oneshot(req(Method::GET, "/kv/absent", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn put_then_get_roundtrip() {
        let app = router_with(MockEngine::new());

        let resp = app
            .clone()
            .oneshot(req(Method::PUT, "/kv/foo", b"bar".to_vec()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let resp = app
            .oneshot(req(Method::GET, "/kv/foo", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(read_body(resp.into_body()).await, b"bar");
    }

    #[tokio::test]
    async fn put_overwrites_existing_value() {
        let app = router_with(MockEngine::new());

        for v in [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()] {
            app.clone()
                .oneshot(req(Method::PUT, "/kv/k", v))
                .await
                .unwrap();
        }

        let resp = app
            .oneshot(req(Method::GET, "/kv/k", vec![]))
            .await
            .unwrap();
        assert_eq!(read_body(resp.into_body()).await, b"three");
    }

    #[tokio::test]
    async fn delete_then_get_returns_404() {
        let app = router_with(MockEngine::new());

        app.clone()
            .oneshot(req(Method::PUT, "/kv/tmp", b"x".to_vec()))
            .await
            .unwrap();
        let resp = app
            .clone()
            .oneshot(req(Method::DELETE, "/kv/tmp", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let resp = app
            .oneshot(req(Method::GET, "/kv/tmp", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_missing_key_is_noop_204() {
        // Tombstones are a valid record type; deleting an absent key isn't an error.
        let app = router_with(MockEngine::new());
        let resp = app
            .oneshot(req(Method::DELETE, "/kv/never_existed", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn put_empty_body_stores_empty_value() {
        // An empty value is valid and must be distinguishable from "missing".
        let app = router_with(MockEngine::new());

        app.clone()
            .oneshot(req(Method::PUT, "/kv/blank", vec![]))
            .await
            .unwrap();
        let resp = app
            .oneshot(req(Method::GET, "/kv/blank", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(read_body(resp.into_body()).await, Vec::<u8>::new());
    }

    #[tokio::test]
    async fn binary_value_preserved_exactly() {
        let app = router_with(MockEngine::new());
        let payload: Vec<u8> = (0u8..=255).collect();

        app.clone()
            .oneshot(req(Method::PUT, "/kv/bin", payload.clone()))
            .await
            .unwrap();
        let resp = app
            .oneshot(req(Method::GET, "/kv/bin", vec![]))
            .await
            .unwrap();
        assert_eq!(read_body(resp.into_body()).await, payload);
    }

    #[tokio::test]
    async fn percent_encoded_key_is_decoded() {
        // "%2F" is "/" — the router must deliver the decoded key to the handler.
        let app = router_with(MockEngine::new());

        app.clone()
            .oneshot(req(Method::PUT, "/kv/path%2Fto%2Fthing", b"v".to_vec()))
            .await
            .unwrap();

        let resp = app
            .oneshot(req(Method::GET, "/kv/path%2Fto%2Fthing", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(read_body(resp.into_body()).await, b"v");
    }

    #[tokio::test]
    async fn engine_error_maps_to_500() {
        let engine = MockEngine::new();
        engine.arm_failure();
        let app = router_with(engine);

        let resp = app
            .oneshot(req(Method::PUT, "/kv/x", b"v".to_vec()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn unknown_route_is_404() {
        let app = router_with(MockEngine::new());
        let resp = app
            .oneshot(req(Method::GET, "/nope", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unsupported_method_is_405() {
        let app = router_with(MockEngine::new());
        let resp = app
            .oneshot(req(Method::POST, "/kv/k", b"v".to_vec()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    // -----------------------------------------------------------------------
    // End-to-end: real `LsmEngine` reached through the async trait, proving
    // the spawn_blocking adapter in db.rs actually works.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn end_to_end_with_real_lsm_engine() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir()
            .join(format!("copperdb_server_e2e_{}_{}", std::process::id(), id));

        let lsm = crate::engine::LsmEngine::open(&dir).unwrap();
        let handle: Arc<dyn StorageEngine> = crate::engine::LsmHandle::new(lsm);
        let app = build_router(handle);

        app.clone()
            .oneshot(req(Method::PUT, "/kv/city", b"london".to_vec()))
            .await
            .unwrap();
        app.clone()
            .oneshot(req(Method::PUT, "/kv/temp", b"cold".to_vec()))
            .await
            .unwrap();

        let resp = app
            .clone()
            .oneshot(req(Method::GET, "/kv/city", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(read_body(resp.into_body()).await, b"london");

        app.clone()
            .oneshot(req(Method::DELETE, "/kv/temp", vec![]))
            .await
            .unwrap();
        let resp = app
            .oneshot(req(Method::GET, "/kv/temp", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -----------------------------------------------------------------------
    // /kv scan endpoint
    // -----------------------------------------------------------------------

    /// Decode a base64 string back to bytes. Helper for asserting on response
    /// values without the encoding obscuring the test intent.
    fn b64_decode(s: &str) -> Vec<u8> {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .unwrap()
    }

    async fn scan_json(app: Router, uri: &str) -> serde_json::Value {
        let resp = app.oneshot(req(Method::GET, uri, vec![])).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "expected 200 OK for {}", uri);
        let body = read_body(resp.into_body()).await;
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn scan_empty_engine_returns_empty_results() {
        let app = router_with(MockEngine::new());
        let json = scan_json(app, "/kv").await;
        assert_eq!(json["results"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn scan_returns_sorted_keys() {
        let app = router_with(MockEngine::new());

        // PUT in non-sorted order — scan should return sorted.
        for (k, v) in [("c", "C"), ("a", "A"), ("b", "B")] {
            app.clone()
                .oneshot(req(Method::PUT, &format!("/kv/{}", k), v.as_bytes().to_vec()))
                .await
                .unwrap();
        }

        let json = scan_json(app, "/kv").await;
        let results = json["results"].as_array().unwrap();
        assert_eq!(results.len(), 3);
        for (i, expected_key) in ["a", "b", "c"].iter().enumerate() {
            assert_eq!(results[i]["key"].as_str().unwrap(), *expected_key);
        }
        // Values round-trip through base64.
        assert_eq!(b64_decode(results[0]["value"].as_str().unwrap()), b"A");
        assert_eq!(b64_decode(results[1]["value"].as_str().unwrap()), b"B");
        assert_eq!(b64_decode(results[2]["value"].as_str().unwrap()), b"C");
    }

    #[tokio::test]
    async fn scan_respects_start_and_end() {
        let app = router_with(MockEngine::new());
        for k in ["a", "b", "c", "d", "e"] {
            app.clone()
                .oneshot(req(Method::PUT, &format!("/kv/{}", k), k.as_bytes().to_vec()))
                .await
                .unwrap();
        }

        // [b, d) — half-open. Expect b, c.
        let json = scan_json(app, "/kv?start=b&end=d").await;
        let keys: Vec<&str> = json["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, vec!["b", "c"]);
    }

    #[tokio::test]
    async fn scan_respects_limit() {
        let app = router_with(MockEngine::new());
        for k in ["a", "b", "c", "d", "e"] {
            app.clone()
                .oneshot(req(Method::PUT, &format!("/kv/{}", k), k.as_bytes().to_vec()))
                .await
                .unwrap();
        }

        let json = scan_json(app, "/kv?limit=2").await;
        let keys: Vec<&str> = json["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn scan_invalid_limit_returns_400() {
        let app = router_with(MockEngine::new());
        let resp = app
            .oneshot(req(Method::GET, "/kv?limit=not-a-number", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn scan_engine_failure_returns_500() {
        let engine = MockEngine::new();
        engine.arm_failure();
        let app = router_with(engine);

        let resp = app
            .oneshot(req(Method::GET, "/kv", vec![]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
