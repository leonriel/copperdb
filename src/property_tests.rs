//! Property tests for `LsmEngine`.
//!
//! Each test generates a random sequence of `put`/`delete`/`get` operations and
//! applies them both to an `LsmEngine` and to a `BTreeMap` oracle, asserting
//! that every `get` agrees. Three variants exercise different parts of the
//! stack:
//!
//! 1. `ops_match_oracle`           — large memtable, no flushes. Tests the WAL
//!                                   + MemTable + read-path plumbing in isolation.
//! 2. `restart_preserves_state`    — large memtable, then drop & reopen. Tests
//!                                   WAL recovery.
//! 3. `flushes_and_compactions_preserve_state` — tiny memtable, many flushes.
//!                                   Tests the immutable queue, flusher, SSTable
//!                                   writer/reader, and compaction.
//!
//! The default `cases: 32` is a smoke-test budget. To actually hunt bugs, run
//! with `PROPTEST_CASES=1024` (or higher) in release mode. Reproductions are
//! auto-saved under `proptest-regressions/` — check that directory in alongside
//! any fix so the regression is replayed forever after.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use proptest::prelude::*;

use crate::engine::LsmEngine;

fn tmp_dir() -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!(
        "copperdb_property_{}_{}",
        std::process::id(),
        id,
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[derive(Debug, Clone)]
enum Op {
    Put(String, Vec<u8>),
    Delete(String),
    Get(String),
}

/// Small key pool so overwrites and tombstone masking happen frequently.
const KEY_SPACE: u32 = 20;

fn key_strategy() -> impl Strategy<Value = String> {
    (0u32..KEY_SPACE).prop_map(|i| format!("key_{:03}", i))
}

fn value_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        6 => prop::collection::vec(any::<u8>(), 0..32),
        2 => prop::collection::vec(any::<u8>(), 1024..4096),
    ]
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (key_strategy(), value_strategy()).prop_map(|(k, v)| Op::Put(k, v)),
        2 => key_strategy().prop_map(Op::Delete),
        5 => key_strategy().prop_map(Op::Get),
    ]
}

fn ops_strategy(max_len: usize) -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op_strategy(), 1..max_len)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        .. ProptestConfig::default()
    })]

    #[test]
    fn ops_match_oracle(ops in ops_strategy(120)) {
        let dir = tmp_dir();
        // 1 MiB memtable — no flushes for reasonable op counts. Isolates the
        // WAL/MemTable/read-path to check basic correctness.
        let engine = LsmEngine::open_with_memtable_size(&dir, 1024 * 1024).unwrap();
        let mut oracle: BTreeMap<String, Vec<u8>> = BTreeMap::new();

        for op in &ops {
            match op {
                Op::Put(k, v) => {
                    engine.put(k.clone(), v.clone()).unwrap();
                    oracle.insert(k.clone(), v.clone());
                }
                Op::Delete(k) => {
                    engine.delete(k.clone()).unwrap();
                    oracle.remove(k);
                }
                Op::Get(k) => {
                    let actual = engine.get(k);
                    let expected = oracle.get(k).cloned();
                    prop_assert_eq!(&actual, &expected,
                        "get({}) mismatch mid-sequence", k);
                }
            }
        }

        for i in 0..KEY_SPACE {
            let k = format!("key_{:03}", i);
            let actual = engine.get(&k);
            let expected = oracle.get(&k).cloned();
            prop_assert_eq!(&actual, &expected, "final get({}) mismatch", k);
        }
    }

    #[test]
    fn restart_preserves_state(ops in ops_strategy(80)) {
        let dir = tmp_dir();
        let mut oracle: BTreeMap<String, Vec<u8>> = BTreeMap::new();

        {
            // Large memtable to minimise flush activity during the initial run
            // so the post-drop reopen is a pure WAL-replay test.
            let engine = LsmEngine::open_with_memtable_size(&dir, 4 * 1024 * 1024).unwrap();
            for op in &ops {
                match op {
                    Op::Put(k, v) => {
                        engine.put(k.clone(), v.clone()).unwrap();
                        oracle.insert(k.clone(), v.clone());
                    }
                    Op::Delete(k) => {
                        engine.delete(k.clone()).unwrap();
                        oracle.remove(k);
                    }
                    Op::Get(_) => {}
                }
            }
        }

        let engine = LsmEngine::open(&dir).unwrap();
        for i in 0..KEY_SPACE {
            let k = format!("key_{:03}", i);
            let actual = engine.get(&k);
            let expected = oracle.get(&k).cloned();
            prop_assert_eq!(&actual, &expected,
                "after restart, get({}) mismatch", k);
        }
    }

    #[test]
    fn flushes_and_compactions_preserve_state(ops in ops_strategy(500)) {
        let dir = tmp_dir();
        // 4 KiB memtable — a single large-value Put forces a freeze.
        let engine = LsmEngine::open_with_memtable_size(&dir, 4 * 1024).unwrap();
        let mut oracle: BTreeMap<String, Vec<u8>> = BTreeMap::new();

        for op in &ops {
            match op {
                Op::Put(k, v) => {
                    engine.put(k.clone(), v.clone()).unwrap();
                    oracle.insert(k.clone(), v.clone());
                }
                Op::Delete(k) => {
                    engine.delete(k.clone()).unwrap();
                    oracle.remove(k);
                }
                Op::Get(k) => {
                    let actual = engine.get(k);
                    let expected = oracle.get(k).cloned();
                    prop_assert_eq!(&actual, &expected,
                        "get({}) mismatch mid-sequence (churn)", k);
                }
            }
        }

        for i in 0..KEY_SPACE {
            let k = format!("key_{:03}", i);
            let actual = engine.get(&k);
            let expected = oracle.get(&k).cloned();
            prop_assert_eq!(&actual, &expected,
                "final get({}) mismatch (churn)", k);
        }
    }
}
