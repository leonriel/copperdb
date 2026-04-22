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
        1 => prop::collection::vec(any::<u8>(), 4096..8192),
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

    /// Multiple crash/reboot cycles: write some ops, drop, reopen, write
    /// more, drop, reopen — then verify final state.
    #[test]
    fn multiple_crash_reboot_cycles(
        ops1 in ops_strategy(100),
        ops2 in ops_strategy(100),
        ops3 in ops_strategy(100),
    ) {
        let dir = tmp_dir();
        let mut oracle: BTreeMap<String, Vec<u8>> = BTreeMap::new();

        // Cycle 1
        {
            let engine = LsmEngine::open_with_memtable_size(&dir, 4 * 1024).unwrap();
            for op in &ops1 {
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
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Verify after first reboot
        {
            let engine = LsmEngine::open_with_memtable_size(&dir, 4 * 1024).unwrap();
            for i in 0..KEY_SPACE {
                let k = format!("key_{:03}", i);
                let actual = engine.get(&k);
                let expected = oracle.get(&k).cloned();
                prop_assert_eq!(&actual, &expected,
                    "after cycle 1 reboot, get({}) mismatch", k);
            }

            // Cycle 2: write more on top of recovered state
            for op in &ops2 {
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
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Verify after second reboot
        {
            let engine = LsmEngine::open_with_memtable_size(&dir, 4 * 1024).unwrap();
            for i in 0..KEY_SPACE {
                let k = format!("key_{:03}", i);
                let actual = engine.get(&k);
                let expected = oracle.get(&k).cloned();
                prop_assert_eq!(&actual, &expected,
                    "after cycle 2 reboot, get({}) mismatch", k);
            }

            // Cycle 3
            for op in &ops3 {
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
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Final verification after third reboot
        let engine = LsmEngine::open(&dir).unwrap();
        for i in 0..KEY_SPACE {
            let k = format!("key_{:03}", i);
            let actual = engine.get(&k);
            let expected = oracle.get(&k).cloned();
            prop_assert_eq!(&actual, &expected,
                "after cycle 3 reboot, get({}) mismatch", k);
        }
    }

    /// Spawn multiple writer threads doing concurrent puts and deletes,
    /// then drop the engine abruptly and verify recovery produces no
    /// corruption. Because thread interleaving is non-deterministic, we
    /// cannot predict the exact final state. Instead we check:
    ///   1. The engine reopens without error.
    ///   2. Every key's value is one that a thread actually wrote (not garbage).
    ///   3. No panics or crashes during recovery.
    #[test]
    fn concurrent_writers_then_crash(ops in ops_strategy(4000)) {
        use std::sync::{Arc, Barrier};
        use std::collections::{HashMap, HashSet};
        use std::thread;

        let dir = tmp_dir();

        let num_threads = 8;
        let chunks: Vec<Vec<Op>> = ops
            .chunks(ops.len().max(1) / num_threads + 1)
            .map(|c| c.to_vec())
            .collect();

        // Collect every value ever written per key, so we can verify the
        // recovered value is one of them (not corrupt data).
        let mut all_values: HashMap<String, HashSet<Vec<u8>>> = HashMap::new();
        let mut any_delete: HashSet<String> = HashSet::new();
        for op in &ops {
            match op {
                Op::Put(k, v) => {
                    all_values.entry(k.clone()).or_default().insert(v.clone());
                }
                Op::Delete(k) => {
                    any_delete.insert(k.clone());
                }
                Op::Get(_) => {}
            }
        }

        {
            let engine = Arc::new(
                LsmEngine::open_with_memtable_size(&dir, 4 * 1024).unwrap()
            );
            let barrier = Arc::new(Barrier::new(chunks.len()));

            let handles: Vec<_> = chunks.into_iter().map(|chunk| {
                let engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();
                    for op in &chunk {
                        match op {
                            Op::Put(k, v) => {
                                engine.put(k.clone(), v.clone()).unwrap();
                            }
                            Op::Delete(k) => {
                                engine.delete(k.clone()).unwrap();
                            }
                            Op::Get(_) => {}
                        }
                    }
                })
            }).collect();

            for h in handles {
                h.join().unwrap();
            }
        }

        // Sleep so background workers (flusher and compaction worker) can finish what they're doing
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Reopen and verify no corruption.
        let engine = LsmEngine::open(&dir).unwrap();
        for i in 0..KEY_SPACE {
            let k = format!("key_{:03}", i);
            match engine.get(&k) {
                Some(v) => {
                    // The value must be one that was actually written.
                    let valid = all_values.get(&k)
                        .map(|vals| vals.contains(&v))
                        .unwrap_or(false);
                    prop_assert!(valid,
                        "after concurrent crash, get({}) returned a value \
                         that was never written", k);
                }
                None => {
                    // Key is absent — valid if it was never written OR if
                    // a delete was issued for it at some point.
                    let was_written = all_values.contains_key(&k);
                    let was_deleted = any_delete.contains(&k);
                    prop_assert!(!was_written || was_deleted,
                        "after concurrent crash, get({}) returned None but \
                         the key was written and never deleted", k);
                }
            }
        }
    }
}
