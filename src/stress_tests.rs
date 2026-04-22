//! Stress tests for `LsmEngine`.
//!
//! Unlike property tests (which reset state each case), these tests hammer
//! a single engine instance with sustained concurrent load to surface race
//! conditions, deadlocks, and corruption under pressure.
//!
//! These tests are expensive. Run them explicitly:
//!     cargo test --release stress_tests -- --ignored

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use crate::engine::LsmEngine;

fn tmp_dir() -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!(
        "copperdb_stress_{}_{}",
        std::process::id(),
        id,
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// 8 writer threads and 4 reader threads hit the same engine simultaneously.
/// Writers do 5,000 ops each (40,000 total writes). Readers do continuous
/// gets throughout. After all writers finish, verify every key holds a value
/// that was actually written.
///
/// Uses a 4 KiB memtable to force frequent flushes and compactions under
/// load.
#[test]
#[ignore] // Run with: cargo test --release stress_tests -- --ignored
fn sustained_concurrent_load() {
    let dir = tmp_dir();
    let engine = Arc::new(
        LsmEngine::open_with_memtable_size(&dir, 4 * 1024).unwrap(),
    );

    const NUM_WRITERS: usize = 8;
    const OPS_PER_WRITER: usize = 5_000;
    const NUM_READERS: usize = 4;
    const KEY_SPACE: usize = 50;

    let barrier = Arc::new(Barrier::new(NUM_WRITERS + NUM_READERS));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut writer_handles = Vec::new();
    let mut reader_handles = Vec::new();

    // Writer threads: each thread writes to a mix of shared and private keys.
    for t in 0..NUM_WRITERS {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);

        writer_handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..OPS_PER_WRITER {
                let key = format!("key_{:04}", i % KEY_SPACE);
                let value = format!("t{}i{}", t, i).into_bytes();

                if i % 10 == 9 {
                    // ~10% deletes
                    engine.delete(key).unwrap();
                } else {
                    engine.put(key, value).unwrap();
                }
            }
        }));
    }

    // Reader threads: continuous gets until writers are done.
    for _ in 0..NUM_READERS {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let done = Arc::clone(&done);

        reader_handles.push(thread::spawn(move || {
            barrier.wait();
            let mut reads = 0u64;
            while !done.load(Ordering::Relaxed) {
                for k in 0..KEY_SPACE {
                    let _ = engine.get(&format!("key_{:04}", k));
                    reads += 1;
                }
            }
            reads
        }));
    }

    // Wait for writers to finish.
    for h in writer_handles {
        h.join().unwrap();
    }
    done.store(true, Ordering::Relaxed);

    // Wait for readers to finish.
    let mut total_reads = 0u64;
    for h in reader_handles {
        total_reads += h.join().unwrap();
    }

    // Give the flusher/compactor time to finish any in-flight work.
    thread::sleep(Duration::from_millis(500));

    // Verify: every key should either be absent (last op was a delete)
    // or hold a value matching the "t{thread}i{index}" pattern.
    for k in 0..KEY_SPACE {
        let key = format!("key_{:04}", k);
        if let Some(v) = engine.get(&key) {
            let s = String::from_utf8(v).expect("value should be valid UTF-8");
            assert!(
                s.starts_with('t') && s.contains('i'),
                "key {} has corrupt value: {:?}",
                key, s,
            );
        }
    }

    eprintln!(
        "[stress] {} writers x {} ops = {} writes, {} reads completed",
        NUM_WRITERS, OPS_PER_WRITER, NUM_WRITERS * OPS_PER_WRITER, total_reads,
    );
}

/// Hammer a single engine through multiple crash/reboot cycles without
/// resetting the data directory. Verifies that accumulated state survives
/// repeated recovery.
#[test]
#[ignore]
fn repeated_crash_reboot_cycles() {
    let dir = tmp_dir();
    let mut expected: HashMap<String, Vec<u8>> = HashMap::new();

    const CYCLES: usize = 10;
    const OPS_PER_CYCLE: usize = 2_000;
    const KEY_SPACE: usize = 30;

    for cycle in 0..CYCLES {
        {
            let engine = LsmEngine::open_with_memtable_size(&dir, 4 * 1024).unwrap();

            for i in 0..OPS_PER_CYCLE {
                let key = format!("key_{:04}", i % KEY_SPACE);
                let value = format!("c{}i{}", cycle, i).into_bytes();

                if i % 8 == 7 {
                    engine.delete(key.clone()).unwrap();
                    expected.remove(&key);
                } else {
                    engine.put(key.clone(), value.clone()).unwrap();
                    expected.insert(key, value);
                }
            }
            // Drop without waiting — simulates abrupt shutdown.
        }
        // Let background threads exit before reopening.
        thread::sleep(Duration::from_millis(100));

        // Verify state after each reboot.
        let engine = LsmEngine::open(&dir).unwrap();
        for k in 0..KEY_SPACE {
            let key = format!("key_{:04}", k);
            let actual = engine.get(&key);
            let exp = expected.get(&key).cloned();
            assert_eq!(
                actual, exp,
                "cycle {}: get({}) mismatch", cycle, key,
            );
        }
    }

    eprintln!(
        "[stress] {} cycles x {} ops = {} total ops, {} keys tracked",
        CYCLES, OPS_PER_CYCLE, CYCLES * OPS_PER_CYCLE, expected.len(),
    );
}
