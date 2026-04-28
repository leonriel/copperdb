use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;

use crate::engine::EngineCore;
use crate::manifest::{sst_path};
use crate::memtable::MemTable;
use crate::sstable::writer::SsTableBuilder;
use crate::wal::wal_path;

/// Entry point for the background flush thread.
///
/// Holds a strong `Arc<EngineCore>` so the shared state stays alive for the
/// duration of any in-flight flush. Exits when `LsmEngine::drop` sets
/// `core.shutdown` and wakes the channel — at which point this thread's Arc
/// is the last outstanding reference to `EngineCore` and it's dropped on
/// return.
pub fn run(core: Arc<EngineCore>, rx: Receiver<()>) {
    for () in &rx {
        if core.shutdown.load(Ordering::SeqCst) {
            break;
        }
        flush_pending(&core);
    }
}

/// Flush all immutable MemTables that are currently waiting, in FIFO order.
fn flush_pending(engine: &EngineCore) {
    while let Some(table) = engine.state.get_oldest_immutable() {
        // Spin-wait for any in-flight writers on this now-frozen table.
        // The table is immutable so no new writers will increment the counter;
        // we just need existing ones to finish their current insert.
        while table.active_writers() > 0 {
            std::hint::spin_loop();
        }

        let file_id = engine.alloc_sst_id();
        let path = sst_path(&engine.data_dir, file_id);

        let Some(path_str) = path.to_str() else {
            eprintln!("[flusher] SSTable path is not valid UTF-8: {:?}", path);
            return;
        };
        let mut builder = match SsTableBuilder::new(path_str) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[flusher] failed to create SSTable file {:?}: {}", path, e);
                return;
            }
        };

        let iter = table.get_iterator(Bound::Unbounded, Bound::Unbounded);
        if let Err(e) = builder.build_from_iterator(iter) {
            eprintln!("[flusher] failed to write SSTable {:?}: {}", path, e);
            return;
        }

        let Some((smallest_key, largest_key, max_seq)) = builder.summary() else {
            eprintln!("[flusher] SSTable {:?} has no keys after build; skipping", path);
            return;
        };

        if let Err(e) = engine.record_flush(file_id, smallest_key, largest_key, max_seq) {
            eprintln!("[flusher] failed to record flush in manifest for {:?}: {}", path, e);
            return;
        }

        engine.state.drop_immutable(&table);

        // The immutable table's ID equals the WAL generation number it was
        // paired with (assigned in LsmEngine::rotate_wal_and_memtable).
        // Deleting the WAL is safe now that every record is on disk.
        // Delete this WAL and any older stale WALs (e.g. from recovery).                                                                                                            
        let flushed_gen = table.id();                                                                                                                                                
        if let Ok(entries) = std::fs::read_dir(&engine.data_dir) {                                                                                                                   
            for entry in entries.flatten() {                                                                                                                                         
                let path = entry.path();                                                                                                                                             
                if path.extension().and_then(|s| s.to_str()) != Some("wal") {                                                                                                        
                    continue;                                                                                                                                                        
                }
                let maybe_gen = path.file_stem()                                                                                                                                           
                    .and_then(|s| s.to_str())                                                                                                                                        
                    .and_then(|s| s.parse::<u64>().ok());
                if let Some(generation) = maybe_gen {                                                                                                                                               
                    if generation <= flushed_gen {                                                                                                                                            
                        if let Err(e) = std::fs::remove_file(&path) {
                            // Non-fatal: the WAL is redundant, not missing it is fine.
                            eprintln!("[flusher] could not delete WAL {:?}: {}", path, e);
                        }
                    }                                                                                                                                                                
                }       
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::LsmEngine;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    fn tmp_dir() -> std::path::PathBuf {
        use std::sync::atomic::AtomicUsize;
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir()
            .join(format!("copperdb_flusher_{}_{}", std::process::id(), id));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// Write enough data to cross the memtable threshold, trigger a freeze, and
    /// verify that the flusher creates a .sst file and updates the MANIFEST.
    ///
    /// Uses a 200 KB threshold (instead of 64 MB) so the SSTable index stays
    /// within the current 4 KB index-block limit (~50 data blocks max).
    #[test]
    fn flushes_immutable_to_sst() {
        const TEST_MEMTABLE_SIZE: usize = 200 * 1024; // 200 KB

        let dir = tmp_dir();
        let engine = LsmEngine::open_with_memtable_size(&dir, TEST_MEMTABLE_SIZE).unwrap();

        // 256 entries × ~1 KB each ≈ 256 KB → exceeds the 200 KB threshold.
        for i in 0u64..256 {
            let key = format!("key_{:06}", i);
            let val = vec![i as u8; 1000];
            engine.put(key, val).unwrap();
        }

        // Give the flusher time to complete.
        std::thread::sleep(Duration::from_millis(500));

        let sst_count = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
            .count();
        assert!(sst_count > 0, "Expected at least one .sst file after flush");
    }

    /// After a flush the paired WAL file should have been deleted.
    #[test]
    fn wal_deleted_after_flush() {
        const TEST_MEMTABLE_SIZE: usize = 200 * 1024;

        let dir = tmp_dir();
        let engine = LsmEngine::open_with_memtable_size(&dir, TEST_MEMTABLE_SIZE).unwrap();

        for i in 0u64..256 {
            engine.put(format!("key_{:06}", i), vec![i as u8; 1000]).unwrap();
        }

        std::thread::sleep(Duration::from_millis(500));

        // The flushed memtable's paired WAL file should be gone.
        // Generation 1 is always the first WAL created on a fresh dir.
        let wal_count = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("wal"))
            .count();

        let sst_count = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
            .count();

        assert!(sst_count > 0, "Expected at least one .sst file");
        // There should be at most one WAL left (the active one for the current memtable).
        assert!(wal_count <= 1, "Flushed WAL files should have been deleted, found {}", wal_count);
    }

    /// WAL files left over from a previous boot must be cleaned up when the
    /// recovery memtable is eventually flushed.
    ///
    /// Scenario: boot → write → crash (drop engine without flushing) → reboot
    /// → the old WAL is replayed into a new memtable with a higher generation.
    /// When that memtable is flushed, the stale WAL from the first boot must
    /// also be deleted.
    #[test]
    fn stale_wals_from_prior_boot_are_deleted() {
        const TEST_MEMTABLE_SIZE: usize = 200 * 1024;

        let dir = tmp_dir();

        // First boot: write some data but do NOT trigger a flush.
        // This leaves a WAL on disk that will be replayed on the next boot.
        {
            let engine = LsmEngine::open_with_memtable_size(&dir, TEST_MEMTABLE_SIZE).unwrap();
            for i in 0u64..10 {
                engine.put(format!("old_{:04}", i), vec![i as u8; 100]).unwrap();
            }
            // Drop without flushing — WAL survives.
        }

        let pre_reboot_wals: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("wal"))
            .map(|e| e.path())
            .collect();
        assert!(!pre_reboot_wals.is_empty(), "Expected at least one WAL from the first boot");

        // Second boot: the old WAL is replayed into a memtable with a new,
        // higher generation. Write enough to trigger a flush.
        {
            let engine = LsmEngine::open_with_memtable_size(&dir, TEST_MEMTABLE_SIZE).unwrap();
            for i in 0u64..256 {
                engine.put(format!("new_{:06}", i), vec![i as u8; 1000]).unwrap();
            }

            std::thread::sleep(Duration::from_millis(500));

            // After the flush, all old WALs should be gone.
            let wal_count = std::fs::read_dir(&dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("wal"))
                .count();

            assert!(
                wal_count <= 1,
                "Stale WALs from prior boot should have been deleted, found {}",
                wal_count,
            );

            // The old WAL files specifically should no longer exist.
            for old_wal in &pre_reboot_wals {
                assert!(
                    !old_wal.exists(),
                    "Stale WAL {:?} was not cleaned up",
                    old_wal,
                );
            }
        }
    }
}
