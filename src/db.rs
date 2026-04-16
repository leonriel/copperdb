use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock, Arc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender;

use crate::core::Record;
use crate::memtable::state::MemTableState;
use crate::wal::{Crc32Checksum, Wal, WalOpType, recover_all};
use crate::manifest::{Manifest, VersionEdit, VersionState};
use crate::sstable::reader::SsTableReader;

const MAX_MEMTABLE_SIZE: usize = 64 * 1024 * 1024;
const MAX_IMMUTABLE_TABLES: usize = 4;

/// Core engine: coordinates WAL writes and MemTable inserts.
///
/// Write path: WAL append → MemTable insert → rotate if frozen → signal flusher.
/// Read path:  MemTableState (active first, then immutables in reverse order).
/// Recovery:   replay all unflushed WAL files on `open`.
pub struct LsmEngine {
    pub(crate) state:    MemTableState,
    active_wal:          Mutex<Wal<Crc32Checksum>>,
    next_seq:            AtomicU64,
    next_wal_gen:        AtomicU64,
    pub(crate) data_dir: PathBuf,
    manifest:            Mutex<Manifest>,
    version:             RwLock<Arc<VersionState>>,
    next_sst_id:         AtomicU64,
    flush_tx:            Sender<()>,
}

/// Returns the path for an SSTable file with the given ID.
/// Zero-padded to 20 digits, matching the WAL naming convention.
pub(crate) fn sst_path(dir: &Path, file_id: u64) -> PathBuf {
    dir.join(format!("{:020}.sst", file_id))
}

impl LsmEngine {
    /// Open (or create) an engine whose data lives in `dir`.
    ///
    /// Replays unflushed WAL files before returning. Returns an `Arc` so the
    /// engine can be shared with the background flusher thread.
    pub fn open(dir: &Path) -> io::Result<Arc<Self>> {
        Self::open_with_memtable_size(dir, MAX_MEMTABLE_SIZE)
    }

    pub(crate) fn open_with_memtable_size(dir: &Path, memtable_size: usize) -> io::Result<Arc<Self>> {
        std::fs::create_dir_all(dir)?;

        let records = recover_all::<Crc32Checksum>(dir)?;
        let next_gen = highest_wal_gen(dir) + 1;
        let max_seq = records.iter().map(|r| r.seq_num).max().unwrap_or(0);

        let state = MemTableState::new(MAX_IMMUTABLE_TABLES, memtable_size, next_gen);

        for r in records {
            let record = match r.op {
                WalOpType::Put => Record::Put(r.value),
                WalOpType::Delete => Record::Delete,
            };
            // Ignore capacity limits during replay. All old WALs get merged
            // into this single starting MemTable.
            state.put(r.key, record, r.seq_num);
        }

        let active_wal = Wal::<Crc32Checksum>::create(dir, next_gen)?;

        let (manifest, version_state) = Manifest::open_or_create(dir)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let next_sst_id = version_state.all_file_ids().max().unwrap_or(0) + 1;

        let (flush_tx, flush_rx) = std::sync::mpsc::channel::<()>();

        let engine = Arc::new(Self {
            state,
            active_wal:   Mutex::new(active_wal),
            next_seq:     AtomicU64::new(max_seq + 1),
            next_wal_gen: AtomicU64::new(next_gen + 1),
            data_dir:     dir.to_path_buf(),
            manifest:     Mutex::new(manifest),
            version:      RwLock::new(Arc::new(version_state)),
            next_sst_id:  AtomicU64::new(next_sst_id),
            flush_tx,
        });

        // The flusher holds a Weak reference to avoid a cycle: if the engine Arc
        // is dropped, the Weak upgrade fails and the flusher exits cleanly.
        let flusher_weak = Arc::downgrade(&engine);
        std::thread::spawn(move || crate::flusher::run(flusher_weak, flush_rx));

        Ok(engine)
    }

    /// Durably write a key-value pair.
    pub fn put(&self, key: String, value: Vec<u8>) -> io::Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);

        {
            let mut wal = self.active_wal.lock().unwrap();
            wal.append_put(seq, &key, &value)?;
        }

        // TODO: stall writes when flusher is falling behind
        // (requires a Condvar notified by the flusher after each drop_immutable)

        if let Some(expected_id) = self.state.put(key, Record::Put(value), seq) {
            self.rotate_wal_and_memtable(expected_id)?;
        }

        Ok(())
    }

    /// Look up a key. Returns `None` if the key was deleted or never written.
    ///
    /// Search order: active MemTable → immutable MemTables → L0 SSTables
    /// (newest-first) → L1+ SSTables (binary search per level).
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        // 1. MemTables: active then immutables, newest first.
        if let Some((record, _)) = self.state.get(key) {
            return match record {
                Record::Put(v) => Some(v),
                Record::Delete => None,
            };
        }

        // 2. SSTables via the current version snapshot.
        let version = self.current_version();

        // L0 files may have overlapping key ranges. Scan newest-first (the
        // Vec is in flush order — oldest at index 0 — so reverse it).
        for meta in version.files_at_level(0).iter().rev() {
            // Skip the file entirely if the key is outside its known range.
            if key < meta.smallest_key.as_str() || key > meta.largest_key.as_str() {
                continue;
            }
            let path = sst_path(&self.data_dir, meta.file_id);
            let Some(path_str) = path.to_str() else { continue };
            let mut reader = match SsTableReader::open(path_str) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[db] failed to open L0 SSTable {:?}: {}", path, e);
                    continue;
                }
            };
            match reader.search(key) {
                Ok(Some((_, Record::Put(v)))) => return Some(v),
                Ok(Some((_, Record::Delete))) => return None,
                Ok(None) => {}
                Err(e) => eprintln!("[db] error searching L0 SSTable {:?}: {}", path, e),
            }
        }

        // L1+ files are non-overlapping and sorted by smallest_key within each
        // level. Use partition_point to find the one candidate file per level.
        for level in 1..7usize {
            let files = version.files_at_level(level);
            if files.is_empty() {
                continue;
            }

            // Find the last file whose smallest_key <= key.
            let pos = files.partition_point(|m| m.smallest_key.as_str() <= key);
            if pos == 0 {
                continue; // key precedes every file at this level
            }
            let meta = &files[pos - 1];
            if meta.largest_key.as_str() < key {
                continue; // key falls in a gap between files
            }

            let path = sst_path(&self.data_dir, meta.file_id);
            let Some(path_str) = path.to_str() else { continue };
            let mut reader = match SsTableReader::open(path_str) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[db] failed to open L{} SSTable {:?}: {}", level, path, e);
                    continue;
                }
            };
            match reader.search(key) {
                Ok(Some((_, Record::Put(v)))) => return Some(v),
                Ok(Some((_, Record::Delete))) => return None,
                Ok(None) => {}
                Err(e) => eprintln!("[db] error searching L{} SSTable {:?}: {}", level, path, e),
            }
        }

        None
    }

    /// Durably delete a key by writing a tombstone.
    pub fn delete(&self, key: String) -> io::Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);

        {
            let mut wal = self.active_wal.lock().unwrap();
            wal.append_delete(seq, &key)?;
        }

        // TODO: stall writes when flusher is falling behind

        if let Some(expected_id) = self.state.put(key, Record::Delete, seq) {
            self.rotate_wal_and_memtable(expected_id)?;
        }

        Ok(())
    }

    /// Claim a unique SSTable file ID. Called by the flusher before writing a new .sst file.
    pub fn alloc_sst_id(&self) -> u64 {
        self.next_sst_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Expose a read snapshot of the current version for the flusher and
    /// compaction worker. Callers get an Arc clone without holding the lock.
    pub fn current_version(&self) -> Arc<VersionState> {
        Arc::clone(&self.version.read().unwrap())
    }

    /// Called by the flusher after successfully writing an SSTable file.
    /// Persists the version edit to disk first, then atomically updates the
    /// in-memory VersionState via CoW.
    pub fn record_flush(
        &self,
        file_id: u64,
        smallest_key: String,
        largest_key: String,
    ) -> io::Result<()> {
        let edit = VersionEdit::AddFile { level: 0, file_id, smallest_key, largest_key };

        self.manifest.lock().unwrap().append(&edit)?;

        let mut guard = self.version.write().unwrap();
        let mut new_version = (**guard).clone();
        new_version.apply(&edit);
        *guard = Arc::new(new_version);

        Ok(())
    }

    /// Called by the compaction worker after merging files. Records all version
    /// edits in one logical batch: removals of inputs, additions of outputs.
    pub fn record_compaction(
        &self,
        removed: &[(u8, u64)],
        added: &[(u8, u64, String, String)],
    ) -> io::Result<()> {
        let mut manifest = self.manifest.lock().unwrap();

        for &(level, file_id) in removed {
            manifest.append(&VersionEdit::RemoveFile { level, file_id })?;
        }
        for (level, file_id, smallest_key, largest_key) in added {
            manifest.append(&VersionEdit::AddFile {
                level:        *level,
                file_id:      *file_id,
                smallest_key: smallest_key.clone(),
                largest_key:  largest_key.clone(),
            })?;
        }
        drop(manifest);

        let mut guard = self.version.write().unwrap();
        let mut new_version = (**guard).clone();
        for &(level, file_id) in removed {
            new_version.apply(&VersionEdit::RemoveFile { level, file_id });
        }
        for (level, file_id, smallest_key, largest_key) in added {
            new_version.apply(&VersionEdit::AddFile {
                level:        *level,
                file_id:      *file_id,
                smallest_key: smallest_key.clone(),
                largest_key:  largest_key.clone(),
            });
        }
        *guard = Arc::new(new_version);

        Ok(())
    }

    fn rotate_wal_and_memtable(&self, expected_id: u64) -> io::Result<()> {
        let mut wal_guard = self.active_wal.lock().unwrap();

        // 2. Identity-Based Double-Checked Locking!
        // If the active table's ID no longer matches the ID of the table we
        // filled up, it means another thread already rotated it. Abort safely!
        if self.state.active_id() != expected_id {
            return Ok(());
        }

        let new_wal_gen = self.next_wal_gen.fetch_add(1, Ordering::SeqCst);
        let new_wal = Wal::<Crc32Checksum>::create(&self.data_dir, new_wal_gen)?;

        self.state.freeze_active(new_wal_gen);
        *wal_guard = new_wal;

        self.flush_tx.send(()).ok();


        // 3. Create the new WAL file
        let new_wal = Wal::<Crc32Checksum>::create(&self.data_dir, new_wal_gen)?;

        // 4. Freeze the MemTable and assign it the EXACT SAME ID
        self.state.freeze_active(new_wal_gen);

        // 5. Swap the active WAL
        *wal_guard = new_wal;

        Ok(())
    }
}

fn highest_wal_gen(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let path = e.ok()?.path();
            if path.extension()?.to_str()? != "wal" {
                return None;
            }
            path.file_stem()?.to_str()?.parse::<u64>().ok()
        })
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::thread;

    fn tmp_dir() -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("copperdb_db_{}_{}", std::process::id(), id));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    // --- existing tests ---

    #[test]
    fn put_and_get() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();
        engine.put("foo".into(), b"bar".to_vec()).unwrap();
        assert_eq!(engine.get("foo"), Some(b"bar".to_vec()));
    }

    #[test]
    fn delete_returns_none() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();
        engine.put("foo".into(), b"bar".to_vec()).unwrap();
        engine.delete("foo".into()).unwrap();
        assert_eq!(engine.get("foo"), None);
    }

    #[test]
    fn get_missing_key_returns_none() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();
        assert_eq!(engine.get("absent"), None);
    }

    #[test]
    fn survives_restart() {
        let dir = tmp_dir();

        {
            let engine = LsmEngine::open(&dir).unwrap();
            engine.put("city".into(), b"london".to_vec()).unwrap();
            engine.put("temp".into(), b"cold".to_vec()).unwrap();
            engine.delete("temp".into()).unwrap();
        }

        let engine = LsmEngine::open(&dir).unwrap();
        assert_eq!(engine.get("city"), Some(b"london".to_vec()));
        assert_eq!(engine.get("temp"), None);
    }

    #[test]
    fn write_after_restart_uses_higher_seq() {
        let dir = tmp_dir();

        let pre_crash_seq;
        {
            let engine = LsmEngine::open(&dir).unwrap();
            engine.put("k".into(), b"v1".to_vec()).unwrap();
            pre_crash_seq = engine.next_seq.load(Ordering::SeqCst);
        }

        let engine = LsmEngine::open(&dir).unwrap();
        let post_crash_seq = engine.next_seq.load(Ordering::SeqCst);

        assert!(
            post_crash_seq >= pre_crash_seq,
            "seq regressed: {} < {}",
            post_crash_seq,
            pre_crash_seq
        );

        engine.put("k".into(), b"v2".to_vec()).unwrap();
        assert_eq!(engine.get("k"), Some(b"v2".to_vec()));
    }

    // --- get() ---

    #[test]
    fn get_returns_latest_value_after_overwrite() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();
        engine.put("k".into(), b"v1".to_vec()).unwrap();
        engine.put("k".into(), b"v2".to_vec()).unwrap();
        engine.put("k".into(), b"v3".to_vec()).unwrap();
        assert_eq!(engine.get("k"), Some(b"v3".to_vec()));
    }

    #[test]
    fn get_returns_none_after_delete_and_reput_returns_new_value() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();
        engine.put("k".into(), b"first".to_vec()).unwrap();
        engine.delete("k".into()).unwrap();
        assert_eq!(engine.get("k"), None, "tombstone should hide prior value");
        engine.put("k".into(), b"second".to_vec()).unwrap();
        assert_eq!(engine.get("k"), Some(b"second".to_vec()), "rewrite after delete");
    }

    #[test]
    fn get_multiple_distinct_keys_do_not_collide() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();
        for i in 0u32..50 {
            engine.put(format!("key_{:04}", i), vec![i as u8]).unwrap();
        }
        for i in 0u32..50 {
            assert_eq!(engine.get(&format!("key_{:04}", i)), Some(vec![i as u8]));
        }
    }

    // After an active memtable is frozen into an immutable, get() must still
    // find keys that lived only in the frozen table.
    #[test]
    fn get_finds_key_in_immutable_table_after_freeze() {
        // Use a tiny memtable so the first big write forces a rotation.
        let dir = tmp_dir();
        let engine = LsmEngine::open_with_memtable_size(&dir, 128).unwrap();
        engine.put("old".into(), b"value".to_vec()).unwrap();
        // This large write should push the active table over 128 bytes and
        // rotate it to the immutable queue.
        engine.put("filler".into(), vec![0u8; 256]).unwrap();
        // "old" is now in the immutable table; the active table holds "filler".
        assert_eq!(engine.get("old"), Some(b"value".to_vec()));
        assert!(engine.get("filler").is_some());
    }

    #[test]
    fn get_tombstone_in_active_masks_value_in_immutable() {
        let dir = tmp_dir();
        let engine = LsmEngine::open_with_memtable_size(&dir, 128).unwrap();
        engine.put("key".into(), b"alive".to_vec()).unwrap();
        // Force a freeze so "key"="alive" lands in an immutable table.
        engine.put("filler".into(), vec![0u8; 256]).unwrap();
        // Write a tombstone for "key" into the new active table.
        engine.delete("key".into()).unwrap();
        // The tombstone in the active table must shadow the value in immutable.
        assert_eq!(engine.get("key"), None);
    }

    // --- record_compaction() ---

    #[test]
    fn record_compaction_removes_inputs_and_registers_outputs() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();

        // Seed two L0 files.
        engine.record_compaction(
            &[],
            &[
                (0, 1, "a".to_string(), "c".to_string()),
                (0, 2, "d".to_string(), "f".to_string()),
            ],
        ).unwrap();
        assert_eq!(engine.current_version().files_at_level(0).len(), 2);

        // Compact L0 → L1.
        engine.record_compaction(
            &[(0, 1), (0, 2)],
            &[(1, 3, "a".to_string(), "f".to_string())],
        ).unwrap();

        let v = engine.current_version();
        assert_eq!(v.files_at_level(0).len(), 0, "L0 inputs must be removed");
        assert_eq!(v.files_at_level(1).len(), 1, "L1 output must be registered");
        assert_eq!(v.files_at_level(1)[0].file_id, 3);
        assert_eq!(v.files_at_level(1)[0].smallest_key, "a");
        assert_eq!(v.files_at_level(1)[0].largest_key, "f");
    }

    #[test]
    fn record_compaction_remove_only_leaves_version_empty() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();

        engine.record_compaction(&[], &[(0, 7, "x".to_string(), "z".to_string())]).unwrap();
        assert_eq!(engine.current_version().files_at_level(0).len(), 1);

        engine.record_compaction(&[(0, 7)], &[]).unwrap();
        assert_eq!(engine.current_version().files_at_level(0).len(), 0);
    }

    #[test]
    fn record_compaction_add_only_places_files_at_correct_level() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();

        engine.record_compaction(
            &[],
            &[
                (1, 10, "apple".to_string(), "mango".to_string()),
                (1, 11, "orange".to_string(), "zebra".to_string()),
            ],
        ).unwrap();

        let v = engine.current_version();
        assert_eq!(v.files_at_level(1).len(), 2);
        // L1 files are kept sorted by smallest_key.
        assert_eq!(v.files_at_level(1)[0].file_id, 10);
        assert_eq!(v.files_at_level(1)[1].file_id, 11);
    }

    #[test]
    fn record_compaction_empty_call_is_noop() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();
        engine.record_compaction(&[], &[]).unwrap();
        let v = engine.current_version();
        for level in 0..7 {
            assert!(v.files_at_level(level).is_empty(), "level {} should be empty", level);
        }
    }

    #[test]
    fn record_compaction_removes_nonexistent_file_is_safe() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();
        // Removing a file that was never registered must not panic or corrupt state.
        engine.record_compaction(&[(0, 999)], &[]).unwrap();
        assert!(engine.current_version().files_at_level(0).is_empty());
    }

    #[test]
    fn record_compaction_spans_multiple_levels() {
        let dir = tmp_dir();
        let engine = LsmEngine::open(&dir).unwrap();

        engine.record_compaction(
            &[],
            &[
                (0, 1, "a".to_string(), "b".to_string()),
                (1, 2, "c".to_string(), "d".to_string()),
                (2, 3, "e".to_string(), "f".to_string()),
            ],
        ).unwrap();

        // Compact L1 file into L2.
        engine.record_compaction(
            &[(1, 2)],
            &[(2, 4, "c".to_string(), "f".to_string())],
        ).unwrap();

        let v = engine.current_version();
        assert_eq!(v.files_at_level(0).len(), 1, "L0 untouched");
        assert_eq!(v.files_at_level(1).len(), 0, "L1 input removed");
        assert_eq!(v.files_at_level(2).len(), 2, "original L2 + new output");
    }

    // --- concurrency ---

    // get() must never panic or return garbage while record_compaction() is
    // concurrently replacing the version state.
    #[test]
    fn concurrent_get_and_record_compaction_no_panic() {
        let dir = tmp_dir();
        let engine = Arc::new(LsmEngine::open(&dir).unwrap());

        // Pre-populate the memtable so get() has real data to return.
        for i in 0u32..100 {
            engine.put(format!("key_{:04}", i), vec![i as u8]).unwrap();
        }

        // Seed an initial file so the compaction thread has something to remove.
        engine.record_compaction(
            &[],
            &[(0, 1000, "key_0000".to_string(), "key_0099".to_string())],
        ).unwrap();

        let e1 = Arc::clone(&engine);
        let compactor = thread::spawn(move || {
            for id in 0u64..200 {
                // Alternate between adding and then removing the same file.
                e1.record_compaction(
                    &[(0, 1000)],
                    &[(1, 2000 + id, "key_0000".to_string(), "key_0099".to_string())],
                ).ok();
                e1.record_compaction(
                    &[(1, 2000 + id)],
                    &[(0, 1000, "key_0000".to_string(), "key_0099".to_string())],
                ).ok();
            }
        });

        let e2 = Arc::clone(&engine);
        let reader = thread::spawn(move || {
            for _ in 0..1000 {
                for i in 0u32..10 {
                    let _ = e2.get(&format!("key_{:04}", i));
                }
            }
        });

        compactor.join().unwrap();
        reader.join().unwrap();

        // Memtable data must remain intact.
        for i in 0u32..100 {
            assert_eq!(engine.get(&format!("key_{:04}", i)), Some(vec![i as u8]));
        }
    }

    // Concurrent puts, gets, and record_compaction calls must not deadlock or
    // corrupt the version state.
    #[test]
    fn concurrent_put_get_and_record_compaction() {
        let dir = tmp_dir();
        let engine = Arc::new(LsmEngine::open(&dir).unwrap());
        let mut handles = vec![];

        // Writer threads.
        for t in 0u32..4 {
            let e = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for i in 0u32..100 {
                    e.put(format!("t{}k{:04}", t, i), vec![(t + i) as u8]).unwrap();
                }
            }));
        }

        // Reader threads.
        for _ in 0..4 {
            let e = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for _ in 0..500 {
                    let _ = e.get("t0k0000");
                    let _ = e.get("nonexistent");
                }
            }));
        }

        // Version-mutation threads (simulating compaction results arriving).
        for base in 0u64..4 {
            let e = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for offset in 0u64..20 {
                    let file_id = base * 100 + offset;
                    e.record_compaction(
                        &[],
                        &[(
                            1,
                            file_id,
                            format!("k{:04}", offset),
                            format!("k{:04}", offset + 9),
                        )],
                    ).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Writes made by the writer threads must all be readable.
        for t in 0u32..4 {
            for i in 0u32..100 {
                assert_eq!(
                    engine.get(&format!("t{}k{:04}", t, i)),
                    Some(vec![(t + i) as u8]),
                );
            }
        }
    }
}
