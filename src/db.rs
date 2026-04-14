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

        if self.state.active_id() != expected_id {
            return Ok(());
        }

        let new_wal_gen = self.next_wal_gen.fetch_add(1, Ordering::SeqCst);
        let new_wal = Wal::<Crc32Checksum>::create(&self.data_dir, new_wal_gen)?;

        self.state.freeze_active(new_wal_gen);
        *wal_guard = new_wal;

        self.flush_tx.send(()).ok();

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

    fn tmp_dir() -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir()
            .join(format!("copperdb_db_{}_{}", std::process::id(), id));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

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
}
