use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::core::Record;
use crate::memtable::state::MemTableState;
use crate::wal::{Crc32Checksum, Wal, WalOpType, recover_all};

const MAX_MEMTABLE_SIZE: usize = 64 * 1024 * 1024;
const MAX_IMMUTABLE_TABLES: usize = 4;

/// Core engine: coordinates WAL writes and MemTable inserts.
///
/// Write path: WAL append → MemTable insert → rotate if frozen.
/// Read path:  MemTableState (active first, then immutables in reverse order).
/// Recovery:   replay all unflushed WAL files on `open`.
pub struct LsmEngine {
    state: MemTableState,
    active_wal: Mutex<Wal<Crc32Checksum>>,
    next_seq: AtomicU64,
    next_wal_gen: AtomicU64,
    data_dir: PathBuf,
}

impl LsmEngine {
    /// Open (or create) an engine whose data lives in `dir`.
    ///
    /// Any WAL files found in `dir` that have not yet been deleted are replayed
    /// to restore pre-crash MemTable state.
    pub fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;

        let records = recover_all::<Crc32Checksum>(dir)?;

        // Continue numbering WAL generations from the highest seen so far.
        let next_gen = highest_wal_gen(dir) + 1;
        let max_seq = records.iter().map(|r| r.seq_num).max().unwrap_or(0);

        // Explicitly tie the starting MemTable to the starting WAL generation
        let state = MemTableState::new(MAX_IMMUTABLE_TABLES, MAX_MEMTABLE_SIZE, next_gen);

        for r in records {
            let record = match r.op {
                WalOpType::Put => Record::Put(r.value),
                WalOpType::Delete => Record::Delete,
            };
            // Ignore capacity limits during replay. All old WALs get merged 
            // into this single starting MemTable.
            state.put(r.key, record, r.seq_num);
        }

        // Create the active WAL file for new user writes
        let active_wal = Wal::<Crc32Checksum>::create(dir, next_gen)?;

        Ok(Self {
            state,
            active_wal: Mutex::new(active_wal),
            next_seq: AtomicU64::new(max_seq + 1),
            next_wal_gen: AtomicU64::new(next_gen + 1),
            data_dir: dir.to_path_buf(),
        })
    }

    /// Durably write a key-value pair.
    ///
    /// The WAL record is flushed to the OS page cache before the MemTable is
    /// touched. A crash between the two steps is safe to replay on the next
    /// `open`. For a stronger guarantee (persist through power loss), call
    /// `wal.sync()` — but that requires an fsync per write.
    pub fn put(&self, key: String, value: Vec<u8>) -> io::Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);

        {
            let mut wal = self.active_wal.lock().unwrap();
            wal.append_put(seq, &key, &value)?;
        }

        // put() returns the ID of the table if it needs freezing
        if let Some(expected_id) = self.state.put(key, Record::Put(value), seq) {
            self.rotate_wal_and_memtable(expected_id)?;
        }

        Ok(())
    }

    /// Look up a key. Returns `None` if the key was deleted or never written.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        match self.state.get(key)? {
            (Record::Put(v), _) => Some(v),
            (Record::Delete, _) => None,
        }
    }

    /// Durably delete a key by writing a tombstone.
    pub fn delete(&self, key: String) -> io::Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);

        {
            let mut wal = self.active_wal.lock().unwrap();
            wal.append_delete(seq, &key)?;
        }

        if let Some(expected_id) = self.state.put(key, Record::Delete, seq) {
            self.rotate_wal_and_memtable(expected_id)?;
        }

        Ok(())
    }

    /// Safely coordinates rotating BOTH the WAL file and the MemTable together.
    /// Expects the ID of the MemTable that triggered the rotation request.
    fn rotate_wal_and_memtable(&self, expected_id: u64) -> io::Result<()> {
        // 1. Lock the WAL. This acts as our synchronization barrier.
        let mut wal_guard = self.active_wal.lock().unwrap();
        
        // 2. Identity-Based Double-Checked Locking!
        // If the active table's ID no longer matches the ID of the table we 
        // filled up, it means another thread already rotated it. Abort safely!
        if self.state.active_id() != expected_id {
            return Ok(());
        }

        let new_wal_gen = self.next_wal_gen.fetch_add(1, Ordering::SeqCst);
        
        // 3. Create the new WAL file
        let new_wal = Wal::<Crc32Checksum>::create(&self.data_dir, new_wal_gen)?;
        
        // 4. Freeze the MemTable and assign it the EXACT SAME ID
        self.state.freeze_active(new_wal_gen);
        
        // 5. Swap the active WAL
        *wal_guard = new_wal;
        
        Ok(())
    }
}

/// Returns the highest generation number found among `*.wal` files in `dir`,
/// or 0 if the directory is empty / contains no WAL files.
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

        // Reopen — WAL replay should restore state.
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

        // New seq must continue from where the previous run left off.
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
