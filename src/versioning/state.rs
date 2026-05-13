use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use super::{SstFileGuard, SstableMetadata, VersionEdit, NUM_LEVELS};

// ---------------------------------------------------------------------------
// VersionState — in-memory snapshot of all live SSTable files
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct VersionState {
    levels: Vec<Vec<SstableMetadata>>,
    /// Directory holding all SSTable files. Stored so `apply` can mint an
    /// `SstFileGuard` for each new file without the caller having to thread
    /// the path through every edit.
    data_dir: PathBuf,
}

impl VersionState {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            levels: vec![Vec::new(); NUM_LEVELS],
            data_dir,
        }
    }

    pub fn apply(&mut self, edit: &VersionEdit) {
        match edit {
            VersionEdit::AddFile { level, file_id, smallest_key, largest_key, max_seq } => {
                let level = *level as usize;
                if level >= self.levels.len() {
                    self.levels.resize(level + 1, Vec::new());
                }
                let meta = SstableMetadata {
                    file_id:      *file_id,
                    level:        level as u8,
                    smallest_key: smallest_key.clone(),
                    largest_key:  largest_key.clone(),
                    max_seq:      *max_seq,
                    file:         Arc::new(SstFileGuard::new(*file_id, &self.data_dir)),
                };
                if level == 0 {
                    // L0 files may have overlapping key ranges; preserve flush
                    // order so reads can scan newest-first by iterating in reverse.
                    self.levels[0].push(meta);
                } else {
                    // L1+ files must have non-overlapping, globally sorted key
                    // ranges. Insert at the sorted position so binary search over
                    // the level is always valid — both during replay and after
                    // each compaction that adds new files.
                    let pos = self.levels[level]
                        .partition_point(|m| m.smallest_key < *smallest_key);
                    self.levels[level].insert(pos, meta);
                }
            }
            VersionEdit::RemoveFile { level, file_id } => {
                let level = *level as usize;
                if level < self.levels.len() {
                    // Mark the guard before retain. The mark propagates to all
                    // clones of this `Arc<SstFileGuard>` (including the ones
                    // held by older snapshots), so once the last reference
                    // drops, `Drop` unlinks the file.
                    if let Some(meta) =
                        self.levels[level].iter().find(|m| m.file_id == *file_id)
                    {
                        meta.file.mark_for_deletion();
                    }
                    self.levels[level].retain(|m| m.file_id != *file_id);
                }
            }
        }
    }

    pub fn files_at_level(&self, level: usize) -> &[SstableMetadata] {
        self.levels.get(level).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Returns all file IDs across every level. Used at startup to compute
    /// the next SSTable ID: `all_file_ids().max().unwrap_or(0) + 1`.
    pub fn all_file_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.levels.iter().flatten().map(|m| m.file_id)
    }

    /// Highest seq_num across every live SSTable. Used at startup to initialise
    /// `LsmEngine::next_seq` so new writes never collide with seqs already on
    /// disk; if they did, compaction's highest-seq-wins dedup could drop the
    /// new writes in favour of stale records.
    pub fn max_seq_num(&self) -> u64 {
        self.levels
            .iter()
            .flatten()
            .map(|m| m.max_seq)
            .max()
            .unwrap_or(0)
    }

    /// Returns metadata for every file at `level` whose key range overlaps
    /// `[lo, hi]`. Used by the compaction worker to find merge inputs.
    pub fn overlapping_files<'a>(
        &'a self,
        level: usize,
        lo: &str,
        hi: &str,
    ) -> Vec<&'a SstableMetadata> {
        self.files_at_level(level)
            .iter()
            .filter(|m| m.smallest_key <= hi.to_string() && m.largest_key >= lo.to_string())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// SharedVersion — the CoW wrapper used by LsmEngine
// ---------------------------------------------------------------------------

/// Thread-safe CoW container for the current `VersionState`.
///
/// Readers clone the inner `Arc` under a brief read-lock; writers clone the
/// whole `VersionState`, mutate the copy, and swap the `Arc` under a
/// write-lock — giving readers an atomic, wait-free view.
pub struct SharedVersion(RwLock<Arc<VersionState>>);

impl SharedVersion {
    /// Create a `SharedVersion` pre-loaded with a replayed `VersionState`.
    /// Used during engine recovery to restore the manifest state from disk.
    pub fn from_state(state: VersionState) -> Self {
        Self(RwLock::new(Arc::new(state)))
    }

    /// Returns a point-in-time snapshot. Callers can read freely without
    /// holding any lock.
    pub fn snapshot(&self) -> Arc<VersionState> {
        Arc::clone(&self.0.read().unwrap())
    }

    /// Atomically apply one or more edits to the version state.
    pub fn apply(&self, edits: &[VersionEdit]) {
        let mut guard = self.0.write().unwrap();
        let mut next = (**guard).clone();
        for edit in edits {
            next.apply(edit);
        }
        *guard = Arc::new(next);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::versioning::sst_path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn tmp_dir() -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir()
            .join(format!("copperdb_state_{}_{}", std::process::id(), id));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn add(file_id: u64, level: u8, lo: &str, hi: &str) -> VersionEdit {
        VersionEdit::AddFile {
            level,
            file_id,
            smallest_key: lo.to_string(),
            largest_key:  hi.to_string(),
            max_seq:      0,
        }
    }

    #[test]
    fn overlapping_files_correct_range() {
        let mut state = VersionState::new(tmp_dir());
        state.apply(&add(1, 1, "apple",  "cherry"));  // [apple,  cherry]
        state.apply(&add(2, 1, "date",   "grape"));   // [date,   grape]
        state.apply(&add(3, 1, "mango",  "peach"));   // [mango,  peach]

        // Query [fig, nectarine] overlaps "date-grape" (grape >= fig) and "mango-peach" (mango <= nectarine)
        // but not "apple-cherry" (cherry < fig)
        let hits = state.overlapping_files(1, "fig", "nectarine");
        let ids: Vec<u64> = hits.iter().map(|m| m.file_id).collect();
        assert!(ids.contains(&2), "date-grape should overlap [fig, nectarine]");
        assert!(ids.contains(&3), "mango-peach should overlap [fig, nectarine]");
        assert!(!ids.contains(&1), "apple-cherry should not overlap [fig, nectarine]");
    }

    #[test]
    fn sst_path_format() {
        use std::path::Path;
        let p = sst_path(Path::new("/data"), 42);
        assert_eq!(p.to_str().unwrap(), "/data/00000000000000000042.sst");
    }

    // L1+ files added in reverse key order must come back sorted by smallest_key.
    // Without the sorted-insert fix, files_at_level(1) would return [mango, date, apple].
    #[test]
    fn l1_files_sorted_after_out_of_order_apply() {
        let mut state = VersionState::new(tmp_dir());
        state.apply(&add(3, 1, "mango", "peach"));
        state.apply(&add(1, 1, "apple", "cherry"));
        state.apply(&add(2, 1, "date",  "grape"));

        let files = state.files_at_level(1);
        let keys: Vec<&str> = files.iter().map(|m| m.smallest_key.as_str()).collect();
        assert_eq!(keys, vec!["apple", "date", "mango"]);
    }

    // L0 must preserve flush order (newest file last, so iterating in reverse
    // visits newest-first), not sort by key.
    #[test]
    fn l0_files_preserve_insertion_order() {
        let mut state = VersionState::new(tmp_dir());
        state.apply(&add(1, 0, "mango", "peach"));
        state.apply(&add(2, 0, "apple", "cherry"));
        state.apply(&add(3, 0, "date",  "grape"));

        let ids: Vec<u64> = state.files_at_level(0).iter().map(|m| m.file_id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    // -----------------------------------------------------------------------
    // Concurrency tests
    //
    // These tests drive the CoW pattern used by LsmEngine:
    //   RwLock<Arc<VersionState>>
    //   write: clone current Arc → mutate copy → swap
    //   read:  clone the Arc and release the lock immediately
    // -----------------------------------------------------------------------

    // N threads each flush one file to L0 concurrently. Because every writer
    // holds the write lock for the duration of its CoW swap, no update can be
    // lost — the last writer always starts from the version left by the
    // second-to-last writer.
    #[test]
    fn concurrent_cow_flushes_all_files_visible() {
        use std::sync::{Arc, RwLock};
        use std::thread;

        let version: Arc<RwLock<Arc<VersionState>>> =
            Arc::new(RwLock::new(Arc::new(VersionState::new(tmp_dir()))));

        const N_THREADS: usize = 8;
        const FILES_PER_THREAD: usize = 10;

        let handles: Vec<_> = (0..N_THREADS)
            .map(|t| {
                let version = Arc::clone(&version);
                thread::spawn(move || {
                    for i in 0..FILES_PER_THREAD {
                        let file_id = (t * FILES_PER_THREAD + i) as u64;
                        let key = format!("{:08}", file_id);
                        let edit = VersionEdit::AddFile {
                            level: 0,
                            file_id,
                            smallest_key: key.clone(),
                            largest_key: key,
                            max_seq: 0,
                        };
                        let mut guard = version.write().unwrap();
                        let mut new_v = (**guard).clone();
                        new_v.apply(&edit);
                        *guard = Arc::new(new_v);
                    }
                })
            })
            .collect();

        for h in handles { h.join().unwrap(); }

        let final_v = Arc::clone(&*version.read().unwrap());
        assert_eq!(final_v.files_at_level(0).len(), N_THREADS * FILES_PER_THREAD);
    }

    // An Arc snapshot captured before a wave of concurrent flushes must still
    // reflect the pre-flush state after those flushes complete. The live version
    // must reflect all of the new files.
    #[test]
    fn snapshot_stable_during_concurrent_flushes() {
        use std::sync::{Arc, RwLock};
        use std::thread;

        let version: Arc<RwLock<Arc<VersionState>>> =
            Arc::new(RwLock::new(Arc::new(VersionState::new(tmp_dir()))));

        // Seed one file before taking the snapshot.
        {
            let mut guard = version.write().unwrap();
            let mut v = (**guard).clone();
            v.apply(&add(0, 0, "seed", "seed"));
            *guard = Arc::new(v);
        }

        let snapshot = Arc::clone(&*version.read().unwrap());
        assert_eq!(snapshot.files_at_level(0).len(), 1);

        const N: usize = 20;
        let handles: Vec<_> = (1..=N)
            .map(|i| {
                let version = Arc::clone(&version);
                thread::spawn(move || {
                    let key = format!("{:08}", i);
                    let edit = VersionEdit::AddFile {
                        level: 0,
                        file_id: i as u64,
                        smallest_key: key.clone(),
                        largest_key: key,
                        max_seq: 0,
                    };
                    let mut guard = version.write().unwrap();
                    let mut new_v = (**guard).clone();
                    new_v.apply(&edit);
                    *guard = Arc::new(new_v);
                })
            })
            .collect();

        for h in handles { h.join().unwrap(); }

        // The snapshot is frozen — it must still show only the seed file.
        assert_eq!(snapshot.files_at_level(0).len(), 1);

        // The live version must show every file.
        let live = Arc::clone(&*version.read().unwrap());
        assert_eq!(live.files_at_level(0).len(), N + 1);
    }

    // N threads concurrently add non-overlapping L1 files in an arbitrary order.
    // Because each writer starts its CoW from the version left by the previous
    // writer, and apply() inserts at the sorted position, the final level must
    // be in ascending smallest_key order regardless of which thread won each
    // write-lock acquisition.
    #[test]
    fn l1_sorted_invariant_preserved_under_concurrent_cow_adds() {
        use std::sync::{Arc, RwLock};
        use std::thread;

        let version: Arc<RwLock<Arc<VersionState>>> =
            Arc::new(RwLock::new(Arc::new(VersionState::new(tmp_dir()))));

        // Each tuple is (file_id, key). Deliberately not in key order.
        let files: &[(u64, &str)] = &[
            (5, "echo"),     (1, "apple"),  (3, "cherry"),   (7, "grape"),
            (2, "banana"),   (6, "fig"),    (4, "date"),     (8, "honeydew"),
        ];

        let handles: Vec<_> = files
            .iter()
            .map(|&(file_id, key)| {
                let version = Arc::clone(&version);
                let key = key.to_string();
                thread::spawn(move || {
                    let edit = VersionEdit::AddFile {
                        level: 1,
                        file_id,
                        smallest_key: key.clone(),
                        largest_key: key,
                        max_seq: 0,
                    };
                    let mut guard = version.write().unwrap();
                    let mut new_v = (**guard).clone();
                    new_v.apply(&edit);
                    *guard = Arc::new(new_v);
                })
            })
            .collect();

        for h in handles { h.join().unwrap(); }

        let final_v = Arc::clone(&*version.read().unwrap());
        assert_eq!(final_v.files_at_level(1).len(), files.len());

        let keys: Vec<&str> = final_v
            .files_at_level(1)
            .iter()
            .map(|m| m.smallest_key.as_str())
            .collect();
        assert_eq!(keys, vec!["apple", "banana", "cherry", "date", "echo", "fig", "grape", "honeydew"]);
    }

    // Readers must never observe a version mid-apply. The CoW swap (replacing
    // the Arc under the write lock) is atomic from the reader's perspective:
    // a reader either gets the version from before an apply or the one after,
    // never a partial state where only some files of a batch have been inserted.
    //
    // Each writer applies a two-file compaction batch (one remove + one add)
    // atomically. A reader that sees the pre-compaction file count must not
    // see the post-compaction add without also seeing the remove, and vice versa.
    #[test]
    fn readers_never_observe_partial_compaction() {
        use std::sync::{Arc, Barrier, RwLock};
        use std::thread;

        let version: Arc<RwLock<Arc<VersionState>>> =
            Arc::new(RwLock::new(Arc::new(VersionState::new(tmp_dir()))));

        // Seed two L1 files that compaction will replace.
        {
            let mut guard = version.write().unwrap();
            let mut v = (**guard).clone();
            v.apply(&add(1, 1, "apple", "fig"));
            v.apply(&add(2, 1, "grape", "mango"));
            *guard = Arc::new(v);
        }

        const N_READERS: usize = 6;
        let barrier = Arc::new(Barrier::new(N_READERS + 1));
        let any_partial = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let reader_handles: Vec<_> = (0..N_READERS)
            .map(|_| {
                let version = Arc::clone(&version);
                let barrier = Arc::clone(&barrier);
                let any_partial = Arc::clone(&any_partial);
                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..200 {
                        let v = Arc::clone(&*version.read().unwrap());
                        let count = v.files_at_level(1).len();
                        // Valid states: 2 (pre-compaction) or 1 (post-compaction).
                        // A partial apply would produce 0 (both removed, none added)
                        // or 3 (add visible before remove).
                        if count != 1 && count != 2 {
                            any_partial.store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();

        // One writer performs the compaction: atomically replace files 1+2 with file 3.
        let writer = {
            let version = Arc::clone(&version);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..50 {
                    // Forward: remove 1+2, add 3.
                    {
                        let mut guard = version.write().unwrap();
                        let mut new_v = (**guard).clone();
                        new_v.apply(&VersionEdit::RemoveFile { level: 1, file_id: 1 });
                        new_v.apply(&VersionEdit::RemoveFile { level: 1, file_id: 2 });
                        new_v.apply(&add(3, 1, "apple", "mango"));
                        *guard = Arc::new(new_v);
                    }
                    // Reverse: remove 3, restore 1+2.
                    {
                        let mut guard = version.write().unwrap();
                        let mut new_v = (**guard).clone();
                        new_v.apply(&VersionEdit::RemoveFile { level: 1, file_id: 3 });
                        new_v.apply(&add(1, 1, "apple", "fig"));
                        new_v.apply(&add(2, 1, "grape", "mango"));
                        *guard = Arc::new(new_v);
                    }
                }
            })
        };

        writer.join().unwrap();
        for h in reader_handles { h.join().unwrap(); }

        assert!(
            !any_partial.load(std::sync::atomic::Ordering::Relaxed),
            "A reader observed a partial compaction state"
        );
    }
}
