use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use super::{sst_path, FileId};

/// Refcounted handle to an on-disk SSTable file.
///
/// Every `SstableMetadata` carries an `Arc<SstFileGuard>`. Cloning a
/// `VersionState` (which `SharedVersion::apply` does on every CoW swap)
/// clones the `Arc`, so a reader holding a `current_version()` snapshot pins
/// every file it could read until the snapshot is dropped — even after
/// compaction has swapped those files out of the live version.
///
/// ## Unlink semantics
///
/// `Drop` only unlinks when `should_unlink` is set. The flag is flipped by
/// `VersionState::apply(RemoveFile)` immediately before the metadata is
/// `retain`'d out of its level. Engine shutdown drops the live `VersionState`
/// without ever flipping that flag, so files referenced by the live version at
/// shutdown stay on disk and are recovered by the next session's manifest
/// replay.
///
/// Note that compaction's old `RemoveFile` edits propagate the mark to *every*
/// clone of the same `Arc<SstFileGuard>` — including clones held by old
/// snapshots — but the actual `remove_file` syscall is deferred until the
/// last `Arc` drops. So a long-running reader keeps the file alive through
/// any number of compactions; the file goes away only when no one needs it.
pub struct SstFileGuard {
    pub file_id: FileId,
    pub path:    PathBuf,
    should_unlink: AtomicBool,
}

impl SstFileGuard {
    pub(crate) fn new(file_id: FileId, dir: &Path) -> Self {
        Self {
            file_id,
            path: sst_path(dir, file_id),
            should_unlink: AtomicBool::new(false),
        }
    }

    /// Mark this file for unlink-on-last-drop. Called from
    /// `VersionState::apply(RemoveFile)` so every clone of the guard observes
    /// that the file is no longer part of any live version.
    pub(crate) fn mark_for_deletion(&self) {
        self.should_unlink.store(true, Ordering::SeqCst);
    }
}

impl Drop for SstFileGuard {
    fn drop(&mut self) {
        if !self.should_unlink.load(Ordering::SeqCst) {
            return;
        }
        match fs::remove_file(&self.path) {
            Ok(()) => {}
            // The file may already be gone — e.g. a previous session's
            // compactor unlinked it before crashing, and replay re-minted then
            // dropped a marked guard for the same file_id. Treat as benign.
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => eprintln!("[sst] unlink {:?} failed: {}", self.path, e),
        }
    }
}

impl std::fmt::Debug for SstFileGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstFileGuard")
            .field("file_id", &self.file_id)
            .finish()
    }
}

/// Two guards are equal iff they refer to the same file id. This lets
/// `SstableMetadata` keep its derived `PartialEq` without comparing pointer
/// identity of the `Arc`.
impl PartialEq for SstFileGuard {
    fn eq(&self, other: &Self) -> bool {
        self.file_id == other.file_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    fn tmp_dir() -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir()
            .join(format!("copperdb_guard_{}_{}", std::process::id(), id));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// An unmarked guard must leave its file on disk when dropped. This is
    /// what protects files referenced by the live `VersionState` from being
    /// destroyed at engine shutdown.
    #[test]
    fn unmarked_guard_does_not_unlink_on_drop() {
        let dir = tmp_dir();
        let file_id: FileId = 1;
        let path = sst_path(&dir, file_id);

        std::fs::write(&path, b"sstable bytes").unwrap();
        assert!(path.exists(), "test setup: file should exist before drop");

        {
            let _guard = SstFileGuard::new(file_id, &dir);
            // Default `should_unlink` is false; no `mark_for_deletion`.
        }

        assert!(
            path.exists(),
            "an unmarked guard must not unlink its underlying file on drop",
        );
    }

    /// A guard whose `should_unlink` flag has been set must unlink its file
    /// when the last `Arc` to it drops.
    #[test]
    fn marked_guard_unlinks_on_drop() {
        let dir = tmp_dir();
        let file_id: FileId = 2;
        let path = sst_path(&dir, file_id);

        std::fs::write(&path, b"sstable bytes").unwrap();
        assert!(path.exists(), "test setup: file should exist before drop");

        {
            let guard = SstFileGuard::new(file_id, &dir);
            guard.mark_for_deletion();
        }

        assert!(
            !path.exists(),
            "a marked guard must unlink its underlying file on drop",
        );
    }

    /// `mark_for_deletion` is called on an `&SstFileGuard` reached via one
    /// Arc clone; the inner `AtomicBool` is shared, so any other Arc clone
    /// observes the mark when its turn to drop comes. This is the contract
    /// that lets `apply(RemoveFile)` mark a guard while older snapshots are
    /// still pinning the file: the unlink is deferred until the last
    /// snapshot releases its clone, at which point Drop sees the mark and
    /// unlinks.
    #[test]
    fn mark_propagates_across_arc_clones_and_unlinks_on_last_drop() {
        let dir = tmp_dir();
        let file_id: FileId = 3;
        let path = sst_path(&dir, file_id);

        std::fs::write(&path, b"sstable bytes").unwrap();

        let g1 = Arc::new(SstFileGuard::new(file_id, &dir));
        let g2 = Arc::clone(&g1);

        // Mark via g1; g2 must observe the same mark.
        g1.mark_for_deletion();

        // Dropping g1 must NOT unlink — g2 still pins the guard.
        drop(g1);
        assert!(
            path.exists(),
            "file should still exist while another Arc clone is alive",
        );

        // Dropping the last reference fires Drop, which sees the mark and unlinks.
        drop(g2);
        assert!(
            !path.exists(),
            "file should be unlinked once the last marked Arc clone drops",
        );
    }
}
