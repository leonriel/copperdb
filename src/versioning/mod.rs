use std::path::{Path, PathBuf};
use std::sync::Arc;

mod guard;
mod manifest;
mod state;

pub use guard::SstFileGuard;
pub use manifest::Manifest;
pub use state::{SharedVersion, VersionState};

type FileId = u64;
type Level = u8;

pub(crate) const NUM_LEVELS: usize = 7;

pub fn sst_path(dir: &Path, file_id: u64) -> PathBuf {
    dir.join(format!("{:020}.sst", file_id))
}

#[derive(Clone, Debug, PartialEq)]
pub struct SstableMetadata {
    pub file_id:      FileId,
    pub level:        Level,
    pub smallest_key: String,
    pub largest_key:  String,
    /// Highest seq_num of any entry in this SSTable. Persisted so the engine
    /// can restore `next_seq` correctly across restarts — otherwise new writes
    /// would get seq numbers lower than records already on disk, and the
    /// highest-seq-wins compaction dedup would drop newer writes in favour of
    /// older SSTable records.
    pub max_seq:      u64,
    /// Refcounted handle that keeps the on-disk file alive for as long as any
    /// snapshot references this metadata. Cloning `SstableMetadata` clones the
    /// `Arc`, so every snapshot returned by `SharedVersion::snapshot()`
    /// automatically pins its files.
    pub file:         Arc<SstFileGuard>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum VersionEdit {
    AddFile {
        level:        Level,
        file_id:      FileId,
        smallest_key: String,
        largest_key:  String,
        max_seq:      u64,
    },
    RemoveFile {
        level:   Level,
        file_id: FileId,
    },
}
