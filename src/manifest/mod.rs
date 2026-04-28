use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::mem::size_of;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(thiserror::Error, Debug)]
pub enum ManifestError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    /// A record with a structurally valid CRC had an unrecognised edit type.
    /// This is distinct from a torn-write (CRC mismatch at the tail) and
    /// indicates genuine file corruption or a version mismatch.
    #[error("Corrupt manifest: unknown edit type 0x{0:02x} at byte offset {1}")]
    UnknownEditType(u8, u64),
}

use crate::wal::Crc32Checksum;
use crate::wal::Checksum;

pub type FileId = u64;
pub type Level  = u8;

const FILE_ID_SIZE:    usize = size_of::<FileId>();
const LEVEL_SIZE:      usize = size_of::<Level>();
const CRC_SIZE:        usize = size_of::<u32>();
const KEY_LEN_SIZE:    usize = size_of::<u32>();
const EDIT_TYPE_SIZE:  usize = size_of::<u8>(); // EditType is #[repr(u8)]

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum EditType {
    AddFile    = 0x01,
    RemoveFile = 0x02,
}

impl TryFrom<u8> for EditType {
    type Error = ();
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(EditType::AddFile),
            0x02 => Ok(EditType::RemoveFile),
            _    => Err(()),
        }
    }
}

const NUM_LEVELS: usize = 7;
const MANIFEST_FILENAME: &str = "MANIFEST";

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join(MANIFEST_FILENAME)
}

pub fn sst_path(dir: &Path, file_id: u64) -> PathBuf {
    dir.join(format!("{:020}.sst", file_id))
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Manifest — append-only on-disk log of VersionEdits
// ---------------------------------------------------------------------------

pub struct Manifest {
    writer: BufWriter<File>,
}

impl Manifest {
    /// Open (or create) the MANIFEST file in `dir`.
    ///
    /// If the file already exists every intact record is replayed into a fresh
    /// `VersionState`. Replay stops at the first CRC mismatch — matching the
    /// WAL crash-recovery semantics in `wal.rs:replay`.
    ///
    /// After replay, any `.sst` file in `dir` not referenced by the final
    /// version state is unlinked. These are orphans from a previous session
    /// that wrote an SSTable to disk but crashed before committing the
    /// `AddFile` edit to the manifest.
    pub fn open_or_create(dir: &Path) -> Result<(Self, VersionState), ManifestError> {
        let path = manifest_path(dir);

        let version = if path.exists() {
            replay(&path, dir)?
        } else {
            VersionState::new(dir.to_path_buf())
        };

        sweep_orphans(dir, &version)?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok((Manifest { writer: BufWriter::new(file) }, version))
    }

    /// Append one version edit. The CRC32 is computed over the payload bytes
    /// that follow the checksum field, identical to the WAL record format.
    pub fn append(&mut self, edit: &VersionEdit) -> io::Result<()> {
        let payload = serialize_edit(edit);
        let checksum = Crc32Checksum::compute(&payload);

        self.writer.write_all(&checksum.to_be_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.flush()
    }
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

/// Serialize a `VersionEdit` into the payload bytes (everything after CRC32).
///
/// AddFile layout    (big-endian):
///   [type: 1B=0x01][file_id: 8B][level: 1B]
///   [smallest_key_len: 4B][smallest_key: N]
///   [largest_key_len:  4B][largest_key:  M]
///   [max_seq: 8B]
///
/// RemoveFile layout (big-endian):
///   [type: 1B=0x02][file_id: 8B][level: 1B]
fn serialize_edit(edit: &VersionEdit) -> Vec<u8> {
    match edit {
        VersionEdit::AddFile { level, file_id, smallest_key, largest_key, max_seq } => {
            let sk = smallest_key.as_bytes();
            let lk = largest_key.as_bytes();
            let mut buf = Vec::with_capacity(
                EDIT_TYPE_SIZE + FILE_ID_SIZE + LEVEL_SIZE +
                KEY_LEN_SIZE + sk.len() +
                KEY_LEN_SIZE + lk.len() +
                size_of::<u64>(),
            );
            buf.push(EditType::AddFile as u8);
            buf.extend_from_slice(&file_id.to_be_bytes());
            buf.push(*level);
            buf.extend_from_slice(&(sk.len() as u32).to_be_bytes());
            buf.extend_from_slice(sk);
            buf.extend_from_slice(&(lk.len() as u32).to_be_bytes());
            buf.extend_from_slice(lk);
            buf.extend_from_slice(&max_seq.to_be_bytes());
            buf
        }
        VersionEdit::RemoveFile { level, file_id } => {
            let mut buf = Vec::with_capacity(EDIT_TYPE_SIZE + FILE_ID_SIZE + LEVEL_SIZE);
            buf.push(EditType::RemoveFile as u8);
            buf.extend_from_slice(&file_id.to_be_bytes());
            buf.push(*level);
            buf
        }
    }
}

/// Read all intact records from the MANIFEST file and build a `VersionState`.
///
/// Stops at the first CRC mismatch without returning an error — that record
/// represents a partial write interrupted by a crash.
///
/// Returns `Err(ManifestError::UnknownEditType)` if a record passes its CRC
/// check but carries an unrecognised edit type. This is distinct from a torn
/// write and indicates genuine corruption or a version mismatch.
/// Remove every `.sst` file in `dir` whose file id isn't tracked by `state`.
///
/// Targets crash-window orphans: the previous session wrote the SSTable to
/// disk but died before the corresponding `AddFile` made it to the manifest,
/// so neither the live `VersionState` nor any guard knows about the file.
/// Without this sweep, those files would accumulate on disk forever.
fn sweep_orphans(dir: &Path, state: &VersionState) -> io::Result<()> {
    let live: HashSet<FileId> = state.all_file_ids().collect();

    let entries = match fs::read_dir(dir) {
        Ok(it) => it,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("sst") {
            continue;
        }
        let Some(file_id) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<FileId>().ok())
        else {
            continue;
        };
        if live.contains(&file_id) {
            continue;
        }
        if let Err(e) = fs::remove_file(&path) {
            if e.kind() != io::ErrorKind::NotFound {
                eprintln!("[sst] failed to sweep orphan {:?}: {}", path, e);
            }
        }
    }

    Ok(())
}

fn replay(path: &Path, dir: &Path) -> Result<VersionState, ManifestError> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut state = VersionState::new(dir.to_path_buf());
    let mut byte_offset: u64 = 0;

    loop {
        let mut crc_buf = [0u8; CRC_SIZE];
        match reader.read_exact(&mut crc_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let expected_crc = u32::from_be_bytes(crc_buf);
        byte_offset += CRC_SIZE as u64;

        // Read edit type
        let mut type_buf = [0u8; EDIT_TYPE_SIZE];
        if reader.read_exact(&mut type_buf).is_err() {
            break;
        }
        let raw_type = type_buf[0];
        let edit_type = EditType::try_from(raw_type)
            .map_err(|_| ManifestError::UnknownEditType(raw_type, byte_offset))?;

        // Read file_id (8B) + level (1B) — present in both variants
        let mut id_level_buf = [0u8; FILE_ID_SIZE + LEVEL_SIZE];
        if reader.read_exact(&mut id_level_buf).is_err() {
            break;
        }
        let file_id = FileId::from_be_bytes(id_level_buf[0..FILE_ID_SIZE].try_into().unwrap());
        let level   = id_level_buf[FILE_ID_SIZE];

        let edit = match edit_type {
            EditType::AddFile => {
                // smallest_key_len (4B) + smallest_key + largest_key_len (4B) + largest_key + max_seq (8B)
                let mut sk_len_buf = [0u8; KEY_LEN_SIZE];
                if reader.read_exact(&mut sk_len_buf).is_err() { break; }
                let sk_len = u32::from_be_bytes(sk_len_buf) as usize;

                let mut sk_buf = vec![0u8; sk_len];
                if reader.read_exact(&mut sk_buf).is_err() { break; }

                let mut lk_len_buf = [0u8; KEY_LEN_SIZE];
                if reader.read_exact(&mut lk_len_buf).is_err() { break; }
                let lk_len = u32::from_be_bytes(lk_len_buf) as usize;

                let mut lk_buf = vec![0u8; lk_len];
                if reader.read_exact(&mut lk_buf).is_err() { break; }

                let mut max_seq_buf = [0u8; size_of::<u64>()];
                if reader.read_exact(&mut max_seq_buf).is_err() { break; }

                // Verify CRC before accepting the record
                let mut payload = Vec::with_capacity(
                    EDIT_TYPE_SIZE + FILE_ID_SIZE + LEVEL_SIZE +
                    KEY_LEN_SIZE + sk_len + KEY_LEN_SIZE + lk_len +
                    size_of::<u64>(),
                );
                payload.push(raw_type);
                payload.extend_from_slice(&id_level_buf);
                payload.extend_from_slice(&sk_len_buf);
                payload.extend_from_slice(&sk_buf);
                payload.extend_from_slice(&lk_len_buf);
                payload.extend_from_slice(&lk_buf);
                payload.extend_from_slice(&max_seq_buf);

                if !Crc32Checksum::verify(&payload, expected_crc) {
                    break;
                }

                let smallest_key = match String::from_utf8(sk_buf) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let largest_key = match String::from_utf8(lk_buf) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let max_seq = u64::from_be_bytes(max_seq_buf);

                VersionEdit::AddFile { level, file_id, smallest_key, largest_key, max_seq }
            }
            EditType::RemoveFile => {
                let mut payload = Vec::with_capacity(EDIT_TYPE_SIZE + FILE_ID_SIZE + LEVEL_SIZE);
                payload.push(raw_type);
                payload.extend_from_slice(&id_level_buf);

                if !Crc32Checksum::verify(&payload, expected_crc) {
                    break;
                }

                VersionEdit::RemoveFile { level, file_id }
            }
        };

        byte_offset += (EDIT_TYPE_SIZE + FILE_ID_SIZE + LEVEL_SIZE) as u64;
        state.apply(&edit);
    }

    Ok(state)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn tmp_dir() -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir()
            .join(format!("copperdb_manifest_{}_{}", std::process::id(), id));
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

    fn remove(file_id: u64, level: u8) -> VersionEdit {
        VersionEdit::RemoveFile { level, file_id }
    }

    #[test]
    fn empty_replay_returns_empty_state() {
        let dir = tmp_dir();
        let (_, state) = Manifest::open_or_create(&dir).unwrap();
        for lvl in 0..NUM_LEVELS {
            assert!(state.files_at_level(lvl).is_empty());
        }
    }

    #[test]
    fn round_trip_add_file() {
        let dir = tmp_dir();

        {
            let (mut m, _) = Manifest::open_or_create(&dir).unwrap();
            m.append(&add(1, 0, "apple", "mango")).unwrap();
        }

        let (_, state) = Manifest::open_or_create(&dir).unwrap();
        let files = state.files_at_level(0);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].file_id, 1);
        assert_eq!(files[0].smallest_key, "apple");
        assert_eq!(files[0].largest_key, "mango");
    }

    #[test]
    fn round_trip_remove_file() {
        let dir = tmp_dir();

        {
            let (mut m, _) = Manifest::open_or_create(&dir).unwrap();
            m.append(&add(1, 0, "a", "z")).unwrap();
            m.append(&remove(1, 0)).unwrap();
        }

        let (_, state) = Manifest::open_or_create(&dir).unwrap();
        assert!(state.files_at_level(0).is_empty());
    }

    #[test]
    fn multiple_files_across_levels() {
        let dir = tmp_dir();

        {
            let (mut m, _) = Manifest::open_or_create(&dir).unwrap();
            m.append(&add(1, 0, "a", "m")).unwrap();
            m.append(&add(2, 0, "n", "z")).unwrap();
            m.append(&add(3, 1, "a", "z")).unwrap();
        }

        let (_, state) = Manifest::open_or_create(&dir).unwrap();
        assert_eq!(state.files_at_level(0).len(), 2);
        assert_eq!(state.files_at_level(1).len(), 1);
        assert_eq!(state.files_at_level(2).len(), 0);
    }

    #[test]
    fn crash_recovery_crc_mismatch_at_tail() {
        let dir = tmp_dir();
        let path = manifest_path(&dir);

        {
            let (mut m, _) = Manifest::open_or_create(&dir).unwrap();
            m.append(&add(1, 0, "apple", "mango")).unwrap();
        }

        // Simulate a torn write: append bytes that look like an AddFile record
        // (valid edit type) but whose CRC (all zeros) won't match the payload.
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            // [crc=0x00000000][type=0x01 AddFile][file_id=0...][level=0]...
            // CRC of 0 will not match crc32(0x01 00 00 00 00 00 00 00 00 00),
            // so replay stops and returns Ok with the intact first record.
            file.write_all(&[0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).unwrap();
        }

        let (_, state) = Manifest::open_or_create(&dir).unwrap();
        assert_eq!(state.files_at_level(0).len(), 1);
        assert_eq!(state.files_at_level(0)[0].file_id, 1);
    }

    #[test]
    fn unknown_edit_type_returns_error() {
        let dir = tmp_dir();
        let path = manifest_path(&dir);

        {
            let (mut m, _) = Manifest::open_or_create(&dir).unwrap();
            m.append(&add(1, 0, "apple", "mango")).unwrap();
        }

        // Append a record whose CRC is valid but whose edit type is unknown (0xFF).
        // This cannot be a torn write — the CRC is correct — so it must be corruption
        // or a version mismatch. Replay should return Err rather than silently
        // truncating the version state.
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            let payload: &[u8] = &[0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
            let crc = crc32fast::hash(payload);
            file.write_all(&crc.to_be_bytes()).unwrap();
            file.write_all(payload).unwrap();
        }

        let result = Manifest::open_or_create(&dir);
        assert!(
            matches!(result, Err(ManifestError::UnknownEditType(0xFF, _))),
            "Expected UnknownEditType error, got: {:?}",
            result.map(|_| ())
        );
    }

    #[test]
    fn all_file_ids_for_next_sst_id() {
        let dir = tmp_dir();
        let (mut m, _) = Manifest::open_or_create(&dir).unwrap();
        m.append(&add(5, 0, "a", "m")).unwrap();
        m.append(&add(12, 1, "n", "z")).unwrap();
        m.append(&add(3, 2, "b", "c")).unwrap();
        drop(m);

        let (_, state) = Manifest::open_or_create(&dir).unwrap();
        let next = state.all_file_ids().max().unwrap_or(0) + 1;
        assert_eq!(next, 13);
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

    // Same invariant must hold after a round-trip through the manifest file,
    // since replay replays edits in the order they were appended (not key order).
    #[test]
    fn l1_files_sorted_after_replay() {
        let dir = tmp_dir();

        {
            let (mut m, _) = Manifest::open_or_create(&dir).unwrap();
            m.append(&add(3, 1, "mango", "peach")).unwrap();
            m.append(&add(1, 1, "apple", "cherry")).unwrap();
            m.append(&add(2, 1, "date",  "grape")).unwrap();
        }

        let (_, state) = Manifest::open_or_create(&dir).unwrap();
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
