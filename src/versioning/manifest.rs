use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};

use crate::wal::{Checksum, Crc32Checksum};

use super::{FileId, VersionEdit, VersionState};

const FILE_ID_SIZE:   usize = size_of::<FileId>();
const LEVEL_SIZE:     usize = size_of::<super::Level>();
const CRC_SIZE:       usize = size_of::<u32>();
const KEY_LEN_SIZE:   usize = size_of::<u32>();
const EDIT_TYPE_SIZE: usize = size_of::<u8>(); // EditType is #[repr(u8)]

const MANIFEST_FILENAME: &str = "MANIFEST";

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

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join(MANIFEST_FILENAME)
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

    /// Append one or more version edits as a single durable batch.
    ///
    /// Each edit is serialised as an independent CRC-framed record (same
    /// on-disk format as a sequence of single-edit appends). The whole batch
    /// is written to the kernel in one `write_all`, the `BufWriter` is
    /// flushed, and `fsync` is called on the underlying file — so when this
    /// returns, every record is durable on disk.
    ///
    /// For copperdb's compaction batch sizes (a few removes + a few adds,
    /// well under a 4 KB page), the kernel commits the dirty page either
    /// entirely or not at all, so this is effectively atomic on disk. A
    /// cross-page partial commit is still possible for very large batches;
    /// in that case, replay stops at the first torn record exactly as for a
    /// single-record torn-write today.
    pub fn append_batch(&mut self, edits: &[VersionEdit]) -> io::Result<()> {
        if edits.is_empty() {
            return Ok(());
        }

        let mut buf = Vec::new();
        for edit in edits {
            let payload = serialize_edit(edit);
            let checksum = Crc32Checksum::compute(&payload);
            buf.extend_from_slice(&checksum.to_be_bytes());
            buf.extend_from_slice(&payload);
        }

        self.writer.write_all(&buf)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }

    /// Append a single version edit. Thin wrapper over `append_batch` so
    /// single-edit callers (e.g. `record_flush`) inherit the same durability
    /// guarantee.
    pub fn append(&mut self, edit: &VersionEdit) -> io::Result<()> {
        self.append_batch(std::slice::from_ref(edit))
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

/// Read all intact records from the MANIFEST file and build a `VersionState`.
///
/// Stops at the first CRC mismatch without returning an error — that record
/// represents a partial write interrupted by a crash.
///
/// Returns `Err(ManifestError::UnknownEditType)` if a record passes its CRC
/// check but carries an unrecognised edit type. This is distinct from a torn
/// write and indicates genuine corruption or a version mismatch.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::versioning::NUM_LEVELS;
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

    // L1+ files added in reverse key order must come back sorted by smallest_key
    // even after a round-trip through the manifest file, since replay replays
    // edits in the order they were appended (not key order).
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

    // A mixed batch of removes and adds written through `append_batch` must
    // survive a clean reopen with the final version state matching what
    // sequential applies would have produced. This pins the contract that
    // record_compaction relies on: the manifest persists every edit in the
    // batch as one durable unit.
    #[test]
    fn append_batch_round_trip_replays_all_records() {
        let dir = tmp_dir();

        // Seed two L1 files so the batch's removes have something to drop.
        {
            let (mut m, _) = Manifest::open_or_create(&dir).unwrap();
            m.append(&add(1, 1, "apple", "fig")).unwrap();
            m.append(&add(2, 1, "grape", "mango")).unwrap();
        }

        // Now write a compaction-shaped batch: remove both L1 inputs, add two
        // L2 outputs.
        {
            let (mut m, _) = Manifest::open_or_create(&dir).unwrap();
            let batch = [
                remove(1, 1),
                remove(2, 1),
                add(3, 2, "apple", "fig"),
                add(4, 2, "grape", "mango"),
            ];
            m.append_batch(&batch).unwrap();
        }

        let (_, state) = Manifest::open_or_create(&dir).unwrap();
        assert!(state.files_at_level(1).is_empty(), "L1 should be empty after batch removes");
        let l2: Vec<u64> = state.files_at_level(2).iter().map(|m| m.file_id).collect();
        assert_eq!(l2, vec![3, 4], "L2 should contain both batch adds, in sorted order");
    }
}
