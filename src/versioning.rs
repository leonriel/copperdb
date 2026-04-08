use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::mem::size_of;

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

const EDIT_ADD_FILE: u8    = 0x01;
const EDIT_REMOVE_FILE: u8 = 0x02;

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

#[derive(Clone, Debug, PartialEq)]
pub struct SstableMetadata {
    pub file_id:      u64,
    pub level:        u8,
    pub smallest_key: String,
    pub largest_key:  String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum VersionEdit {
    AddFile {
        level:        u8,
        file_id:      u64,
        smallest_key: String,
        largest_key:  String,
    },
    RemoveFile {
        level:   u8,
        file_id: u64,
    },
}

// ---------------------------------------------------------------------------
// VersionState — in-memory snapshot of all live SSTable files
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct VersionState {
    levels: Vec<Vec<SstableMetadata>>,
}

impl VersionState {
    pub fn new() -> Self {
        Self {
            levels: vec![Vec::new(); NUM_LEVELS],
        }
    }

    pub fn apply(&mut self, edit: &VersionEdit) {
        match edit {
            VersionEdit::AddFile { level, file_id, smallest_key, largest_key } => {
                let level = *level as usize;
                if level >= self.levels.len() {
                    self.levels.resize(level + 1, Vec::new());
                }
                self.levels[level].push(SstableMetadata {
                    file_id:      *file_id,
                    level:        level as u8,
                    smallest_key: smallest_key.clone(),
                    largest_key:  largest_key.clone(),
                });
            }
            VersionEdit::RemoveFile { level, file_id } => {
                let level = *level as usize;
                if level < self.levels.len() {
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
    pub fn open_or_create(dir: &Path) -> Result<(Self, VersionState), ManifestError> {
        let path = manifest_path(dir);

        let version = if path.exists() {
            replay(&path)?
        } else {
            VersionState::new()
        };

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
///
/// RemoveFile layout (big-endian):
///   [type: 1B=0x02][file_id: 8B][level: 1B]
fn serialize_edit(edit: &VersionEdit) -> Vec<u8> {
    match edit {
        VersionEdit::AddFile { level, file_id, smallest_key, largest_key } => {
            let sk = smallest_key.as_bytes();
            let lk = largest_key.as_bytes();
            let mut buf = Vec::with_capacity(
                1 + size_of::<u64>() + 1 +
                size_of::<u32>() + sk.len() +
                size_of::<u32>() + lk.len(),
            );
            buf.push(EDIT_ADD_FILE);
            buf.extend_from_slice(&file_id.to_be_bytes());
            buf.push(*level);
            buf.extend_from_slice(&(sk.len() as u32).to_be_bytes());
            buf.extend_from_slice(sk);
            buf.extend_from_slice(&(lk.len() as u32).to_be_bytes());
            buf.extend_from_slice(lk);
            buf
        }
        VersionEdit::RemoveFile { level, file_id } => {
            let mut buf = Vec::with_capacity(1 + size_of::<u64>() + 1);
            buf.push(EDIT_REMOVE_FILE);
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
fn replay(path: &Path) -> Result<VersionState, ManifestError> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut state = VersionState::new();
    let mut byte_offset: u64 = 0;

    loop {
        let mut crc_buf = [0u8; 4];
        match reader.read_exact(&mut crc_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let expected_crc = u32::from_be_bytes(crc_buf);
        byte_offset += 4;

        // Read edit type
        let mut type_buf = [0u8; 1];
        if reader.read_exact(&mut type_buf).is_err() {
            break;
        }
        let edit_type = type_buf[0];

        // Read file_id (8B) + level (1B) — present in both variants
        let mut id_level_buf = [0u8; 9];
        if reader.read_exact(&mut id_level_buf).is_err() {
            break;
        }
        let file_id = u64::from_be_bytes(id_level_buf[0..8].try_into().unwrap());
        let level   = id_level_buf[8];

        let edit = match edit_type {
            EDIT_ADD_FILE => {
                // smallest_key_len (4B) + smallest_key + largest_key_len (4B) + largest_key
                let mut sk_len_buf = [0u8; 4];
                if reader.read_exact(&mut sk_len_buf).is_err() { break; }
                let sk_len = u32::from_be_bytes(sk_len_buf) as usize;

                let mut sk_buf = vec![0u8; sk_len];
                if reader.read_exact(&mut sk_buf).is_err() { break; }

                let mut lk_len_buf = [0u8; 4];
                if reader.read_exact(&mut lk_len_buf).is_err() { break; }
                let lk_len = u32::from_be_bytes(lk_len_buf) as usize;

                let mut lk_buf = vec![0u8; lk_len];
                if reader.read_exact(&mut lk_buf).is_err() { break; }

                // Verify CRC before accepting the record
                let mut payload = Vec::with_capacity(1 + 9 + 4 + sk_len + 4 + lk_len);
                payload.push(edit_type);
                payload.extend_from_slice(&id_level_buf);
                payload.extend_from_slice(&sk_len_buf);
                payload.extend_from_slice(&sk_buf);
                payload.extend_from_slice(&lk_len_buf);
                payload.extend_from_slice(&lk_buf);

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

                VersionEdit::AddFile { level, file_id, smallest_key, largest_key }
            }
            EDIT_REMOVE_FILE => {
                let mut payload = Vec::with_capacity(1 + 9);
                payload.push(edit_type);
                payload.extend_from_slice(&id_level_buf);

                if !Crc32Checksum::verify(&payload, expected_crc) {
                    break;
                }

                VersionEdit::RemoveFile { level, file_id }
            }
            _ => return Err(ManifestError::UnknownEditType(edit_type, byte_offset)),
        };

        byte_offset += 1 + 9; // type byte + file_id + level
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
            .join(format!("copperdb_versioning_{}_{}", std::process::id(), id));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn add(file_id: u64, level: u8, lo: &str, hi: &str) -> VersionEdit {
        VersionEdit::AddFile {
            level,
            file_id,
            smallest_key: lo.to_string(),
            largest_key:  hi.to_string(),
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
        let mut state = VersionState::new();
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
}
