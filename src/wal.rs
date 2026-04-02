use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::{Path, PathBuf};

/// Abstraction over checksum algorithms. Swap the implementing type to change
/// the checksum scheme without touching any WAL read/write logic.
pub trait Checksum {
    fn compute(data: &[u8]) -> u32;

    fn verify(data: &[u8], expected: u32) -> bool {
        Self::compute(data) == expected
    }
}

pub struct Crc32Checksum;

impl Checksum for Crc32Checksum {
    fn compute(data: &[u8]) -> u32 {
        crc32fast::hash(data)
    }
}

const OP_PUT: u8 = 0x01;
const OP_DELETE: u8 = 0x02;

#[derive(Debug, Clone, PartialEq)]
pub enum WalOpType {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub seq_num: u64,
    pub op: WalOpType,
    pub key: String,
    /// Empty for `Delete` records.
    pub value: Vec<u8>,
}

/// An append-only WAL file paired with exactly one MemTable generation.
///
/// `C` is the checksum strategy — swap it without touching any I/O logic.
///
/// # Record layout
/// ```text
/// [checksum: 4B LE][seq_num: 8B LE][op_type: 1B][key_len: 4B LE][key: N][val_len: 4B LE][val: M]
/// ```
/// The checksum covers every byte *after* the checksum field.
pub struct Wal<C: Checksum> {
    writer: BufWriter<File>,
    path: PathBuf,
    generation: u64,
    _checksum: PhantomData<C>,
}

fn wal_path(dir: &Path, generation: u64) -> PathBuf {
    // Zero-padded so lexicographic order == numeric order.
    dir.join(format!("{:020}.wal", generation))
}

impl<C: Checksum> Wal<C> {
    /// Create a new WAL file for `generation` inside `dir`.
    /// Fails if a file for that generation already exists.
    pub fn create(dir: &Path, generation: u64) -> io::Result<Self> {
        let path = wal_path(dir, generation);
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        Ok(Wal {
            writer: BufWriter::new(file),
            path,
            generation,
            _checksum: PhantomData,
        })
    }

    /// Append a `Put` record. Must be called before inserting into the MemTable.
    pub fn append_put(&mut self, seq_num: u64, key: &str, value: &[u8]) -> io::Result<()> {
        self.write_record(seq_num, OP_PUT, key.as_bytes(), value)
    }

    /// Append a `Delete` (tombstone) record. Must be called before inserting
    /// into the MemTable.
    pub fn append_delete(&mut self, seq_num: u64, key: &str) -> io::Result<()> {
        self.write_record(seq_num, OP_DELETE, key.as_bytes(), &[])
    }

    /// fsync the underlying file. Call this when strict durability is required
    /// (e.g. after every write, or after a batch of writes).
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }

    /// Consume this WAL and delete its file from disk.
    /// Call this after the paired MemTable has been successfully flushed to an SSTable.
    pub fn delete(self) -> io::Result<()> {
        let path = self.path.clone();
        drop(self.writer); // close before unlinking
        fs::remove_file(path)
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    fn write_record(
        &mut self,
        seq_num: u64,
        op_type: u8,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<()> {
        let key_len = key.len() as u32;
        let val_len = value.len() as u32;

        let mut payload = Vec::with_capacity(
            size_of::<u64>() + size_of::<u8>() + size_of::<u32>() + key.len() + size_of::<u32>() + value.len(),
        );
        payload.extend_from_slice(&seq_num.to_le_bytes());
        payload.push(op_type);
        payload.extend_from_slice(&key_len.to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(&val_len.to_le_bytes());
        payload.extend_from_slice(value);

        let checksum = C::compute(&payload);

        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        // Flush the BufWriter on every record so the OS has the bytes even if
        // we skip fsync. A full sync() is available for stricter guarantees.
        self.writer.flush()
    }
}

/// Read all intact records from a single WAL file.
///
/// Stops at the first record whose checksum doesn't match — that record
/// represents a partial write interrupted by a crash. All prior records are
/// returned as valid.
pub fn replay<C: Checksum>(path: &Path) -> io::Result<Vec<WalRecord>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut records = Vec::new();

    loop {
        let mut checksum_buf = [0u8; 4];
        match reader.read_exact(&mut checksum_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let expected = u32::from_le_bytes(checksum_buf);

        // seq_num (8) + op_type (1) + key_len (4)
        let mut header = [0u8; 13];
        if reader.read_exact(&mut header).is_err() {
            break;
        }
        let seq_num = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let op_type = header[8];
        let key_len = u32::from_le_bytes(header[9..13].try_into().unwrap()) as usize;

        let mut key_buf = vec![0u8; key_len];
        if reader.read_exact(&mut key_buf).is_err() {
            break;
        }

        let mut val_len_buf = [0u8; 4];
        if reader.read_exact(&mut val_len_buf).is_err() {
            break;
        }
        let val_len = u32::from_le_bytes(val_len_buf) as usize;

        let mut val_buf = vec![0u8; val_len];
        if reader.read_exact(&mut val_buf).is_err() {
            break;
        }

        // Reconstruct the payload in the same order it was written.
        let mut payload = Vec::with_capacity(header.len() + key_len + size_of::<u32>() + val_len);
        payload.extend_from_slice(&header);
        payload.extend_from_slice(&key_buf);
        payload.extend_from_slice(&val_len_buf);
        payload.extend_from_slice(&val_buf);

        if !C::verify(&payload, expected) {
            break;
        }

        let op = match op_type {
            OP_PUT => WalOpType::Put,
            OP_DELETE => WalOpType::Delete,
            _ => break,
        };

        let key = match String::from_utf8(key_buf) {
            Ok(k) => k,
            Err(_) => break,
        };

        records.push(WalRecord { seq_num, op, key, value: val_buf });
    }

    Ok(records)
}

/// Scan `dir` for all `*.wal` files, replay them in generation order, and
/// return the full ordered sequence of records.
///
/// Called by `db.rs` on startup to rebuild the MemTable. WAL files for
/// generations that were already flushed to SSTables will have been deleted
/// (`Wal::delete`), so only unflushed generations are replayed.
pub fn recover_all<C: Checksum>(dir: &Path) -> io::Result<Vec<WalRecord>> {
    let mut wal_files: Vec<(u64, PathBuf)> = fs::read_dir(dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()?.to_str()? != "wal" {
                return None;
            }
            let stem = path.file_stem()?.to_str()?;
            let generation: u64 = stem.parse().ok()?;
            Some((generation, path))
        })
        .collect();

    wal_files.sort_by_key(|(g, _)| *g);

    let mut all_records = Vec::new();
    for (_, path) in wal_files {
        all_records.extend(replay::<C>(&path)?);
    }

    Ok(all_records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn make_test_dir() -> PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("copperdb_test_{}_{}", std::process::id(), id));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn roundtrip_put() {
        let dir = make_test_dir();
        let mut wal = Wal::<Crc32Checksum>::create(dir.as_path(), 0).unwrap();
        wal.append_put(1, "hello", b"world").unwrap();
        drop(wal);

        let records = replay::<Crc32Checksum>(&wal_path(dir.as_path(), 0)).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].seq_num, 1);
        assert_eq!(records[0].op, WalOpType::Put);
        assert_eq!(records[0].key, "hello");
        assert_eq!(records[0].value, b"world");
    }

    #[test]
    fn roundtrip_delete() {
        let dir = make_test_dir();
        let mut wal = Wal::<Crc32Checksum>::create(dir.as_path(), 0).unwrap();
        wal.append_delete(2, "bye").unwrap();
        drop(wal);

        let records = replay::<Crc32Checksum>(&wal_path(dir.as_path(), 0)).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op, WalOpType::Delete);
        assert_eq!(records[0].key, "bye");
        assert!(records[0].value.is_empty());
    }

    #[test]
    fn multiple_records_in_order() {
        let dir = make_test_dir();
        let mut wal = Wal::<Crc32Checksum>::create(dir.as_path(), 0).unwrap();
        for i in 0u64..10 {
            wal.append_put(i, &format!("key{i}"), format!("val{i}").as_bytes())
                .unwrap();
        }
        drop(wal);

        let records = replay::<Crc32Checksum>(&wal_path(dir.as_path(), 0)).unwrap();
        assert_eq!(records.len(), 10);
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.seq_num, i as u64);
            assert_eq!(r.key, format!("key{i}"));
        }
    }

    #[test]
    fn corrupt_record_stops_replay() {
        let dir = make_test_dir();
        let path = wal_path(dir.as_path(), 0);

        {
            let mut wal = Wal::<Crc32Checksum>::create(dir.as_path(), 0).unwrap();
            wal.append_put(1, "a", b"1").unwrap();
            wal.append_put(2, "b", b"2").unwrap();
        }

        // Append raw garbage to simulate a partial/corrupt third write.
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"\xff\xff\xff\xff garbage bytes").unwrap();
        }

        let records = replay::<Crc32Checksum>(&path).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn recover_all_multiple_generations() {
        let dir = make_test_dir();

        for g in 0u64..3 {
            let mut wal = Wal::<Crc32Checksum>::create(dir.as_path(), g).unwrap();
            wal.append_put(g * 10, &format!("k{g}"), b"v").unwrap();
        }

        let records = recover_all::<Crc32Checksum>(dir.as_path()).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].seq_num, 0);
        assert_eq!(records[1].seq_num, 10);
        assert_eq!(records[2].seq_num, 20);
    }

    #[test]
    fn delete_removes_file() {
        let dir = make_test_dir();
        let path = wal_path(dir.as_path(), 0);

        let wal = Wal::<Crc32Checksum>::create(dir.as_path(), 0).unwrap();
        assert!(path.exists());
        wal.delete().unwrap();
        assert!(!path.exists());
    }
}
