use bloomfilter::Bloom;

use crate::core::{InternalKey, Record};
use crate::sstable::block::{Block, BlockError};
use crate::sstable::{
    FOOTER_SIZE, INDEX_OFFSET_SIZE, MAGIC_NUMBER, MAGIC_SIZE, META_OFFSET_SIZE,
    MagicNumber,
};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

#[derive(thiserror::Error, Debug)]
pub enum ReaderError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Block error: {0}")]
    Block(#[from] BlockError),

    #[error("Invalid magic number: expected 0xDEADBEEFCAFEBABE, found {0:#X}")]
    InvalidMagicNumber(MagicNumber),

    #[error("Corrupt SSTable data: {0}")]
    CorruptData(String),
}

/// Provides point-lookup access to an on-disk SSTable file.
///
/// On `open`, the footer is validated (magic number check) and the index
/// block is loaded into memory. Subsequent `search` calls use the index to
/// locate candidate data blocks, reading at most two blocks from disk to
/// find the newest version of a key.
pub struct SsTableReader {
    file: File,
    index_block: Block,
    /// Byte offset where the meta block begins (i.e. where data blocks end)
    meta_offset: u64,
    bloom: Bloom<String>,
}

impl SsTableReader {
    /// Opens an SSTable file, verifies the footer, and loads the index block into memory.
    pub fn open(path: &str) -> Result<Self, ReaderError> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();

        // The smallest valid SSTable is just a footer
        if file_len < (FOOTER_SIZE as u64) {
            return Err(ReaderError::CorruptData(
                "File too short to contain a valid footer".to_string(),
            ));
        }

        // 1. Read the footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;

        // FOOTER_SIZE is already a usize, so no casting needed here
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;

        // Determine slice boundaries based on our constants
        let meta_end = META_OFFSET_SIZE;
        let index_end = meta_end + INDEX_OFFSET_SIZE;
        let magic_end = index_end + MAGIC_SIZE;

        // Safely attempt to convert the slices into 8-byte arrays
        let meta_offset =
            u64::from_be_bytes(footer_buf[0..meta_end].try_into().map_err(|_| {
                ReaderError::CorruptData("Failed to parse meta offset from footer".to_string())
            })?);
        let index_offset =
            u64::from_be_bytes(footer_buf[meta_end..index_end].try_into().map_err(|_| {
                ReaderError::CorruptData("Failed to parse index offset from footer".to_string())
            })?);
        let magic =
            u64::from_be_bytes(footer_buf[index_end..magic_end].try_into().map_err(|_| {
                ReaderError::CorruptData("Failed to parse magic number from footer".to_string())
            })?);

        if magic != MAGIC_NUMBER {
            return Err(ReaderError::InvalidMagicNumber(magic));
        }

        // 2. Read and deserialize the Meta Block (bloom filter)
        let meta_size = index_offset - meta_offset;
        file.seek(SeekFrom::Start(meta_offset))?;
        let mut meta_data = vec![0u8; meta_size as usize];
        file.read_exact(&mut meta_data)?;

        let bloom = Bloom::from_bytes(meta_data)
            .map_err(|e| ReaderError::CorruptData(format!("Invalid bloom filter: {}", e)))?;

        // 3. Read and decode the Index Block
        let index_size = file_len - (FOOTER_SIZE as u64) - index_offset;
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_data = vec![0u8; index_size as usize];
        file.read_exact(&mut index_data)?;

        let index_block = Block::decode(index_data);

        Ok(Self {
            file,
            index_block,
            meta_offset,
            bloom,
        })
    }

    /// Searches for a specific user key in the SSTable.
    /// Returns Ok(None) if the key does not exist in this file.
    pub fn search(
        &mut self,
        target_key: &str,
    ) -> Result<Option<(InternalKey, Record)>, ReaderError> {
        // Fast path: if the bloom filter says the key is absent, skip disk reads
        if !self.bloom.check(&target_key.to_string()) {
            return Ok(None);
        }

        let num_offsets = self.index_block.get_num_offsets()?;

        if num_offsets == 0 {
            return Ok(None);
        }

        let mut last_less_than = None;
        let mut first_equal = None;

        // 1. Scan the index to find the candidate blocks
        for i in 0..num_offsets {
            let entry_offset = self.index_block.get_offset(i, num_offsets)?;
            let (key, record) = self.index_block.decode_entry(entry_offset)?;

            let block_start = match record {
                Record::Put(val) => u64::from_be_bytes(val.try_into().map_err(|_| {
                    ReaderError::CorruptData("Index block pointer is not 8 bytes".to_string())
                })?),
                Record::Delete => {
                    return Err(ReaderError::CorruptData(
                        "Index block contains Delete record".to_string(),
                    ));
                }
            };

            let next_block_offset = if i + 1 < num_offsets {
                let next_entry_offset = self.index_block.get_offset(i + 1, num_offsets)?;
                let (_, next_record) = self.index_block.decode_entry(next_entry_offset)?;

                match next_record {
                    Record::Put(val) => u64::from_be_bytes(val.try_into().map_err(|_| {
                        ReaderError::CorruptData("Next index pointer is not 8 bytes".to_string())
                    })?),
                    Record::Delete => {
                        return Err(ReaderError::CorruptData(
                            "Next index contains Delete".to_string(),
                        ));
                    }
                }
            } else {
                self.meta_offset
            };

            // Route to the correct candidate blocks
            if key.user_key.as_str() < target_key {
                last_less_than = Some((block_start, next_block_offset));
            } else if key.user_key.as_str() == target_key {
                if first_equal.is_none() {
                    first_equal = Some((block_start, next_block_offset));
                }
                // Once we find the first block starting with the target, we don't need to look
                // further. Subsequent blocks will only contain older versions.
                break;
            } else {
                // key.user_key > target_key
                break;
            }
        }

        // Gather our candidates (at most 2 blocks)
        let mut candidate_blocks = Vec::new();
        if let Some(block) = last_less_than {
            candidate_blocks.push(block);
        }
        if let Some(block) = first_equal {
            candidate_blocks.push(block);
        }

        // 2. Read the candidate blocks from disk and search them in order
        for (start_offset, next_offset) in candidate_blocks {
            let block_size = next_offset - start_offset;

            self.file.seek(SeekFrom::Start(start_offset))?;
            let mut block_data = vec![0u8; block_size as usize];
            self.file.read_exact(&mut block_data)?;

            let data_block = Block::decode(block_data);

            // If we find the key in the first candidate (which contains the highest seq numbers),
            // we return it immediately and skip reading the second block.
            if let Some(result) = data_block.search(target_key)? {
                return Ok(Some(result));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::KvIterator;
    use crate::core::Record;
    use crate::sstable::writer::SsTableBuilder;
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::PathBuf; // Assuming this is where KvIterator lives

    // =========================================================================
    // Test Helpers
    // =========================================================================

    struct MockIterator {
        entries: std::vec::IntoIter<(String, Record, u64)>,
        current: Option<(String, Record, u64)>,
    }

    impl MockIterator {
        fn new(entries: Vec<(String, Record, u64)>) -> Self {
            let mut entries_iter = entries.into_iter();
            let current = entries_iter.next();
            Self {
                entries: entries_iter,
                current,
            }
        }
    }

    impl KvIterator for MockIterator {
        fn is_valid(&self) -> bool {
            self.current.is_some()
        }

        fn next(&mut self) -> Option<(String, Record, u64)> {
            let item_to_return = self.current.take();
            self.current = self.entries.next();
            item_to_return
        }
    }

    struct TempFileGuard {
        path: PathBuf,
    }

    impl TempFileGuard {
        fn new(name: &str) -> Self {
            let mut path = std::env::temp_dir();
            path.push(name);
            let _ = fs::remove_file(&path);
            Self { path }
        }

        fn path_str(&self) -> &str {
            self.path.to_str().unwrap()
        }
    }

    impl Drop for TempFileGuard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn b(str: &str) -> Vec<u8> {
        str.as_bytes().to_vec()
    }

    /// Helper to quickly build an SSTable for reading tests
    fn build_test_sst(path: &str, entries: Vec<(String, Record, u64)>) {
        let mut builder = SsTableBuilder::new(path).unwrap();
        let iter = Box::new(MockIterator::new(entries));
        builder.build_from_iterator(iter).unwrap();
    }

    // =========================================================================
    // SsTableReader Tests (5 Tests)
    // =========================================================================

    #[test]
    fn test_reader_open_file_too_short() {
        let file = TempFileGuard::new("too_short.sst");

        // Write a 10-byte file (smaller than our FOOTER_SIZE)
        {
            let mut f = File::create(&file.path).unwrap();
            f.write_all(&[0u8; 10]).unwrap();
        }

        let result = SsTableReader::open(file.path_str());
        assert!(result.is_err());
        if let Err(ReaderError::CorruptData(msg)) = result {
            assert!(msg.contains("too short"));
        } else {
            panic!("Expected CorruptData error for short file");
        }
    }

    #[test]
    fn test_reader_open_invalid_magic() {
        let file = TempFileGuard::new("bad_magic.sst");

        // Write exactly FOOTER_SIZE bytes, but with a bad magic number
        {
            let mut f = File::create(&file.path).unwrap();
            let meta_offset = 0u64.to_be_bytes();
            let index_offset = 0u64.to_be_bytes();
            let bad_magic = 0xBADBADBADBADBADBu64.to_be_bytes();
            f.write_all(&meta_offset).unwrap();
            f.write_all(&index_offset).unwrap();
            f.write_all(&bad_magic).unwrap();
        }

        let result = SsTableReader::open(file.path_str());
        assert!(result.is_err());
        if let Err(ReaderError::InvalidMagicNumber(magic)) = result {
            assert_eq!(magic, 0xBADBADBADBADBADBu64);
        } else {
            panic!("Expected InvalidMagicNumber error");
        }
    }

    #[test]
    fn test_reader_search_single_block() {
        let file = TempFileGuard::new("read_single_block.sst");

        // 3 entries will easily fit in one 4KB block
        build_test_sst(
            file.path_str(),
            vec![
                ("apple".to_string(), Record::Put(b("red")), 3),
                ("banana".to_string(), Record::Put(b("yellow")), 2),
                ("cherry".to_string(), Record::Delete, 1),
            ],
        );

        let mut reader = SsTableReader::open(file.path_str()).unwrap();

        // 1. Search for existing Put
        let (k1, r1) = reader.search("apple").unwrap().expect("apple should exist");
        assert_eq!(k1.user_key, "apple");
        assert_eq!(k1.seq_num, 3);
        assert!(matches!(r1, Record::Put(v) if v == b("red")));

        // 2. Search for existing Delete
        let (k2, r2) = reader
            .search("cherry")
            .unwrap()
            .expect("cherry should exist");
        assert_eq!(k2.user_key, "cherry");
        assert_eq!(k2.seq_num, 1);
        assert!(matches!(r2, Record::Delete));
    }

    #[test]
    fn test_reader_search_multiple_blocks() {
        let file = TempFileGuard::new("read_multi_block.sst");

        let mut entries = Vec::new();
        // Create 10 entries with 1000 byte values. This will force multiple blocks.
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            let val = format!("val_{:02}", i).into_bytes();
            // Pad the value to 1000 bytes
            let mut padded_val = val.clone();
            padded_val.resize(1000, 0);
            entries.push((key, Record::Put(padded_val), 100 - i));
        }

        build_test_sst(file.path_str(), entries);

        let mut reader = SsTableReader::open(file.path_str()).unwrap();

        // Check a key from the beginning, middle, and end of the SSTable
        let test_indices = [0, 5, 9];
        for i in test_indices {
            let target_key = format!("key_{:02}", i);
            let (k, r) = reader
                .search(&target_key)
                .unwrap()
                .expect("Key should exist");

            assert_eq!(k.user_key, target_key);
            assert_eq!(k.seq_num, 100 - i);

            if let Record::Put(val) = r {
                let expected_prefix = format!("val_{:02}", i).into_bytes();
                assert_eq!(&val[0..6], &expected_prefix[..]);
                assert_eq!(val.len(), 1000);
            } else {
                panic!("Expected Put record");
            }
        }
    }

    #[test]
    fn test_reader_search_not_present() {
        let file = TempFileGuard::new("read_not_present.sst");

        build_test_sst(
            file.path_str(),
            vec![
                ("b_key".to_string(), Record::Put(b("b")), 2),
                ("d_key".to_string(), Record::Put(b("d")), 1),
                ("f_key".to_string(), Record::Put(b("f")), 0),
            ],
        );

        let mut reader = SsTableReader::open(file.path_str()).unwrap();

        // 1. Target key is lexicographically smaller than the first key
        let res_before = reader.search("a_key").unwrap();
        assert!(
            res_before.is_none(),
            "Should return None for key smaller than first entry"
        );

        // 2. Target key falls between two existing keys (between b_key and d_key)
        let res_middle = reader.search("c_key").unwrap();
        assert!(
            res_middle.is_none(),
            "Should return None for missing key in the middle of a block"
        );

        // 3. Target key is lexicographically larger than the last key
        let res_after = reader.search("z_key").unwrap();
        assert!(
            res_after.is_none(),
            "Should return None for key larger than last entry"
        );
    }

    #[test]
    fn test_reader_search_multiple_versions_across_blocks() {
        let file = TempFileGuard::new("read_mvcc_multi_block.sst");

        let mut entries = Vec::new();

        // We use a 2000-byte padding so that each "banana" takes up ~half a block.
        // This guarantees that the 3 versions of banana span across at least 2 block boundaries.
        let pad = vec![0u8; 2000];

        entries.push(("apple".to_string(), Record::Put(b("red")), 1));

        // NEWEST version: Should end up in the same block as "apple" or start its own
        entries.push(("banana".to_string(), Record::Put(pad.clone()), 3));

        // MIDDLE version: Will overflow into a new block
        entries.push(("banana".to_string(), Record::Put(pad.clone()), 2));

        // OLDEST version: Will overflow into yet another block
        entries.push(("banana".to_string(), Record::Put(pad.clone()), 1));

        entries.push(("cherry".to_string(), Record::Put(b("red")), 1));

        build_test_sst(file.path_str(), entries);

        let mut reader = SsTableReader::open(file.path_str()).unwrap();

        // When we search for "banana", the reader must correctly identify the block
        // containing sequence number 3 and return it, ignoring 2 and 1.
        let (k, r) = reader
            .search("banana")
            .unwrap()
            .expect("banana should exist");

        assert_eq!(k.user_key, "banana");
        assert_eq!(
            k.seq_num, 3,
            "Failed to retrieve the newest version! Reader index routing might be flawed."
        );

        if let Record::Put(val) = r {
            assert_eq!(val.len(), 2000);
        } else {
            panic!("Expected Put record");
        }
    }

    #[test]
    fn test_reader_search_large_record_spanning_multiple_blocks() {
        let file = TempFileGuard::new("read_large_record.sst");

        // Create a 10KB value, which is significantly larger than a standard 4KB block
        let large_val_size = 10 * 1024;
        let mut large_val = vec![0u8; large_val_size];

        // Fill it with a predictable pattern to verify data integrity later
        for i in 0..large_val_size {
            large_val[i] = (i % 256) as u8;
        }

        // Build an SSTable with the large record sandwiched between two normal records
        build_test_sst(
            file.path_str(),
            vec![
                ("a_small".to_string(), Record::Put(b("tiny")), 3),
                ("b_large".to_string(), Record::Put(large_val.clone()), 2),
                ("c_small".to_string(), Record::Put(b("tiny_again")), 1),
            ],
        );

        let mut reader = SsTableReader::open(file.path_str()).unwrap();

        // 1. Retrieve and verify the massive record
        let (k, r) = reader
            .search("b_large")
            .unwrap()
            .expect("large key should exist");

        assert_eq!(k.user_key, "b_large");
        assert_eq!(k.seq_num, 2);

        if let Record::Put(val) = r {
            assert_eq!(
                val.len(),
                large_val_size,
                "The retrieved value should be exactly 10KB"
            );
            // Check that the data wasn't corrupted or truncated across block boundaries
            assert_eq!(
                val, large_val,
                "The retrieved data pattern should perfectly match"
            );
        } else {
            panic!("Expected Put record for b_large");
        }

        // 2. Verify the key located AFTER the massive block is still indexed and readable
        let (k2, r2) = reader
            .search("c_small")
            .unwrap()
            .expect("small key after large should exist");
        assert_eq!(k2.user_key, "c_small");
        assert!(matches!(r2, Record::Put(v) if v == b("tiny_again")));

        // 3. Verify the key located BEFORE the massive block
        let (k1, r1) = reader
            .search("a_small")
            .unwrap()
            .expect("small key before large should exist");
        assert_eq!(k1.user_key, "a_small");
        assert!(matches!(r1, Record::Put(v) if v == b("tiny")));
    }
}
