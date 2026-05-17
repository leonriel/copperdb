// writer.rs
use std::fs::File;
use std::io::Write;

use bloomfilter::Bloom;

use crate::core::{InternalKey, KvIterator, Record};
use crate::sstable::block::BlockBuilder;
use crate::sstable::{
    FOOTER_SIZE, INDEX_OFFSET_SIZE, IndexOffset, MAGIC_NUMBER, MAGIC_SIZE, META_OFFSET_SIZE,
};

#[derive(thiserror::Error, Debug)]
pub enum WriterError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid data: {0}")]
    InvalidData(String),
}


/// The default false-positive rate for the bloom filter (1%).
const BLOOM_FP_RATE: f64 = 0.01;

/// The estimated number of unique keys per SSTable, used to size the bloom
/// filter. Overestimating wastes a small amount of memory; underestimating
/// raises the false-positive rate.
const BLOOM_ESTIMATED_ITEMS: u32 = 10_000;

/// Target byte budget for an L1 (leaf) index block. Each L1 block maps a
/// contiguous run of data blocks to their byte offsets. At ~51 bytes per
/// entry, 64 KB holds ~1,300 data-block pointers (≈ 5 MB of data per L1
/// block), which keeps a single L1 read cheap on the read path while still
/// allowing the top index to stay tiny.
const L1_INDEX_TARGET_BLOCK_SIZE: usize = 64 * 1024;

/// Target byte budget for the top index block. The top index has one entry
/// per L1 block; for typical 64 MB SSTables this is ~16 entries, well under
/// 1 KB. Sized at 1 MB so multi-GB SSTables can still address all their L1
/// blocks from a single top block (~80 GB at this size).
const TOP_INDEX_TARGET_BLOCK_SIZE: usize = 1024 * 1024;

/// Writes a sorted stream of key-record pairs to an on-disk SSTable file.
///
/// `build_from_iterator` consumes a `KvIterator` (typically from a frozen
/// memtable), splits the entries into 4 KB data blocks, and appends an index
/// block mapping each block's first key to its byte offset. The file is
/// finalized with a footer containing the index block offset and a magic
/// number for validation.
pub struct SsTableBuilder {
    file: File,
    current_block: BlockBuilder,
    /// Stores the first key of each block and the block's physical byte offset in the file
    block_index: Vec<(String, u64)>,
    current_offset: IndexOffset,
    /// Tracks the first user_key added to the current block
    first_key_of_current_block: Option<String>,
    /// Bloom filter tracking every unique user_key added to this SSTable
    bloom: Bloom<String>,
    /// First user_key written — set once on the first entry, never updated.
    smallest_key: Option<String>,
    /// Last user_key written — updated on every entry; holds the largest after build.
    largest_key: Option<String>,
    /// Highest seq_num seen across every entry. Needed on engine reopen so
    /// `next_seq` can exceed every durable record — otherwise new writes get
    /// seq numbers lower than SSTable records and lose compaction dedup.
    max_seq_num: u64,
}

impl SsTableBuilder {
    pub fn new(path: &str) -> Result<Self, WriterError> {
        let file = File::create(path)?;
        let bloom = Bloom::new_for_fp_rate(BLOOM_ESTIMATED_ITEMS as usize, BLOOM_FP_RATE)
            .map_err(|e| WriterError::InvalidData(e.to_string()))?;
        Ok(Self {
            file,
            current_block: BlockBuilder::new(),
            block_index: Vec::new(),
            current_offset: 0,
            first_key_of_current_block: None,
            bloom,
            smallest_key: None,
            largest_key: None,
            max_seq_num: 0,
        })
    }

    /// Returns the inclusive key range `(smallest, largest)` and the highest
    /// seq_num of all entries written to this SSTable. Returns `None` if the
    /// iterator was empty. Must be called after `build_from_iterator`.
    pub fn summary(self) -> Option<(String, String, u64)> {
        match (self.smallest_key, self.largest_key) {
            (Some(lo), Some(hi)) => Some((lo, hi, self.max_seq_num)),
            _ => None,
        }
    }

    /// Consumes a MemTable iterator, slicing it into 4KB blocks and writing to disk.
    pub fn build_from_iterator(
        &mut self,
        mut iter: Box<dyn KvIterator>,
    ) -> Result<(), WriterError> {
        while let Some((user_key, record, seq_num)) = iter.next() {
            let internal_key = InternalKey {
                user_key: user_key.clone(),
                seq_num,
            };

            // Attempt to add to `current_block`.
            // BlockBuilder::add returns `false` if the entry pushes us over TARGET_BLOCK_SIZE.
            if !self.current_block.add(&internal_key, &record) {
                // Block is full. Flush it.
                self.finish_current_block()?;

                // Add the entry to the fresh block
                let added = self.current_block.add(&internal_key, &record);
                if !added {
                    return Err(WriterError::InvalidData(
                        "A single key-value pair exceeds the TARGET_BLOCK_SIZE.".to_string(),
                    ));
                }
            }

            // Track every user_key in the bloom filter
            self.bloom.set(&user_key);

            // Track key range across the whole file
            if self.smallest_key.is_none() {
                self.smallest_key = Some(user_key.clone());
            }
            self.largest_key = Some(user_key.clone());
            if seq_num > self.max_seq_num {
                self.max_seq_num = seq_num;
            }

            // If this is the first entry in a block, save it for the index
            if self.first_key_of_current_block.is_none() {
                self.first_key_of_current_block = Some(user_key);
            }
        }

        // Flush the final data block if it has anything in it
        if !self.current_block.is_empty() {
            self.finish_current_block()?;
        }

        // Write the Meta Block (serialized bloom filter)
        let meta_offset = self.current_offset;
        let meta_data = self.bloom.to_bytes();
        self.file.write_all(&meta_data)?;
        self.current_offset += meta_data.len() as u64;

        // Write the two-level index (L1 blocks then top index).
        // `top_index_offset` is what gets recorded in the footer.
        let top_index_offset = self.write_two_level_index()?;

        // Write a fixed-size Footer (24 bytes)
        let mut footer = [0u8; FOOTER_SIZE];
        let meta_end = META_OFFSET_SIZE;
        let index_end = meta_end + INDEX_OFFSET_SIZE;
        let magic_end = index_end + MAGIC_SIZE;

        footer[0..meta_end].copy_from_slice(&meta_offset.to_be_bytes());
        footer[meta_end..index_end].copy_from_slice(&top_index_offset.to_be_bytes());
        footer[index_end..magic_end].copy_from_slice(&MAGIC_NUMBER.to_be_bytes());

        self.file.write_all(&footer)?;
        self.file.sync_all()?;

        Ok(())
    }

    /// Add a single entry to the SSTable. Flushes the current data block to
    /// disk when it is full and opens a fresh one. Call `finish_file` when done.
    pub fn add_entry(
        &mut self,
        user_key: &str,
        record: &Record,
        seq_num: u64,
    ) -> Result<(), WriterError> {
        let internal_key = InternalKey {
            user_key: user_key.to_string(),
            seq_num,
        };

        if !self.current_block.add(&internal_key, record) {
            self.finish_current_block()?;
            if !self.current_block.add(&internal_key, record) {
                return Err(WriterError::InvalidData(
                    "A single key-value pair exceeds TARGET_BLOCK_SIZE".to_string(),
                ));
            }
        }

        self.bloom.set(&user_key.to_string());

        if self.smallest_key.is_none() {
            self.smallest_key = Some(user_key.to_string());
        }
        self.largest_key = Some(user_key.to_string());
        if seq_num > self.max_seq_num {
            self.max_seq_num = seq_num;
        }

        if self.first_key_of_current_block.is_none() {
            self.first_key_of_current_block = Some(user_key.to_string());
        }

        Ok(())
    }

    /// Bytes committed to disk so far, not counting data still buffered in the
    /// current unflushed block. Used by the compaction worker to decide when to
    /// roll over to a new output file.
    pub fn current_size(&self) -> u64 {
        self.current_offset
    }

    /// Flush remaining buffered data, write the meta block (bloom filter),
    /// index block, and footer, then sync. Returns
    /// `Some((smallest_key, largest_key, max_seq_num))` if any entries were
    /// written, or `None` for an empty file.
    pub fn finish_file(mut self) -> Result<Option<(String, String, u64)>, WriterError> {
        if !self.current_block.is_empty() {
            self.finish_current_block()?;
        }

        // Write the Meta Block (serialized bloom filter)
        let meta_offset = self.current_offset;
        let meta_data = self.bloom.to_bytes();
        self.file.write_all(&meta_data)?;
        self.current_offset += meta_data.len() as u64;

        // Write the two-level index (L1 blocks then top index).
        let top_index_offset = self.write_two_level_index()?;

        // Write the Footer: [meta_offset | top_index_offset | magic]
        let mut footer = [0u8; FOOTER_SIZE];
        let meta_end = META_OFFSET_SIZE;
        let index_end = meta_end + INDEX_OFFSET_SIZE;
        let magic_end = index_end + MAGIC_SIZE;
        footer[0..meta_end].copy_from_slice(&meta_offset.to_be_bytes());
        footer[meta_end..index_end].copy_from_slice(&top_index_offset.to_be_bytes());
        footer[index_end..magic_end].copy_from_slice(&MAGIC_NUMBER.to_be_bytes());
        self.file.write_all(&footer)?;
        self.file.sync_all()?;

        Ok(match (self.smallest_key, self.largest_key) {
            (Some(lo), Some(hi)) => Some((lo, hi, self.max_seq_num)),
            _ => None,
        })
    }

    /// Build and write the two-level index for this SSTable.
    ///
    /// Walks `self.block_index` (the in-memory list of `(first_key,
    /// data_block_offset)` recorded by `finish_current_block`), packs the
    /// entries into L1 index blocks of `L1_INDEX_TARGET_BLOCK_SIZE`, writes
    /// each L1 block to disk, then builds and writes the top index whose
    /// entries point at the L1 blocks. Returns the byte offset of the top
    /// index block, which the caller writes into the footer.
    ///
    /// The two-level structure removes the single-block index ceiling that
    /// would otherwise cap SSTables at ~1 MB of index entries.
    fn write_two_level_index(&mut self) -> Result<u64, WriterError> {
        let mut l1_index: Vec<(String, u64)> = Vec::new();
        let mut current_l1 = BlockBuilder::with_target_size(L1_INDEX_TARGET_BLOCK_SIZE);
        let mut current_l1_first_key: Option<String> = None;

        for (first_key, data_block_offset) in &self.block_index {
            let entry_key = InternalKey {
                user_key: first_key.clone(),
                seq_num: 0, // Dummy seq for index entries.
            };
            let entry_val = Record::Put(data_block_offset.to_be_bytes().to_vec());

            if !current_l1.add(&entry_key, &entry_val) {
                // Current L1 is full. Flush it, then start a fresh L1 that
                // opens with the rejected entry.
                let full_l1 = std::mem::replace(
                    &mut current_l1,
                    BlockBuilder::with_target_size(L1_INDEX_TARGET_BLOCK_SIZE),
                );
                let l1_data = full_l1.build();
                let l1_offset = self.current_offset;
                self.file.write_all(&l1_data)?;
                self.current_offset += l1_data.len() as u64;
                l1_index.push((
                    current_l1_first_key
                        .take()
                        .expect("L1 builder accepted entries; first key must be set"),
                    l1_offset,
                ));

                // The just-rejected entry becomes the first of the new L1
                // block. A single index entry should always fit in a fresh
                // L1 block; if not, the L1 target is misconfigured.
                if !current_l1.add(&entry_key, &entry_val) {
                    return Err(WriterError::InvalidData(
                        "A single index entry exceeds L1_INDEX_TARGET_BLOCK_SIZE"
                            .to_string(),
                    ));
                }
                current_l1_first_key = Some(first_key.clone());
            } else if current_l1_first_key.is_none() {
                current_l1_first_key = Some(first_key.clone());
            }
        }

        // Flush the trailing partial L1 block (if any entries are buffered).
        if !current_l1.is_empty() {
            let l1_data = current_l1.build();
            let l1_offset = self.current_offset;
            self.file.write_all(&l1_data)?;
            self.current_offset += l1_data.len() as u64;
            l1_index.push((
                current_l1_first_key
                    .take()
                    .expect("non-empty L1 block has a first key"),
                l1_offset,
            ));
        }

        // Build and write the top index. Each entry maps the first key of
        // an L1 block to that block's offset.
        let mut top = BlockBuilder::with_target_size(TOP_INDEX_TARGET_BLOCK_SIZE);
        for (first_key, l1_offset) in &l1_index {
            let key = InternalKey {
                user_key: first_key.clone(),
                seq_num: 0,
            };
            let val = Record::Put(l1_offset.to_be_bytes().to_vec());
            if !top.add(&key, &val) {
                return Err(WriterError::InvalidData(
                    "Top index exceeded TOP_INDEX_TARGET_BLOCK_SIZE; \
                     a third level is needed for SSTables this large"
                        .to_string(),
                ));
            }
        }
        let top_data = top.build();
        let top_offset = self.current_offset;
        self.file.write_all(&top_data)?;
        self.current_offset += top_data.len() as u64;

        Ok(top_offset)
    }

    /// Helper to write the current block to disk and record its location in the index.
    fn finish_current_block(&mut self) -> Result<(), WriterError> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        // Retrieve and reset the first key gracefully
        let first_key = self.first_key_of_current_block.take().ok_or_else(|| {
            WriterError::InvalidData(
                "Internal error: Block is not empty but lacks a tracked first key.".to_string(),
            )
        })?;

        // Save to our in-memory index builder
        self.block_index.push((first_key, self.current_offset));

        // Swap out the current block for a fresh one
        let old_block = std::mem::replace(&mut self.current_block, BlockBuilder::new());
        let block_data = old_block.build();

        // Write the bytes and update offset
        self.file.write_all(&block_data)?;
        self.current_offset += block_data.len() as u64;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::MagicNumber;
    use crate::sstable::{MetaOffset, IndexOffset};
    use crate::sstable::block::Block;
    use std::fs;
    use std::path::PathBuf;

    // =========================================================================
    // Test Helpers
    // =========================================================================

    /// A mock iterator to feed data into our SsTableBuilder
    struct MockIterator {
        entries: std::vec::IntoIter<(String, Record, u64)>,
        /// Holds the element that is queued to be returned by the next `next()` call
        current: Option<(String, Record, u64)>,
    }

    impl MockIterator {
        fn new(entries: Vec<(String, Record, u64)>) -> Self {
            let mut entries_iter = entries.into_iter();
            // Pre-fetch the first element to initialize `current`
            let current = entries_iter.next();
            Self {
                entries: entries_iter,
                current,
            }
        }
    }

    impl KvIterator for MockIterator {
        /// Checks if the iterator still has elements without advancing it
        fn is_valid(&self) -> bool {
            self.current.is_some()
        }

        /// Returns the current element and advances the iterator
        fn next(&mut self) -> Option<(String, Record, u64)> {
            // Take the currently queued item, leaving `None` in its place
            let item_to_return = self.current.take();
            // Immediately queue up the next item for future `is_valid` checks
            self.current = self.entries.next();

            item_to_return
        }
    }

    /// Automatically deletes the test file when it goes out of scope
    struct TempFileGuard {
        path: PathBuf,
    }

    impl TempFileGuard {
        fn new(name: &str) -> Self {
            let mut path = std::env::temp_dir();
            path.push(name);
            // Clean up any lingering file from a previous aborted run
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

    /// Parses the footer from raw SSTable bytes, returning (meta_offset, index_offset).
    fn parse_footer(data: &[u8]) -> (usize, usize) {
        let len = data.len();
        let footer_start = len - FOOTER_SIZE;

        let meta_offset = MetaOffset::from_be_bytes(
            data[footer_start..footer_start + META_OFFSET_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        let index_offset = IndexOffset::from_be_bytes(
            data[footer_start + META_OFFSET_SIZE..footer_start + META_OFFSET_SIZE + INDEX_OFFSET_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        (meta_offset, index_offset)
    }

    // =========================================================================
    // SsTableBuilder Tests (4 Tests)
    // =========================================================================

    #[test]
    fn test_builder_invalid_path_returns_error() {
        // Attempt to write to a root directory (which will fail) or an invalid path
        let result = SsTableBuilder::new("./this_directory_does_not_exist/test.sst");
        assert!(
            result.is_err(),
            "Builder should return an IO error for invalid paths"
        );

        if let Err(WriterError::Io(_)) = result {
            // Expected
        } else {
            panic!("Expected Io error variant");
        }
    }

    #[test]
    fn test_builder_empty_iterator() {
        let file = TempFileGuard::new("empty_test.sst");
        let mut builder = SsTableBuilder::new(file.path_str()).unwrap();

        let iter = Box::new(MockIterator::new(vec![]));
        builder.build_from_iterator(iter).unwrap();

        // Verify File Contents
        let data = fs::read(&file.path).unwrap();

        // Even empty, it should have a Meta Block + empty Index Block + Footer
        assert!(data.len() >= FOOTER_SIZE);

        let len = data.len();
        let magic_start = len - MAGIC_SIZE;
        let magic = MagicNumber::from_be_bytes(data[magic_start..len].try_into().unwrap());
        assert_eq!(
            magic, MAGIC_NUMBER,
            "Footer magic number is missing/corrupted"
        );

        let (meta_offset, index_offset) = parse_footer(&data);
        assert_eq!(
            meta_offset, 0,
            "Since there are no data blocks, meta offset should be 0"
        );

        // Decode the index block
        let index_block_data = data[index_offset..len - FOOTER_SIZE].to_vec();
        let index_block = Block::decode(index_block_data);
        assert_eq!(
            index_block.get_num_offsets().unwrap(),
            0,
            "Index block should be empty"
        );
    }

    /// Decode an index entry's `Record::Put` value as a u64 offset.
    fn decode_offset_record(record: Record) -> u64 {
        match record {
            Record::Put(val) => u64::from_be_bytes(val.try_into().unwrap()),
            Record::Delete => panic!("Index block should not contain deletes"),
        }
    }

    #[test]
    fn test_builder_single_block() {
        let file = TempFileGuard::new("single_block_test.sst");
        let mut builder = SsTableBuilder::new(file.path_str()).unwrap();

        // 3 entries will easily fit inside the 4KB TARGET_BLOCK_SIZE
        let entries = vec![
            ("apple".to_string(), Record::Put(b("red")), 3),
            ("banana".to_string(), Record::Put(b("yellow")), 2),
            ("cherry".to_string(), Record::Delete, 1),
        ];

        let iter = Box::new(MockIterator::new(entries));
        builder.build_from_iterator(iter).unwrap();

        // --- Verification: navigate the two-level index ---
        let data = fs::read(&file.path).unwrap();
        let len = data.len();

        let (meta_offset, top_index_offset) = parse_footer(&data);

        // 1. Top index: exactly 1 L1 block.
        let top_data = data[top_index_offset..len - FOOTER_SIZE].to_vec();
        let top_index = Block::decode(top_data);
        assert_eq!(
            top_index.get_num_offsets().unwrap(),
            1,
            "Top index should contain exactly 1 L1 pointer"
        );
        let (top_key, top_record) = top_index
            .decode_entry(top_index.get_offset(0, 1).unwrap())
            .unwrap();
        assert_eq!(top_key.user_key, "apple", "Top index opens with apple");
        let l1_start = decode_offset_record(top_record) as usize;

        // 2. L1 block (between meta block and top index, opens at l1_start).
        let l1_end = top_index_offset;
        let l1_data = data[l1_start..l1_end].to_vec();
        let l1_block = Block::decode(l1_data);
        assert_eq!(
            l1_block.get_num_offsets().unwrap(),
            1,
            "L1 should contain exactly 1 data-block pointer"
        );
        let (l1_key, l1_record) = l1_block
            .decode_entry(l1_block.get_offset(0, 1).unwrap())
            .unwrap();
        assert_eq!(l1_key.user_key, "apple", "L1 entry opens with apple");
        assert_eq!(
            decode_offset_record(l1_record),
            0,
            "First data block should start at byte 0"
        );

        // 3. Data block (data blocks end where the meta block begins).
        let data_block_bytes = data[0..meta_offset].to_vec();
        let data_block = Block::decode(data_block_bytes);
        assert_eq!(data_block.get_num_offsets().unwrap(), 3);

        let (cherry_k, cherry_r) = data_block.search("cherry").unwrap().unwrap();
        assert_eq!(cherry_k.seq_num, 1);
        assert!(matches!(cherry_r, Record::Delete));
    }

    #[test]
    fn test_builder_multiple_blocks() {
        let file = TempFileGuard::new("multi_block_test.sst");
        let mut builder = SsTableBuilder::new(file.path_str()).unwrap();

        let mut entries = Vec::new();
        // Create 10 entries, each with a 1000 byte value.
        // This guarantees we exceed the 4096 byte TARGET_BLOCK_SIZE multiple times.
        for i in 0..10 {
            let key = format!("key_{:02}", i); // key_00, key_01...
            let val = vec![i as u8; 1000];
            entries.push((key, Record::Put(val), 100 - i));
        }

        let iter = Box::new(MockIterator::new(entries));
        builder.build_from_iterator(iter).unwrap();

        // --- Verification: walk top → L1 → data ---
        let data = fs::read(&file.path).unwrap();
        let len = data.len();

        let (meta_offset, top_index_offset) = parse_footer(&data);

        // Top index → collect (l1_start, l1_end) ranges.
        let top_data = data[top_index_offset..len - FOOTER_SIZE].to_vec();
        let top_index = Block::decode(top_data);
        let n_l1 = top_index.get_num_offsets().unwrap();
        assert!(n_l1 >= 1);
        let mut l1_ranges: Vec<(usize, usize)> = Vec::with_capacity(n_l1);
        for i in 0..n_l1 {
            let entry_off = top_index.get_offset(i, n_l1).unwrap();
            let (_, rec) = top_index.decode_entry(entry_off).unwrap();
            let start = decode_offset_record(rec) as usize;
            let end = if i + 1 < n_l1 {
                let next_off = top_index.get_offset(i + 1, n_l1).unwrap();
                let (_, next_rec) = top_index.decode_entry(next_off).unwrap();
                decode_offset_record(next_rec) as usize
            } else {
                top_index_offset
            };
            l1_ranges.push((start, end));
        }

        // Across all L1 blocks → collect the flat list of data-block ranges.
        let mut data_block_starts: Vec<usize> = Vec::new();
        let mut data_block_first_keys: Vec<String> = Vec::new();
        for (l1_start, l1_end) in &l1_ranges {
            let l1_data = data[*l1_start..*l1_end].to_vec();
            let l1_block = Block::decode(l1_data);
            let n = l1_block.get_num_offsets().unwrap();
            for j in 0..n {
                let entry_off = l1_block.get_offset(j, n).unwrap();
                let (idx_key, idx_rec) = l1_block.decode_entry(entry_off).unwrap();
                data_block_starts.push(decode_offset_record(idx_rec) as usize);
                data_block_first_keys.push(idx_key.user_key);
            }
        }

        assert!(
            data_block_starts.len() > 1,
            "Builder should have created multiple data blocks"
        );

        // Walk every data block, confirm its first key matches what the
        // index claims.
        for i in 0..data_block_starts.len() {
            let block_start = data_block_starts[i];
            let block_end = if i + 1 < data_block_starts.len() {
                data_block_starts[i + 1]
            } else {
                meta_offset
            };

            let data_block_bytes = data[block_start..block_end].to_vec();
            let data_block = Block::decode(data_block_bytes);
            let first_key_offset = data_block
                .get_offset(0, data_block.get_num_offsets().unwrap())
                .unwrap();
            let (actual_first_key, _) = data_block.decode_entry(first_key_offset).unwrap();

            assert_eq!(
                data_block_first_keys[i], actual_first_key.user_key,
                "Index key does not match actual first key of data block {}",
                i
            );
        }
    }

    /// Files built with `add_entry` + `finish_file` must be readable by
    /// `SsTableReader::open` and return correct results from `search`.
    #[test]
    fn test_finish_file_readable_by_reader() {
        use crate::sstable::reader::SsTableReader;

        let file = TempFileGuard::new("finish_file_reader.sst");
        let mut builder = SsTableBuilder::new(file.path_str()).unwrap();

        // Write entries via add_entry (sorted by user_key asc, seq_num desc).
        builder.add_entry("apple",  &Record::Put(b("red")),    10).unwrap();
        builder.add_entry("apple",  &Record::Put(b("green")),   5).unwrap();
        builder.add_entry("banana", &Record::Put(b("yellow")),  8).unwrap();
        builder.add_entry("cherry", &Record::Delete,             3).unwrap();
        builder.add_entry("date",   &Record::Put(b("brown")),   1).unwrap();

        let summary = builder.finish_file().unwrap();
        assert_eq!(
            summary,
            Some(("apple".to_string(), "date".to_string(), 10)),
            "finish_file should return the correct key range and max seq_num",
        );

        // The file must be openable by SsTableReader.
        let mut reader = SsTableReader::open(file.path_str()).unwrap();

        // Put: returns the newest version.
        let (key, record) = reader.search("apple").unwrap().unwrap();
        assert_eq!(key.user_key, "apple");
        assert_eq!(key.seq_num, 10);
        assert_eq!(record, Record::Put(b("red")));

        // Put: single version.
        let (key, record) = reader.search("banana").unwrap().unwrap();
        assert_eq!(key.user_key, "banana");
        assert_eq!(record, Record::Put(b("yellow")));

        // Delete tombstone.
        let (key, record) = reader.search("cherry").unwrap().unwrap();
        assert_eq!(key.user_key, "cherry");
        assert!(matches!(record, Record::Delete));

        // Last key.
        let (key, record) = reader.search("date").unwrap().unwrap();
        assert_eq!(key.user_key, "date");
        assert_eq!(record, Record::Put(b("brown")));

        // Missing key: bloom filter should reject.
        assert!(reader.search("fig").unwrap().is_none());
    }

    #[test]
    fn test_builder_oversized_block_creation() {
        let file = TempFileGuard::new("writer_oversized_block.sst");

        // 1. Create a massive 10KB value, which far exceeds a standard 4KB block size limit.
        let large_val_size = 10 * 1024;
        let large_val = vec![7u8; large_val_size];

        let entries = vec![
            ("a_small".to_string(), Record::Put(b("tiny").to_vec()), 3),
            ("b_massive".to_string(), Record::Put(large_val), 2),
            ("c_small".to_string(), Record::Put(b("tiny2").to_vec()), 1),
        ];

        let iter = Box::new(MockIterator::new(entries));
        let mut builder = SsTableBuilder::new(file.path_str()).expect("Failed to create builder");

        // 2. The builder should handle the oversized block gracefully without panicking
        // or throwing a chunking/buffer overflow error.
        let build_result = builder.build_from_iterator(iter);
        assert!(
            build_result.is_ok(),
            "Builder failed to write the oversized block"
        );

        // 3. Verify the file was actually written to disk and has a sane size.
        // It must be at least as large as our 10KB payload, plus the index, footer, and other keys.
        let metadata = std::fs::metadata(file.path_str()).expect("Failed to read file metadata");
        let file_size = metadata.len();

        assert!(
            file_size > large_val_size as u64,
            "File size ({} bytes) is too small to contain the oversized block!",
            file_size
        );

        // The file should be roughly:
        // Block 1 (small) + Block 2 (10KB) + Block 3 (small) + Meta + Index + Footer.
        // The bloom filter adds ~12KB, so the total should be under 35KB.
        assert!(
            file_size < 35 * 1024,
            "File size ({} bytes) is unusually large, possible runaway allocation",
            file_size
        );
    }

    /// An SSTable with enough distinct keys to force the L1 index to spill
    /// across multiple blocks must still round-trip cleanly through
    /// `SsTableReader::search`. Pre-multi-level this would fail with
    /// "Index block exceeded INDEX_TARGET_BLOCK_SIZE".
    #[test]
    fn sstable_round_trip_multi_l1_index() {
        use crate::sstable::reader::SsTableReader;

        const NUM_KEYS: u64 = 30_000;
        const VALUE_SIZE: usize = 256;

        let file = TempFileGuard::new("multi_l1_round_trip.sst");
        let mut builder = SsTableBuilder::new(file.path_str()).unwrap();

        let entries: Vec<(String, Record, u64)> = (0..NUM_KEYS)
            .map(|i| {
                let key = format!("key_{:020}", i);
                let val = vec![(i % 256) as u8; VALUE_SIZE];
                (key, Record::Put(val), NUM_KEYS - i)
            })
            .collect();

        builder
            .build_from_iterator(Box::new(MockIterator::new(entries)))
            .unwrap();

        // Verify structurally: walk the top index and confirm there's more
        // than one L1 block (which is what proves the multi-level path
        // is being exercised).
        let raw = fs::read(&file.path).unwrap();
        let (_meta_offset, top_index_offset) = parse_footer(&raw);
        let top_data = raw[top_index_offset..raw.len() - FOOTER_SIZE].to_vec();
        let top_index = Block::decode(top_data);
        let n_l1 = top_index.get_num_offsets().unwrap();
        assert!(
            n_l1 > 1,
            "Expected multiple L1 index blocks for {} keys, got {}",
            NUM_KEYS,
            n_l1,
        );

        // Reads succeed across the whole key range (first, middle, last).
        let mut reader = SsTableReader::open(file.path_str()).unwrap();
        for sample_i in [0u64, NUM_KEYS / 2, NUM_KEYS - 1] {
            let key = format!("key_{:020}", sample_i);
            let got = reader.search(&key).unwrap();
            let (k, r) = got.unwrap_or_else(|| panic!("missing {}", key));
            assert_eq!(k.user_key, key);
            assert_eq!(k.seq_num, NUM_KEYS - sample_i);
            assert!(matches!(r, Record::Put(v) if v.len() == VALUE_SIZE));
        }

        // A key absent from the SSTable returns None (bloom + structural).
        assert!(reader.search("key_99999999999999999999").unwrap().is_none());
    }
}
