// writer.rs
use std::fs::File;
use std::io::Write;

use crate::core::{InternalKey, KvIterator, Record};
use crate::sstable::block::BlockBuilder;
use crate::sstable::{
    FOOTER_SIZE, INDEX_OFFSET_SIZE, IndexOffset, MAGIC_NUMBER, MAGIC_SIZE,
};

#[derive(thiserror::Error, Debug)]
pub enum WriterError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid data: {0}")]
    InvalidData(String),
}

pub struct SsTableBuilder {
    file: File,
    current_block: BlockBuilder,
    /// Stores the first key of each block and the block's physical byte offset in the file
    block_index: Vec<(String, u64)>,
    current_offset: IndexOffset,
    /// Tracks the first user_key added to the current block
    first_key_of_current_block: Option<String>,
}

impl SsTableBuilder {
    pub fn new(path: &str) -> Result<Self, WriterError> {
        let file = File::create(path)?;
        Ok(Self {
            file,
            current_block: BlockBuilder::new(),
            block_index: Vec::new(),
            current_offset: 0,
            first_key_of_current_block: None,
        })
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

            // If this is the first entry in a block, save it for the index
            if self.first_key_of_current_block.is_none() {
                self.first_key_of_current_block = Some(user_key);
            }
        }

        // Flush the final data block if it has anything in it
        if !self.current_block.is_empty() {
            self.finish_current_block()?;
        }

        // Build the Index Block
        let mut index_block = BlockBuilder::new();
        for (first_key, offset) in &self.block_index {
            let index_key = InternalKey {
                user_key: first_key.clone(),
                seq_num: 0, // Dummy seq_num for index entries
            };
            let index_record = Record::Put(offset.to_be_bytes().to_vec());

            let added = index_block.add(&index_key, &index_record);
            if !added {
                return Err(WriterError::InvalidData(
                    "Index block exceeded TARGET_BLOCK_SIZE. Multi-level index implementation required.".to_string()
                ));
            }
        }

        // Write the Index Block
        let index_data = index_block.build();
        let index_offset = self.current_offset;

        self.file.write_all(&index_data)?;
        self.current_offset += index_data.len() as u64;

        // Write a fixed-size Footer (16 bytes)
        let mut footer = [0u8; FOOTER_SIZE];
        let index_end = INDEX_OFFSET_SIZE;
        let magic_end = index_end + MAGIC_SIZE;

        footer[0..index_end].copy_from_slice(&index_offset.to_be_bytes());
        footer[index_end..magic_end].copy_from_slice(&MAGIC_NUMBER.to_be_bytes());

        self.file.write_all(&footer)?;
        self.file.sync_all()?;

        Ok(())
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

        // Even empty, it should have an empty Index Block + 16 byte Footer
        assert!(data.len() >= FOOTER_SIZE);

        let len = data.len();
        let magic = u64::from_be_bytes(data[len - 8..len].try_into().unwrap());
        assert_eq!(
            magic, 0xDEADBEEFCAFEBABEu64,
            "Footer magic number is missing/corrupted"
        );

        let index_offset =
            u64::from_be_bytes(data[len - FOOTER_SIZE..len - 8].try_into().unwrap()) as usize;
        assert_eq!(
            index_offset, 0,
            "Since there are no data blocks, index offset should be 0"
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

        // --- Verification ---
        let data = fs::read(&file.path).unwrap();
        let len = data.len();

        let index_offset =
            u64::from_be_bytes(data[len - FOOTER_SIZE..len - 8].try_into().unwrap()) as usize;

        // 1. Verify Index Block
        let index_data = data[index_offset..len - FOOTER_SIZE].to_vec();
        let index_block = Block::decode(index_data);

        assert_eq!(
            index_block.get_num_offsets().unwrap(),
            1,
            "There should be exactly 1 data block indexed"
        );
        let offset_index_0 = index_block.get_offset(0, 1).unwrap();
        let (first_key_entry, pointer_record) = index_block.decode_entry(offset_index_0).unwrap();

        assert_eq!(
            first_key_entry.user_key, "apple",
            "Index should track the first key of the block"
        );

        // Extract the physical offset to the data block
        let data_block_offset = match pointer_record {
            Record::Put(val) => u64::from_be_bytes(val.try_into().unwrap()) as usize,
            Record::Delete => panic!("Index block should not contain deletes"),
        };
        assert_eq!(
            data_block_offset, 0,
            "The first data block should start at byte 0"
        );

        // 2. Verify Data Block
        let data_block_bytes = data[0..index_offset].to_vec();
        let data_block = Block::decode(data_block_bytes);

        assert_eq!(
            data_block.get_num_offsets().unwrap(),
            3,
            "Data block should contain 3 entries"
        );

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

        // --- Verification ---
        let data = fs::read(&file.path).unwrap();
        let len = data.len();

        let index_offset =
            u64::from_be_bytes(data[len - FOOTER_SIZE..len - 8].try_into().unwrap()) as usize;

        // 1. Verify Index Block has multiple pointers
        let index_data = data[index_offset..len - FOOTER_SIZE].to_vec();
        let index_block = Block::decode(index_data);

        let num_data_blocks = index_block.get_num_offsets().unwrap();
        assert!(
            num_data_blocks > 1,
            "Builder should have created multiple data blocks"
        );

        // 2. Walk through the index and verify EVERY Data Block
        for i in 0..num_data_blocks {
            let idx_entry_offset = index_block.get_offset(i, num_data_blocks).unwrap();
            let (idx_key, pointer_record) = index_block.decode_entry(idx_entry_offset).unwrap();

            let block_start = match pointer_record {
                Record::Put(val) => u64::from_be_bytes(val.try_into().unwrap()) as usize,
                Record::Delete => panic!("Index contains a Delete"),
            };

            // The block ends where the next block begins, or where the index block begins
            let block_end = if i + 1 < num_data_blocks {
                let next_idx_entry_offset = index_block.get_offset(i + 1, num_data_blocks).unwrap();
                let (_, next_pointer_record) =
                    index_block.decode_entry(next_idx_entry_offset).unwrap();
                match next_pointer_record {
                    Record::Put(val) => u64::from_be_bytes(val.try_into().unwrap()) as usize,
                    _ => unreachable!(),
                }
            } else {
                index_offset
            };

            let data_block_bytes = data[block_start..block_end].to_vec();
            let data_block = Block::decode(data_block_bytes);

            // Ensure the data block is valid and its first key matches what the index block claims!
            let first_key_offset = data_block
                .get_offset(0, data_block.get_num_offsets().unwrap())
                .unwrap();
            let (actual_first_key, _) = data_block.decode_entry(first_key_offset).unwrap();

            assert_eq!(
                idx_key.user_key, actual_first_key.user_key,
                "Index block key does not match actual first key of data block {}",
                i
            );
        }
    }
}
