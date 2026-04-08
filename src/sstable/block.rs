use std::mem::size_of;

use crate::core::{CoreError, InternalKey, Record, RecordTag};

const TARGET_BLOCK_SIZE: usize = 4096;

// --- Binary Layout Types ---
pub type KeyLen = u16;
pub type ValueLen = u32;
pub type Offset = u16;

// --- Compile-Time Size Constants ---
const KEY_LEN_SIZE: usize = size_of::<KeyLen>();
const VALUE_LEN_SIZE: usize = size_of::<ValueLen>();
const OFFSET_SIZE: usize = size_of::<Offset>();
const SEQ_NUM_SIZE: usize = size_of::<u64>();
const RECORD_TYPE_SIZE: usize = size_of::<RecordTag>();

// --- Error Handling ---

#[derive(thiserror::Error, Debug)]
pub enum BlockError {
    #[error("Corrupt block data: {0}")]
    CorruptData(String),

    #[error("Invalid UTF-8 in key: {0}")]
    InvalidUtf8(std::str::Utf8Error),
}

impl From<std::str::Utf8Error> for BlockError {
    fn from(err: std::str::Utf8Error) -> Self {
        BlockError::InvalidUtf8(err)
    }
}

impl From<CoreError> for BlockError {
    fn from(err: CoreError) -> Self {
        match err {
            CoreError::CorruptData(msg) => BlockError::CorruptData(msg),
        }
    }
}

/// Incrementally serializes key-record pairs into a single block.
///
/// Entries are appended in sorted order. The builder tracks the estimated
/// size and rejects new entries (returning `false` from `add`) once the
/// block would exceed `TARGET_BLOCK_SIZE` (4 KB). Calling `build` finalizes
/// the block by appending the offset array and entry count footer.
pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<Offset>,
}

impl BlockBuilder {
    /// Creates a new, empty `BlockBuilder` ready to accept entries.
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Serializes and appends a key-record pair to the block.
    ///
    /// Returns `true` if the entry was added, or `false` if adding it would
    /// exceed `TARGET_BLOCK_SIZE`. The first entry is always accepted
    /// regardless of size.
    pub fn add(&mut self, key: &InternalKey, record: &Record) -> bool {
        let key_bytes = key.user_key.as_bytes();
        let key_len = key_bytes.len() as KeyLen;

        let mut entry_size = KEY_LEN_SIZE + key_bytes.len() + SEQ_NUM_SIZE + RECORD_TYPE_SIZE;
        match record {
            Record::Put(val) => entry_size += VALUE_LEN_SIZE + val.len(),
            Record::Delete => {}
        }

        let estimated_size =
            self.data.len() + entry_size + (self.offsets.len() + 1) * OFFSET_SIZE + OFFSET_SIZE;

        if !self.is_empty() && estimated_size > TARGET_BLOCK_SIZE {
            return false;
        }

        self.offsets.push(self.data.len() as Offset);

        // Serialize InternalKey
        self.data.extend_from_slice(&key_len.to_be_bytes());
        self.data.extend_from_slice(key_bytes);
        self.data.extend_from_slice(&key.seq_num.to_be_bytes());

        // Serialize Record
        let tag = record.tag() as u8;
        self.data.push(tag);

        if let Record::Put(val) = record {
            let val_len = val.len() as ValueLen;
            self.data.extend_from_slice(&val_len.to_be_bytes());
            self.data.extend_from_slice(val);
        }

        true
    }

    /// Returns `true` if no entries have been added to the builder yet.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Consumes the builder and produces the final block bytes.
    ///
    /// Appends the offset array and entry count footer after the serialized
    /// entry data, yielding a complete block ready to be written to disk.
    pub fn build(mut self) -> Vec<u8> {
        for offset in &self.offsets {
            self.data.extend_from_slice(&offset.to_be_bytes());
        }

        let num_offsets = self.offsets.len() as Offset;
        self.data.extend_from_slice(&num_offsets.to_be_bytes());

        self.data
    }
}

/// A read-only view over a serialized block of key-record entries.
///
/// The binary layout is: serialized entries, followed by an array of `Offset`
/// values pointing to each entry, followed by a `u16` entry count footer.
/// Supports random access via `decode_entry` and binary search via `search`.
pub struct Block {
    data: Vec<u8>,
}

impl Block {
    /// Wraps raw bytes into a `Block` for reading. No validation is performed
    /// at construction time; errors surface when entries are accessed.
    pub fn decode(data: Vec<u8>) -> Self {
        Self { data }
    }

    // --- Helper Methods ---

    /// Reads the entry count from the block footer. Returns `0` if the block
    /// is too small to contain a valid footer.
    pub fn get_num_offsets(&self) -> Result<usize, BlockError> {
        if self.data.len() < OFFSET_SIZE {
            return Ok(0);
        }
        let footer_start = self.data.len() - OFFSET_SIZE;

        let mut bytes = [0u8; OFFSET_SIZE];
        bytes.copy_from_slice(&self.data[footer_start..footer_start + OFFSET_SIZE]);
        Ok(Offset::from_be_bytes(bytes) as usize)
    }

    /// Returns the byte offset of the `index`-th entry within the block's
    /// data section. `num_offsets` must match the value from `get_num_offsets`.
    pub fn get_offset(&self, index: usize, num_offsets: usize) -> Result<usize, BlockError> {
        let offsets_total_size = num_offsets * OFFSET_SIZE;
        if self.data.len() < OFFSET_SIZE + offsets_total_size {
            return Err(BlockError::CorruptData(
                "Block too small to contain offset array".into(),
            ));
        }

        let offsets_start = self.data.len() - OFFSET_SIZE - offsets_total_size;
        let pos = offsets_start + (index * OFFSET_SIZE);

        if pos + OFFSET_SIZE > self.data.len() {
            return Err(BlockError::CorruptData("Offset index out of bounds".into()));
        }

        let mut bytes = [0u8; OFFSET_SIZE];
        bytes.copy_from_slice(&self.data[pos..pos + OFFSET_SIZE]);
        Ok(Offset::from_be_bytes(bytes) as usize)
    }

    /// Reads only the user key string at the given byte offset without
    /// parsing the rest of the entry. Used by binary search to compare keys
    /// cheaply.
    fn peek_user_key(&self, offset: usize) -> Result<&str, BlockError> {
        if offset + KEY_LEN_SIZE > self.data.len() {
            return Err(BlockError::CorruptData(
                "Offset out of bounds reading key length".into(),
            ));
        }

        let mut len_bytes = [0u8; KEY_LEN_SIZE];
        len_bytes.copy_from_slice(&self.data[offset..offset + KEY_LEN_SIZE]);
        let key_len = KeyLen::from_be_bytes(len_bytes) as usize;

        let key_start = offset + KEY_LEN_SIZE;
        if key_start + key_len > self.data.len() {
            return Err(BlockError::CorruptData(
                "Parsed key length exceeds block boundaries".into(),
            ));
        }

        let key_str = std::str::from_utf8(&self.data[key_start..key_start + key_len])?;
        Ok(key_str)
    }

    /// Fully deserializes the entry at the given byte offset, returning
    /// the `InternalKey` (user key + sequence number) and the `Record`
    /// (either a `Put` with its value or a `Delete` tombstone).
    pub fn decode_entry(&self, offset: usize) -> Result<(InternalKey, Record), BlockError> {
        // Parse User Key length
        if offset + KEY_LEN_SIZE > self.data.len() {
            return Err(BlockError::CorruptData(
                "Offset out of bounds reading key length".into(),
            ));
        }
        let mut len_bytes = [0u8; KEY_LEN_SIZE];
        len_bytes.copy_from_slice(&self.data[offset..offset + KEY_LEN_SIZE]);
        let key_len = KeyLen::from_be_bytes(len_bytes) as usize;

        // Parse User Key string
        let key_start = offset + KEY_LEN_SIZE;
        if key_start + key_len > self.data.len() {
            return Err(BlockError::CorruptData(
                "Key length exceeds block boundaries".into(),
            ));
        }
        let user_key = std::str::from_utf8(&self.data[key_start..key_start + key_len])?.to_string();

        // Parse Sequence Number
        let seq_start = key_start + key_len;
        if seq_start + SEQ_NUM_SIZE + RECORD_TYPE_SIZE > self.data.len() {
            return Err(BlockError::CorruptData(
                "Unexpected end of block reading sequence/tag".into(),
            ));
        }
        let mut seq_bytes = [0u8; SEQ_NUM_SIZE];
        seq_bytes.copy_from_slice(&self.data[seq_start..seq_start + SEQ_NUM_SIZE]);
        let seq_num = u64::from_be_bytes(seq_bytes);

        // Parse Record Type
        let record_type_pos = seq_start + SEQ_NUM_SIZE;
        let tag_byte = self.data[record_type_pos];
        let tag = RecordTag::try_from(tag_byte)?; // Uses the mapped TryFrom error

        let record = match tag {
            RecordTag::Put => {
                let val_len_start = record_type_pos + RECORD_TYPE_SIZE;
                if val_len_start + VALUE_LEN_SIZE > self.data.len() {
                    return Err(BlockError::CorruptData(
                        "Unexpected end of block reading value length".into(),
                    ));
                }

                let mut val_len_bytes = [0u8; VALUE_LEN_SIZE];
                val_len_bytes
                    .copy_from_slice(&self.data[val_len_start..val_len_start + VALUE_LEN_SIZE]);
                let val_len = ValueLen::from_be_bytes(val_len_bytes) as usize;

                let val_start = val_len_start + VALUE_LEN_SIZE;
                if val_start + val_len > self.data.len() {
                    return Err(BlockError::CorruptData(
                        "Parsed value length exceeds block boundaries".into(),
                    ));
                }

                Record::Put(self.data[val_start..val_start + val_len].to_vec())
            }
            RecordTag::Delete => Record::Delete,
        };

        Ok((InternalKey { user_key, seq_num }, record))
    }

    // --- The Core Search ---

    /// Binary-searches the block for `target_key` and returns the newest
    /// version (highest sequence number) if found. Returns `Ok(None)` if the
    /// key is not present in this block.
    pub fn search(&self, target_key: &str) -> Result<Option<(InternalKey, Record)>, BlockError> {
        let num_offsets = self.get_num_offsets()?;
        if num_offsets == 0 {
            return Ok(None);
        }

        let mut left = 0;
        let mut right = num_offsets;

        while left < right {
            let mid = left + (right - left) / 2;
            let offset = self.get_offset(mid, num_offsets)?;
            let mid_key = self.peek_user_key(offset)?;

            if mid_key >= target_key {
                right = mid;
            } else {
                left = mid + 1;
            }
        }

        if left < num_offsets {
            let offset = self.get_offset(left, num_offsets)?;
            let first_key = self.peek_user_key(offset)?;

            if first_key == target_key {
                let entry = self.decode_entry(offset)?;
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Helper Functions ---
    fn b(str: &str) -> Vec<u8> {
        str.as_bytes().to_vec()
    }

    fn make_key(key: &str, seq_num: u64) -> InternalKey {
        InternalKey {
            user_key: key.to_string(),
            seq_num,
        }
    }

    /// Helper to quickly spin up a Block with specific entries for testing
    fn build_test_block() -> Block {
        let mut builder = BlockBuilder::new();
        builder.add(&make_key("apple", 10), &Record::Put(b("red")));
        builder.add(&make_key("banana", 5), &Record::Delete);
        Block::decode(builder.build())
    }

    // =========================================================================
    // BlockBuilder Tests (5 Tests)
    // =========================================================================

    #[test]
    fn test_block_builder_initially_empty() {
        let builder = BlockBuilder::new();
        assert!(builder.is_empty());
    }

    #[test]
    fn test_block_builder_add_single_put() {
        let mut builder = BlockBuilder::new();
        let key = make_key("key1", 1);
        let record = Record::Put(b("value1"));

        let added = builder.add(&key, &record);
        assert!(added);
        assert!(!builder.is_empty());

        // Basic sanity check on the output size.
        // It shouldn't be empty, but it shouldn't be 4KB yet.
        let block_bytes = builder.build();
        assert!(block_bytes.len() > 0);
        assert!(block_bytes.len() < TARGET_BLOCK_SIZE);
    }

    #[test]
    fn test_block_builder_add_single_delete() {
        let mut builder = BlockBuilder::new();
        let key = make_key("key1", 1);
        let record = Record::Delete;

        assert!(builder.add(&key, &record));
        let block_bytes = builder.build();

        // Delete records are smaller than Put records since they lack a value payload.
        assert!(block_bytes.len() > 0);
    }

    #[test]
    fn test_block_builder_respects_size_limit() {
        let mut builder = BlockBuilder::new();

        // Target block size is 4096. Let's create a massive 4000-byte value.
        let big_val = vec![0u8; 4000];
        let key1 = make_key("massive_key", 1);

        // 1. The first massive key should succeed because the block is empty,
        // and we always allow at least one entry even if it skirts the limit.
        assert!(builder.add(&key1, &Record::Put(big_val)));

        // 2. Now the block has ~4000+ bytes in it. Adding another moderate size key
        // should calculate an estimated size > 4096 and reject it.
        let key2 = make_key("straw_that_broke_the_camels_back", 2);
        let moderate_val = vec![0u8; 200];

        let added = builder.add(&key2, &Record::Put(moderate_val));
        assert!(
            !added,
            "Builder failed to enforce the TARGET_BLOCK_SIZE limit!"
        );
    }

    #[test]
    fn test_block_builder_multiple_entries() {
        let mut builder = BlockBuilder::new();
        assert!(builder.add(&make_key("a", 1), &Record::Put(b("val_a"))));
        assert!(builder.add(&make_key("b", 2), &Record::Delete));
        assert!(builder.add(&make_key("c", 3), &Record::Put(b("val_c"))));

        let bytes = builder.build();
        // A block with 3 entries should decode properly
        let block = Block::decode(bytes);
        assert_eq!(block.get_num_offsets().unwrap(), 3);
    }

    // =========================================================================
    // Block Internal Method Tests
    // =========================================================================

    #[test]
    fn test_block_decode() {
        let data = vec![1, 2, 3, 4];
        let block = Block::decode(data.clone());
        assert_eq!(block.data, data);
    }

    #[test]
    fn test_block_get_num_offsets() {
        // 1. Empty block (less than OFFSET_SIZE bytes)
        let empty_block = Block::decode(vec![]);
        assert_eq!(empty_block.get_num_offsets().unwrap(), 0);

        // 2. Block with just a 0 offset footer
        let zero_offset_block = Block::decode(vec![0, 0]);
        assert_eq!(zero_offset_block.get_num_offsets().unwrap(), 0);

        // 3. Normal block
        let block = build_test_block();
        assert_eq!(block.get_num_offsets().unwrap(), 2);
    }

    #[test]
    fn test_block_get_offset() {
        let block = build_test_block();
        let num_offsets = block.get_num_offsets().unwrap();

        // The first entry ("apple") starts at byte 0
        let offset_0 = block.get_offset(0, num_offsets).unwrap();
        assert_eq!(offset_0, 0);

        // The second entry ("banana") starts immediately after "apple"
        let offset_1 = block.get_offset(1, num_offsets).unwrap();
        assert!(offset_1 > 0);
    }

    #[test]
    fn test_block_peek_user_key() {
        let block = build_test_block();
        let num_offsets = block.get_num_offsets().unwrap();

        let offset_0 = block.get_offset(0, num_offsets).unwrap();
        let key_0 = block.peek_user_key(offset_0).unwrap();
        assert_eq!(key_0, "apple");

        let offset_1 = block.get_offset(1, num_offsets).unwrap();
        let key_1 = block.peek_user_key(offset_1).unwrap();
        assert_eq!(key_1, "banana");
    }

    // =========================================================================
    // Block Decode Entry Tests (Puts, Deletes, and Corruption)
    // =========================================================================

    #[test]
    fn test_block_decode_entry_put() {
        let block = build_test_block();
        let offset_0 = block
            .get_offset(0, block.get_num_offsets().unwrap())
            .unwrap();

        let (key, record) = block.decode_entry(offset_0).unwrap();

        assert_eq!(key.user_key, "apple");
        assert_eq!(key.seq_num, 10);

        if let Record::Put(val) = record {
            assert_eq!(val, b("red"));
        } else {
            panic!("Expected Put record but got Delete");
        }
    }

    #[test]
    fn test_block_decode_entry_delete() {
        let block = build_test_block();
        let offset_1 = block
            .get_offset(1, block.get_num_offsets().unwrap())
            .unwrap();

        let (key, record) = block.decode_entry(offset_1).unwrap();

        assert_eq!(key.user_key, "banana");
        assert_eq!(key.seq_num, 5);
        assert!(matches!(record, Record::Delete));
    }

    #[test]
    fn test_block_decode_entry_invalid_tag_errors() {
        let mut builder = BlockBuilder::new();
        let key = make_key("corrupt_me", 1);
        builder.add(&key, &Record::Put(b("data")));

        let mut raw_data = builder.build();

        // Calculate exactly where the tag byte is.
        // KEY_LEN_SIZE (2) + key_bytes.len() (10) + SEQ_NUM_SIZE (8)
        let tag_index = KEY_LEN_SIZE + "corrupt_me".len() + SEQ_NUM_SIZE;

        // Intentionally corrupt the byte on "disk" to an invalid enum tag
        raw_data[tag_index] = 255;

        let block = Block::decode(raw_data);

        // This should return an Err(BlockError::CorruptData) instead of panicking
        let result = block.decode_entry(0);
        assert!(result.is_err(), "Expected an error due to invalid tag");

        // Optional: Ensure it's the exact corruption error we expect
        if let Err(BlockError::CorruptData(msg)) = result {
            assert!(msg.contains("invalid RecordTag"));
        } else {
            panic!("Expected CorruptData error!");
        }
    }

    // =========================================================================
    // Block Search Tests (8 Tests)
    // =========================================================================

    #[test]
    fn test_block_decode_and_search_empty() {
        // An empty block (just an empty vector)
        let block = Block::decode(vec![]);
        assert!(block.search("any_key").unwrap().is_none());
    }

    #[test]
    fn test_block_search_exact_match_put() {
        let mut builder = BlockBuilder::new();
        builder.add(&make_key("my_target", 5), &Record::Put(b("my_value")));

        let block = Block::decode(builder.build());
        let (k, r) = block
            .search("my_target")
            .unwrap()
            .expect("Target key should be found");

        assert_eq!(k.user_key, "my_target");
        assert_eq!(k.seq_num, 5);
        if let Record::Put(val) = r {
            assert_eq!(val, b("my_value"));
        } else {
            panic!("Expected Put record!");
        }
    }

    #[test]
    fn test_block_search_exact_match_delete() {
        let mut builder = BlockBuilder::new();
        builder.add(&make_key("deleted_key", 10), &Record::Delete);

        let block = Block::decode(builder.build());
        let (k, r) = block
            .search("deleted_key")
            .unwrap()
            .expect("Deleted key should be found");

        assert_eq!(k.user_key, "deleted_key");
        assert_eq!(k.seq_num, 10);
        assert!(matches!(r, Record::Delete));
    }

    #[test]
    fn test_block_search_not_found_less_than_all() {
        let mut builder = BlockBuilder::new();
        builder.add(&make_key("m", 1), &Record::Put(b("val_m")));
        builder.add(&make_key("z", 1), &Record::Put(b("val_z")));

        let block = Block::decode(builder.build());
        // "a" comes before "m", so it should cleanly return None without error
        assert!(block.search("a").unwrap().is_none());
    }

    #[test]
    fn test_block_search_not_found_greater_than_all() {
        let mut builder = BlockBuilder::new();
        builder.add(&make_key("a", 1), &Record::Put(b("val_a")));
        builder.add(&make_key("m", 1), &Record::Put(b("val_m")));

        let block = Block::decode(builder.build());
        // "z" comes after "m", testing the right-edge bounds
        assert!(block.search("z").unwrap().is_none());
    }

    #[test]
    fn test_block_search_not_found_in_middle() {
        let mut builder = BlockBuilder::new();
        builder.add(&make_key("a", 1), &Record::Put(b("val_a")));
        builder.add(&make_key("c", 1), &Record::Put(b("val_c")));

        let block = Block::decode(builder.build());
        // "b" is missing from the middle
        assert!(block.search("b").unwrap().is_none());
    }

    #[test]
    fn test_block_search_mvcc_multiple_versions() {
        let mut builder = BlockBuilder::new();

        // In LSM trees, MemTable iterations yield descending sequence numbers.
        // We write them to the block exactly in that order.
        builder.add(&make_key("A", 100), &Record::Put(b("newest_val"))); // Newest
        builder.add(&make_key("A", 90), &Record::Put(b("older_val"))); // Older
        builder.add(&make_key("A", 80), &Record::Delete); // Oldest

        let block = Block::decode(builder.build());

        // Searching for "A" MUST return sequence 100, ignoring the others.
        let (k, r) = block.search("A").unwrap().expect("Key A should be found");

        assert_eq!(k.user_key, "A");
        assert_eq!(
            k.seq_num, 100,
            "MVCC lower-bound search failed! Returned older version."
        );
        if let Record::Put(val) = r {
            assert_eq!(val, b("newest_val"));
        } else {
            panic!("Expected Put record!");
        }
    }

    #[test]
    fn test_block_search_mixed_keys_and_versions() {
        let mut builder = BlockBuilder::new();

        // Setup a complex block with multiple keys and versions
        builder.add(&make_key("apple", 5), &Record::Put(b("a5")));

        builder.add(&make_key("banana", 20), &Record::Delete);
        builder.add(&make_key("banana", 15), &Record::Put(b("b15")));
        builder.add(&make_key("banana", 10), &Record::Put(b("b10")));

        builder.add(&make_key("cherry", 8), &Record::Put(b("c8")));
        builder.add(&make_key("cherry", 2), &Record::Put(b("c2")));

        let block = Block::decode(builder.build());

        // Test banana
        let (k_b, r_b) = block
            .search("banana")
            .unwrap()
            .expect("banana should be found");
        assert_eq!(k_b.seq_num, 20); // Should grab the delete tombstone
        assert!(matches!(r_b, Record::Delete));

        // Test cherry
        let (k_c, r_c) = block
            .search("cherry")
            .unwrap()
            .expect("cherry should be found");
        assert_eq!(k_c.seq_num, 8);
        if let Record::Put(val) = r_c {
            assert_eq!(val, b("c8"));
        } else {
            panic!("Expected Put record!");
        }
    }
}
