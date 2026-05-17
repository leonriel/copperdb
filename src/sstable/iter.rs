// ---------------------------------------------------------------------------
// SsTableIterator — full sequential scan of one SSTable file
// ---------------------------------------------------------------------------

use std::{fs::File, io::SeekFrom};
use std::io::{Read, Seek};
use std::path::Path;

use crate::core::KvIterator;
use crate::sstable::block::Block;
use crate::sstable::{INDEX_OFFSET_SIZE, MAGIC_NUMBER, META_OFFSET_SIZE};
use crate::{compaction::CompactionError, core::{InternalKey, Record}, sstable::FOOTER_SIZE};

/// Reads an SSTable file block-by-block and yields every (key, record, seq_num)
/// entry in on-disk (sorted) order. Used as one input stream to `MergingIterator`
/// — both by the compactor and by `LsmEngine::scan`.
pub(crate) struct SsTableIterator {
    file: File,
    /// Byte ranges for each data block: (start_inclusive, end_exclusive).
    block_ranges: Vec<(u64, u64)>,
    /// Index of the next block to load from `block_ranges`.
    next_block_idx: usize,
    /// Entries decoded from the currently loaded block.
    current_entries: Vec<(InternalKey, Record)>,
    /// Index of the next entry to yield from `current_entries`.
    current_entry_idx: usize,
}

impl SsTableIterator {
    pub(crate) fn open(path: &Path) -> Result<Self, CompactionError> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();

        if file_len < FOOTER_SIZE as u64 {
            return Err(CompactionError::SSTable(format!(
                "SSTable {:?} is too short to contain a valid footer ({} bytes)",
                path, file_len,
            )));
        }

        // Read the footer.
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;

        let meta_offset = u64::from_be_bytes(
            footer_buf[0..META_OFFSET_SIZE]
                .try_into()
                .map_err(|_| CompactionError::SSTable("Failed to parse meta offset".to_string()))?,
        );

        let index_offset_offset = META_OFFSET_SIZE;
        let index_offset = u64::from_be_bytes(
            footer_buf[index_offset_offset..index_offset_offset + INDEX_OFFSET_SIZE]
                .try_into()
                .map_err(|_| CompactionError::SSTable("Failed to parse index offset".to_string()))?,
        );

        let magic_offset = META_OFFSET_SIZE + INDEX_OFFSET_SIZE;
        let magic = u64::from_be_bytes(
            footer_buf[magic_offset..]
                .try_into()
                .map_err(|_| CompactionError::SSTable("Failed to parse magic number".to_string()))?,
        );

        if magic != MAGIC_NUMBER {
            return Err(CompactionError::SSTable(format!(
                "SSTable {:?} has invalid magic number: {:#018X}",
                path, magic,
            )));
        }

        // Read the top index block (sits between the last L1 block and the
        // footer; `index_offset` in the footer now means top-index offset).
        let top_index_offset = index_offset;
        let top_index_size = file_len
            .saturating_sub(FOOTER_SIZE as u64)
            .saturating_sub(top_index_offset) as usize;

        file.seek(SeekFrom::Start(top_index_offset))?;
        let mut top_index_data = vec![0u8; top_index_size];
        file.read_exact(&mut top_index_data)?;
        let top_index = Block::decode(top_index_data);

        // Helper: decode an 8-byte big-endian u64 offset from an index entry.
        let decode_offset = |record: Record| -> Result<u64, CompactionError> {
            match record {
                Record::Put(v) => {
                    let arr: [u8; 8] = v.try_into().map_err(|_| {
                        CompactionError::SSTable("Index pointer is not 8 bytes".to_string())
                    })?;
                    Ok(u64::from_be_bytes(arr))
                }
                Record::Delete => Err(CompactionError::SSTable(
                    "Index block contains a Delete record".to_string(),
                )),
            }
        };

        // Walk the top index → for each L1 entry, read that L1 block from
        // disk and collect every data-block offset it points at.
        let n_top = top_index
            .get_num_offsets()
            .map_err(|e| CompactionError::SSTable(e.to_string()))?;

        let mut block_starts: Vec<u64> = Vec::new();
        for top_i in 0..n_top {
            let top_entry_offset = top_index
                .get_offset(top_i, n_top)
                .map_err(|e| CompactionError::SSTable(e.to_string()))?;
            let (_, top_record) = top_index
                .decode_entry(top_entry_offset)
                .map_err(|e| CompactionError::SSTable(e.to_string()))?;
            let l1_start = decode_offset(top_record)?;

            let l1_end = if top_i + 1 < n_top {
                let next_top_entry_offset = top_index
                    .get_offset(top_i + 1, n_top)
                    .map_err(|e| CompactionError::SSTable(e.to_string()))?;
                let (_, next_top_record) = top_index
                    .decode_entry(next_top_entry_offset)
                    .map_err(|e| CompactionError::SSTable(e.to_string()))?;
                decode_offset(next_top_record)?
            } else {
                top_index_offset
            };

            let l1_size = (l1_end - l1_start) as usize;
            file.seek(SeekFrom::Start(l1_start))?;
            let mut l1_data = vec![0u8; l1_size];
            file.read_exact(&mut l1_data)?;
            let l1_block = Block::decode(l1_data);

            let n_l1 = l1_block
                .get_num_offsets()
                .map_err(|e| CompactionError::SSTable(e.to_string()))?;
            for j in 0..n_l1 {
                let entry_offset = l1_block
                    .get_offset(j, n_l1)
                    .map_err(|e| CompactionError::SSTable(e.to_string()))?;
                let (_, record) = l1_block
                    .decode_entry(entry_offset)
                    .map_err(|e| CompactionError::SSTable(e.to_string()))?;
                block_starts.push(decode_offset(record)?);
            }
        }

        // Build (start, end) ranges. The last data block ends where the meta
        // block begins.
        let mut block_ranges: Vec<(u64, u64)> = Vec::with_capacity(block_starts.len());
        for (i, &start) in block_starts.iter().enumerate() {
            let end = if i + 1 < block_starts.len() {
                block_starts[i + 1]
            } else {
                meta_offset
            };
            block_ranges.push((start, end));
        }

        Ok(Self {
            file,
            block_ranges,
            next_block_idx: 0,
            current_entries: Vec::new(),
            current_entry_idx: 0,
        })
    }

    /// Load the next data block from disk into `current_entries`. Returns `true`
    /// if a block was loaded, `false` if there are no more blocks.
    fn load_next_block(&mut self) -> Result<bool, CompactionError> {
        if self.next_block_idx >= self.block_ranges.len() {
            return Ok(false);
        }

        let (start, end) = self.block_ranges[self.next_block_idx];
        self.next_block_idx += 1;

        let size = (end - start) as usize;
        self.file.seek(SeekFrom::Start(start))?;
        let mut data = vec![0u8; size];
        self.file.read_exact(&mut data)?;

        let block = Block::decode(data);
        self.current_entries = block
            .iter_all()
            .map_err(|e| CompactionError::SSTable(e.to_string()))?;
        self.current_entry_idx = 0;

        Ok(true)
    }
}

impl KvIterator for SsTableIterator {
    fn is_valid(&self) -> bool {
        self.current_entry_idx < self.current_entries.len()
            || self.next_block_idx < self.block_ranges.len()
    }

    fn next(&mut self) -> Option<(String, Record, u64)> {
        loop {
            if self.current_entry_idx < self.current_entries.len() {
                let (key, record) = self.current_entries[self.current_entry_idx].clone();
                self.current_entry_idx += 1;
                return Some((key.user_key, record, key.seq_num));
            }

            match self.load_next_block() {
                Ok(true) => continue,
                _ => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::writer::SsTableBuilder;
    use std::sync::atomic::Ordering;

    #[test]
    fn sstable_iterator_reads_all_entries() {
        use crate::sstable::writer::SsTableBuilder;
        use std::sync::atomic::AtomicUsize;

        static CNT: AtomicUsize = AtomicUsize::new(0);
        let id = CNT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "iter_test_{}_{}.sst",
            std::process::id(),
            id
        ));

        struct SliceIter {
            inner: std::vec::IntoIter<(String, Record, u64)>,
        }
        impl KvIterator for SliceIter {
            fn is_valid(&self) -> bool { false }
            fn next(&mut self) -> Option<(String, Record, u64)> { self.inner.next() }
        }

        let entries: Vec<(String, Record, u64)> = (0u32..20)
            .map(|i| (
                format!("key_{:04}", i),
                Record::Put(vec![i as u8; 500]),
                (20 - i) as u64,
            ))
            .collect();

        {
            let mut builder = SsTableBuilder::new(path.to_str().unwrap()).unwrap();
            builder
                .build_from_iterator(Box::new(SliceIter {
                    inner: entries.clone().into_iter(),
                }))
                .unwrap();
        }

        let mut iter = SsTableIterator::open(&path).unwrap();
        let mut count = 0usize;
        while let Some(_) = iter.next() {
            count += 1;
        }
        assert_eq!(count, entries.len(), "iterator must yield all entries");

        let _ = std::fs::remove_file(&path);
    }

    /// `SsTableIterator` must walk both index levels — it can't assume the
    /// single-level layout. Builds a file whose L1 index spans multiple
    /// blocks (enough distinct keys to overflow the 64 KB L1 target), then
    /// iterates it end-to-end and verifies entry count + sorted order.
    #[test]
    fn sstable_iterator_walks_multi_l1() {
        use std::sync::atomic::AtomicUsize;

        static CNT: AtomicUsize = AtomicUsize::new(0);
        let id = CNT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "iter_multi_l1_{}_{}.sst",
            std::process::id(),
            id,
        ));

        struct SliceIter {
            inner: std::vec::IntoIter<(String, Record, u64)>,
        }
        impl KvIterator for SliceIter {
            fn is_valid(&self) -> bool {
                false
            }
            fn next(&mut self) -> Option<(String, Record, u64)> {
                self.inner.next()
            }
        }

        // 30k entries × 256B values = ~7.5 MB of data → ~7,500 data blocks
        // → ~370 KB of L1 index → ~6 L1 blocks at the 64 KB L1 target.
        const NUM_KEYS: u32 = 30_000;
        const VALUE_SIZE: usize = 256;

        let entries: Vec<(String, Record, u64)> = (0..NUM_KEYS)
            .map(|i| {
                (
                    format!("key_{:020}", i),
                    Record::Put(vec![(i % 256) as u8; VALUE_SIZE]),
                    (NUM_KEYS - i) as u64,
                )
            })
            .collect();

        {
            let mut builder = SsTableBuilder::new(path.to_str().unwrap()).unwrap();
            builder
                .build_from_iterator(Box::new(SliceIter {
                    inner: entries.clone().into_iter(),
                }))
                .unwrap();
        }

        let mut iter = SsTableIterator::open(&path).unwrap();
        let mut yielded: Vec<(String, u64)> = Vec::new();
        while let Some((k, _, seq)) = iter.next() {
            yielded.push((k, seq));
        }

        assert_eq!(
            yielded.len(),
            entries.len(),
            "iterator must yield every entry across L1 boundaries",
        );

        // Confirm sorted-by-key order is preserved across L1 boundaries.
        let expected_keys: Vec<String> =
            entries.iter().map(|(k, _, _)| k.clone()).collect();
        let actual_keys: Vec<String> = yielded.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(actual_keys, expected_keys);

        let _ = std::fs::remove_file(&path);
    }
}