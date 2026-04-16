use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Weak};
use std::sync::mpsc::Receiver;

use crate::core::{InternalKey, KvIterator, Record};
use crate::db::LsmEngine;
use crate::manifest::{SstableMetadata, VersionState, sst_path};
use crate::sstable::block::Block;
use crate::sstable::{FOOTER_SIZE, INDEX_OFFSET_SIZE, MAGIC_NUMBER, META_OFFSET_SIZE};
use crate::sstable::writer::SsTableBuilder;

// ---------------------------------------------------------------------------
// Configuration — no magic numbers
// ---------------------------------------------------------------------------

/// Number of L0 files that triggers a L0→L1 compaction.
const L0_COMPACTION_TRIGGER: usize = 4;

/// Maximum total bytes at L1 before triggering an L1→L2 compaction.
const L1_MAX_BYTES: u64 = 640 * 1024 * 1024;

/// Each level is this many times larger than the one above it.
const LEVEL_SIZE_MULTIPLIER: u64 = 10;

/// Target size of each output SSTable produced by compaction.
const MAX_OUTPUT_FILE_SIZE: u64 = 64 * 1024 * 1024;

/// Total number of levels in the LSM tree.
const NUM_LEVELS: usize = 7;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(thiserror::Error, Debug)]
pub enum CompactionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("SSTable error: {0}")]
    SSTable(String),
}

// ---------------------------------------------------------------------------
// SsTableIterator — full sequential scan of one SSTable file
// ---------------------------------------------------------------------------

/// Reads an SSTable file block-by-block and yields every (key, record, seq_num)
/// entry in on-disk (sorted) order. Used as one input stream to `MergingIterator`.
struct SsTableIterator {
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
    fn open(path: &Path) -> Result<Self, CompactionError> {
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

        // Read the index block (sits between meta block and footer).
        let index_size = file_len
            .saturating_sub(FOOTER_SIZE as u64)
            .saturating_sub(index_offset) as usize;

        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_data = vec![0u8; index_size];
        file.read_exact(&mut index_data)?;

        let index_block = Block::decode(index_data);
        let num_index_entries = index_block
            .get_num_offsets()
            .map_err(|e| CompactionError::SSTable(e.to_string()))?;

        // Collect the start byte of each data block from the index.
        let mut block_starts: Vec<u64> = Vec::with_capacity(num_index_entries);
        for i in 0..num_index_entries {
            let entry_offset = index_block
                .get_offset(i, num_index_entries)
                .map_err(|e| CompactionError::SSTable(e.to_string()))?;

            let (_, record) = index_block
                .decode_entry(entry_offset)
                .map_err(|e| CompactionError::SSTable(e.to_string()))?;

            let block_start = match record {
                Record::Put(v) => {
                    let arr: [u8; 8] = v.try_into().map_err(|_| {
                        CompactionError::SSTable(
                            "Index block pointer is not 8 bytes".to_string(),
                        )
                    })?;
                    u64::from_be_bytes(arr)
                }
                Record::Delete => {
                    return Err(CompactionError::SSTable(
                        "Index block contains a Delete record".to_string(),
                    ));
                }
            };
            block_starts.push(block_start);
        }

        // Build (start, end) ranges. The last block ends where the index block begins.
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

// ---------------------------------------------------------------------------
// MergingIterator — K-way merge using a min-heap
// ---------------------------------------------------------------------------

/// One entry held in the heap, tagged with the source iterator index so we can
/// advance that iterator after popping.
struct HeapEntry {
    key: InternalKey,
    record: Record,
    source_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap. Reversing the key ordering makes it a
        // min-heap: the entry with the smallest InternalKey has the highest
        // priority and is popped first.
        //
        // InternalKey sorts by user_key ASC, then seq_num DESC, so for the
        // same user_key the entry with the highest seq_num comes out first —
        // exactly what the compaction dedup logic expects.
        other.key.cmp(&self.key)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Merges N sorted iterators into one sorted stream using a binary min-heap.
struct MergingIterator {
    iterators: Vec<Box<dyn KvIterator>>,
    heap: BinaryHeap<HeapEntry>,
}

impl MergingIterator {
    fn new(mut iterators: Vec<Box<dyn KvIterator>>) -> Self {
        let mut heap = BinaryHeap::with_capacity(iterators.len());

        for (idx, iter) in iterators.iter_mut().enumerate() {
            if let Some((user_key, record, seq_num)) = iter.next() {
                heap.push(HeapEntry {
                    key: InternalKey { user_key, seq_num },
                    record,
                    source_idx: idx,
                });
            }
        }

        Self { iterators, heap }
    }
}

impl KvIterator for MergingIterator {
    fn is_valid(&self) -> bool {
        !self.heap.is_empty()
    }

    fn next(&mut self) -> Option<(String, Record, u64)> {
        let entry = self.heap.pop()?;

        // Advance the iterator that produced this entry and push its next item.
        if let Some((user_key, record, seq_num)) = self.iterators[entry.source_idx].next() {
            self.heap.push(HeapEntry {
                key: InternalKey { user_key, seq_num },
                record,
                source_idx: entry.source_idx,
            });
        }

        Some((entry.key.user_key, entry.record, entry.key.seq_num))
    }
}

// ---------------------------------------------------------------------------
// Background worker entry point
// ---------------------------------------------------------------------------

/// Entry point for the background compaction thread.
///
/// Woken up by a signal on `rx` (sent by the engine after each compaction
/// round or after registering a new SSTable). Holds a `Weak<LsmEngine>` so
/// the engine can be cleanly dropped.
pub fn run(engine: Weak<LsmEngine>, rx: Receiver<()>) {
    for () in &rx {
        let Some(engine) = engine.upgrade() else { break };
        compact_if_needed(&engine);
    }

    // Channel closed. Do a final pass in case a signal arrived just before
    // shutdown.
    if let Some(engine) = engine.upgrade() {
        compact_if_needed(&engine);
    }
}

// ---------------------------------------------------------------------------
// Compaction orchestration
// ---------------------------------------------------------------------------

/// Check every level (lowest first) and compact the first one that exceeds
/// its size/file-count threshold.
fn compact_if_needed(engine: &Arc<LsmEngine>) {
    let version = engine.current_version();

    for level in 0..NUM_LEVELS - 1 {
        let needs_compact = if level == 0 {
            version.files_at_level(0).len() >= L0_COMPACTION_TRIGGER
        } else {
            level_bytes(&version, &engine.data_dir, level) > level_max_bytes(level)
        };

        if needs_compact {
            if let Err(e) = compact_level(engine, &version, level) {
                eprintln!("[compaction] L{}→L{} error: {}", level, level + 1, e);
            }
            return;
        }
    }
}

/// Sum of the on-disk sizes of all SSTable files at `level`.
fn level_bytes(version: &VersionState, dir: &Path, level: usize) -> u64 {
    version
        .files_at_level(level)
        .iter()
        .map(|m| {
            std::fs::metadata(sst_path(dir, m.file_id))
                .map(|meta| meta.len())
                .unwrap_or(0)
        })
        .sum()
}

/// Maximum total bytes allowed at `level` (1-based; level 0 uses a file-count
/// trigger). Each level is `LEVEL_SIZE_MULTIPLIER` × the level above it.
fn level_max_bytes(level: usize) -> u64 {
    let exponent = (level.saturating_sub(1)) as u32;
    L1_MAX_BYTES.saturating_mul(LEVEL_SIZE_MULTIPLIER.saturating_pow(exponent))
}

/// Pick the input files for a compaction at `level`, merge them with any
/// overlapping files from `level + 1`, write the results, and update the
/// version state.
fn compact_level(
    engine: &Arc<LsmEngine>,
    version: &VersionState,
    level: usize,
) -> Result<(), CompactionError> {
    let output_level = level + 1;

    // Pick victim files from `level`.
    // L0: compact every file (they may overlap, so we must merge all of them
    //     together to produce non-overlapping output at L1).
    // L1+: pick the first file by smallest key (simple round-robin approximation).
    let inputs_from_level: Vec<SstableMetadata> = if level == 0 {
        version.files_at_level(0).to_vec()
    } else {
        match version.files_at_level(level).first() {
            Some(m) => vec![m.clone()],
            None => return Ok(()),
        }
    };

    if inputs_from_level.is_empty() {
        return Ok(());
    }

    // Key range spanned by the chosen input files.
    let lo = inputs_from_level[0].smallest_key.as_str();
    let hi = inputs_from_level[0].largest_key.as_str();
    let (lo, hi) = inputs_from_level[1..].iter().fold((lo, hi), |(lo, hi), m| {
        (
            if m.smallest_key.as_str() < lo { m.smallest_key.as_str() } else { lo },
            if m.largest_key.as_str()  > hi { m.largest_key.as_str()  } else { hi },
        )
    });

    // All overlapping files from the output level.
    let inputs_from_output_level: Vec<SstableMetadata> = version
        .overlapping_files(output_level, lo, hi)
        .into_iter()
        .cloned()
        .collect();

    // Open one iterator per input file.
    let mut iterators: Vec<Box<dyn KvIterator>> = Vec::new();

    for meta in inputs_from_level.iter().chain(inputs_from_output_level.iter()) {
        let path = sst_path(&engine.data_dir, meta.file_id);
        let iter = SsTableIterator::open(&path)?;
        iterators.push(Box::new(iter));
    }

    // Tombstones are only safe to drop at the bottommost occupied level —
    // below that there is no older data that the tombstone needs to mask.
    let drop_tombstones = is_bottommost_level(version, output_level);

    let merging = MergingIterator::new(iterators);
    let output_files =
        merge_and_write(engine, merging, output_level as u8, drop_tombstones)?;

    let removed: Vec<(u8, u64)> = inputs_from_level
        .iter()
        .chain(inputs_from_output_level.iter())
        .map(|m| (m.level, m.file_id))
        .collect();

    engine.record_compaction(&removed, &output_files)?;

    // Delete the now-superseded SSTable files from disk.
    for meta in inputs_from_level.iter().chain(inputs_from_output_level.iter()) {
        let path = sst_path(&engine.data_dir, meta.file_id);
        if let Err(e) = std::fs::remove_file(&path) {
            eprintln!("[compaction] could not delete {:?}: {}", path, e);
        }
    }

    Ok(())
}

/// Returns `true` when `level` is the deepest level that currently holds files.
fn is_bottommost_level(version: &VersionState, level: usize) -> bool {
    (level + 1..NUM_LEVELS).all(|deeper| version.files_at_level(deeper).is_empty())
}

/// Drive the K-way merge from `iter` and write output into one or more new
/// SSTable files at `output_level`. Returns
/// `(level, file_id, smallest_key, largest_key)` for every new file.
fn merge_and_write(
    engine: &Arc<LsmEngine>,
    mut iter: MergingIterator,
    output_level: u8,
    drop_tombstones: bool,
) -> Result<Vec<(u8, u64, String, String)>, CompactionError> {
    let mut output_files: Vec<(u8, u64, String, String)> = Vec::new();
    let mut current_writer: Option<(SsTableBuilder, u64)> = None;
    let mut last_user_key: Option<String> = None;

    while let Some((user_key, record, seq_num)) = iter.next() {
        // The MergingIterator yields entries in InternalKey order (user_key ASC,
        // seq_num DESC). The first occurrence of each user_key is the most recent
        // version; all subsequent ones are older and must be discarded.
        if last_user_key.as_deref() == Some(user_key.as_str()) {
            continue;
        }

        // Drop tombstones at the bottommost level — no older data can lurk below.
        if drop_tombstones && matches!(record, Record::Delete) {
            last_user_key = Some(user_key);
            continue;
        }

        last_user_key = Some(user_key.clone());

        // Roll over to a new output file when the current one hits the size limit.
        if current_writer
            .as_ref()
            .map_or(false, |(w, _)| w.current_size() >= MAX_OUTPUT_FILE_SIZE)
        {
            if let Some((writer, file_id)) = current_writer.take() {
                if let Some((lo, hi)) = writer
                    .finish_file()
                    .map_err(|e| CompactionError::SSTable(e.to_string()))?
                {
                    output_files.push((output_level, file_id, lo, hi));
                }
            }
        }

        // Open a new output file if we don't have one yet.
        if current_writer.is_none() {
            let file_id = engine.alloc_sst_id();
            let path = sst_path(&engine.data_dir, file_id);
            let path_str = path.to_str().ok_or_else(|| {
                CompactionError::SSTable(format!(
                    "SSTable path {:?} is not valid UTF-8",
                    path
                ))
            })?;
            let builder = SsTableBuilder::new(path_str)
                .map_err(|e| CompactionError::SSTable(e.to_string()))?;
            current_writer = Some((builder, file_id));
        }

        if let Some((ref mut builder, _)) = current_writer {
            builder
                .add_entry(&user_key, &record, seq_num)
                .map_err(|e| CompactionError::SSTable(e.to_string()))?;
        }
    }

    // Finalize the last output file.
    if let Some((writer, file_id)) = current_writer {
        if let Some((lo, hi)) = writer
            .finish_file()
            .map_err(|e| CompactionError::SSTable(e.to_string()))?
        {
            output_files.push((output_level, file_id, lo, hi));
        }
    }

    Ok(output_files)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn tmp_dir() -> std::path::PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir()
            .join(format!("copperdb_compaction_{}_{}", std::process::id(), id));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn merging_iterator_yields_sorted_deduped_output() {
        struct VecIter {
            inner: std::vec::IntoIter<(String, Record, u64)>,
        }
        impl KvIterator for VecIter {
            fn is_valid(&self) -> bool { false }
            fn next(&mut self) -> Option<(String, Record, u64)> { self.inner.next() }
        }
        fn vi(v: Vec<(&str, u64)>) -> Box<dyn KvIterator> {
            Box::new(VecIter {
                inner: v.into_iter()
                    .map(|(k, s)| (k.to_string(), Record::Put(vec![]), s))
                    .collect::<Vec<_>>()
                    .into_iter(),
            })
        }

        // File A (L0 newer): apple@10, cherry@5
        // File B (L0 older): apple@3,  banana@7
        let iters = vec![vi(vec![("apple", 10), ("cherry", 5)]),
                         vi(vec![("apple", 3),  ("banana", 7)])];
        let mut merge = MergingIterator::new(iters);

        let first = merge.next().unwrap();
        assert_eq!(first.0, "apple");
        assert_eq!(first.2, 10, "highest seq_num wins");

        let second = merge.next().unwrap();
        assert_eq!(second.0, "apple");
        assert_eq!(second.2, 3, "older version still visible to dedup logic");

        let third = merge.next().unwrap();
        assert_eq!(third.0, "banana");

        let fourth = merge.next().unwrap();
        assert_eq!(fourth.0, "cherry");

        assert!(merge.next().is_none());
    }

    /// Write enough data to fill several L0 files and verify that compaction
    /// produces at least one L1 file and reduces the number of L0 files.
    #[test]
    fn compaction_reduces_l0_file_count() {
        // Small memtable so each flush creates a small L0 file quickly.
        // With 32 KB and ~120 bytes per entry, each flush covers ~273 entries.
        const SMALL_MEMTABLE: usize = 32 * 1024;

        let dir = tmp_dir();
        let engine = LsmEngine::open_with_memtable_size(&dir, SMALL_MEMTABLE).unwrap();

        // 2 000 entries × ~120 bytes ≈ 240 KB → ~7 flushes, triggering at
        // least one L0→L1 compaction (trigger is 4 files).
        for i in 0u64..2000 {
            let key = format!("key_{:08}", i);
            let val = vec![i as u8; 100];
            engine.put(key, val).unwrap();
        }

        std::thread::sleep(Duration::from_secs(3));

        let version = engine.current_version();
        let l0_after = version.files_at_level(0).len();
        let l1_after = version.files_at_level(1).len();

        assert!(
            l0_after < L0_COMPACTION_TRIGGER,
            "Expected L0 < {} after compaction, got {}",
            L0_COMPACTION_TRIGGER,
            l0_after,
        );
        assert!(
            l1_after > 0,
            "Expected at least one L1 file after compaction, L0={}",
            l0_after,
        );
    }

    /// Keys written before compaction must be readable after compaction.
    #[test]
    fn reads_survive_compaction() {
        const SMALL_MEMTABLE: usize = 32 * 1024;

        let dir = tmp_dir();
        let engine = LsmEngine::open_with_memtable_size(&dir, SMALL_MEMTABLE).unwrap();

        let entries: Vec<(String, Vec<u8>)> = (0u64..500)
            .map(|i| (format!("key_{:08}", i), vec![i as u8; 100]))
            .collect();

        for (k, v) in &entries {
            engine.put(k.clone(), v.clone()).unwrap();
        }

        std::thread::sleep(Duration::from_secs(2));

        for (k, v) in &entries {
            let got = engine.get(k);
            assert_eq!(got.as_deref(), Some(v.as_slice()), "key {} missing after compaction", k);
        }
    }

    /// Tombstones must not resurrect deleted keys after compaction.
    #[test]
    fn tombstones_respected_after_compaction() {
        const SMALL_MEMTABLE: usize = 32 * 1024;

        let dir = tmp_dir();
        let engine = LsmEngine::open_with_memtable_size(&dir, SMALL_MEMTABLE).unwrap();

        for i in 0u64..200 {
            engine.put(format!("key_{:08}", i), vec![i as u8; 100]).unwrap();
        }
        for i in 0u64..100 {
            engine.delete(format!("key_{:08}", i)).unwrap();
        }
        for i in 200u64..500 {
            engine.put(format!("key_{:08}", i), vec![i as u8; 100]).unwrap();
        }

        std::thread::sleep(Duration::from_secs(2));

        for i in 0u64..100 {
            assert_eq!(
                engine.get(&format!("key_{:08}", i)),
                None,
                "key_{:08} should have been deleted",
                i
            );
        }
        for i in 100u64..200 {
            assert!(
                engine.get(&format!("key_{:08}", i)).is_some(),
                "key_{:08} should still exist",
                i
            );
        }
    }

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
}
