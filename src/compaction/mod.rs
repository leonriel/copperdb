use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::mpsc::Receiver;

use crate::core::merge::MergingIterator;
use crate::core::{KvIterator, Record};
use crate::engine::EngineCore;
use crate::sstable::iter::SsTableIterator;
use crate::versioning::{SstableMetadata, VersionState, sst_path};
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

/// Absorb `SsTableIterator::IterError` into `CompactionError` so the
/// compactor can keep using `?` against `SsTableIterator::open(...)`
/// without splitting variants in the iterator's error type to match
/// our own.
impl From<crate::sstable::iter::IterError> for CompactionError {
    fn from(e: crate::sstable::iter::IterError) -> Self {
        match e {
            crate::sstable::iter::IterError::Io(io) => CompactionError::Io(io),
            crate::sstable::iter::IterError::Corrupt(msg) => CompactionError::SSTable(msg),
        }
    }
}


// ---------------------------------------------------------------------------
// Background worker entry point
// ---------------------------------------------------------------------------

/// Entry point for the background compaction thread.
///
/// Woken up by a signal on `rx` (sent by the engine after each compaction
/// round or after registering a new SSTable). Holds a strong `Arc<EngineCore>`
/// so the shared state stays alive for the duration of any in-flight
/// compaction; `LsmEngine::drop` sets `core.shutdown` and wakes the channel
/// to cleanly tear this thread down.
pub fn run(core: Arc<EngineCore>, rx: Receiver<()>) {
    for () in &rx {
        if core.shutdown.load(AtomicOrdering::SeqCst) {
            break;
        }
        compact_if_needed(&core);
    }
}

// ---------------------------------------------------------------------------
// Compaction orchestration
// ---------------------------------------------------------------------------

/// Check every level (lowest first) and compact the first one that exceeds
/// its size/file-count threshold.
fn compact_if_needed(engine: &EngineCore) {
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
    engine: &EngineCore,
    version: &VersionState,
    level: usize,
) -> Result<(), CompactionError> {
    // Mark this compaction as in-flight for the duration of the call. Drop
    // fires on every exit path (success, error, early return) so the count
    // stays consistent even if `?` propagates an error mid-function.
    let _in_flight = InFlightGuard::new(&engine.in_flight_compactions);

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
    engine.total_compactions.fetch_add(1, AtomicOrdering::Relaxed);

    // Files are unlinked via `Arc<SstFileGuard>` refcounts: `apply(RemoveFile)`
    // marks the guard, and the actual `remove_file` syscall fires when the
    // last clone drops (typically when the old `Arc<VersionState>` snapshot
    // is released by readers). No manual cleanup needed here.

    Ok(())
}

/// RAII guard that increments an `AtomicU64` on construction and decrements
/// on drop. Used to track `in_flight_compactions` consistently across all
/// exit paths of `compact_level`.
struct InFlightGuard<'a> {
    counter: &'a std::sync::atomic::AtomicU64,
}

impl<'a> InFlightGuard<'a> {
    fn new(counter: &'a std::sync::atomic::AtomicU64) -> Self {
        counter.fetch_add(1, AtomicOrdering::Relaxed);
        Self { counter }
    }
}

impl<'a> Drop for InFlightGuard<'a> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, AtomicOrdering::Relaxed);
    }
}

/// Returns `true` when `level` is the deepest level that currently holds files.
fn is_bottommost_level(version: &VersionState, level: usize) -> bool {
    (level + 1..NUM_LEVELS).all(|deeper| version.files_at_level(deeper).is_empty())
}

/// Drive the K-way merge from `iter` and write output into one or more new
/// SSTable files at `output_level`. Returns
/// `(level, file_id, smallest_key, largest_key, max_seq)` for every new file.
fn merge_and_write(
    engine: &EngineCore,
    mut iter: MergingIterator,
    output_level: u8,
    drop_tombstones: bool,
) -> Result<Vec<(u8, u64, String, String, u64)>, CompactionError> {
    let mut output_files: Vec<(u8, u64, String, String, u64)> = Vec::new();
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
                if let Some((lo, hi, max_seq)) = writer
                    .finish_file()
                    .map_err(|e| CompactionError::SSTable(e.to_string()))?
                {
                    output_files.push((output_level, file_id, lo, hi, max_seq));
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
        if let Some((lo, hi, max_seq)) = writer
            .finish_file()
            .map_err(|e| CompactionError::SSTable(e.to_string()))?
        {
            output_files.push((output_level, file_id, lo, hi, max_seq));
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
    use crate::engine::LsmEngine;
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

    /// A high-cardinality workload — tens of thousands of distinct keys —
    /// must flush and compact without errors. Pre-multi-level the L0→L1
    /// compaction output's index would overflow the single 1 MB index
    /// budget at this cardinality and compaction silently failed. The
    /// existing stress tests miss this because they hammer 30–50 keys
    /// repeatedly — after dedup, merged outputs are tiny.
    #[test]
    fn compaction_handles_high_cardinality_distinct_keys() {
        const SMALL_MEMTABLE: usize = 32 * 1024;
        const NUM_KEYS: u64 = 50_000;
        const VALUE_SIZE: usize = 256;

        let dir = tmp_dir();
        let engine = LsmEngine::open_with_memtable_size(&dir, SMALL_MEMTABLE).unwrap();

        for i in 0u64..NUM_KEYS {
            let key = format!("key_{:08}", i);
            let val = vec![i as u8; VALUE_SIZE];
            engine.put(key, val).unwrap();
        }

        // Give the flusher + compactor time to drain. Compactions cascade,
        // so this must be long enough for at least one L0→L1 pass. 50 k
        // keys × 256 B against a 32 KB memtable means several hundred
        // flushes plus the cascading L0→L1, so we allow generous slack.
        std::thread::sleep(Duration::from_secs(15));

        let version = engine.current_version();
        let l0 = version.files_at_level(0).len();
        let l1 = version.files_at_level(1).len();

        // Structural invariant: compaction must have produced L1 output.
        // If compaction silently failed (as it did pre-fix), L1 would be
        // empty and L0 would balloon past the trigger.
        assert!(
            l1 > 0,
            "expected at least one L1 file after compaction; L0={}",
            l0,
        );
        assert!(
            l0 <= L0_COMPACTION_TRIGGER * 2,
            "L0 file count ({}) exceeded 2× compaction trigger ({}); compaction \
             likely failed silently",
            l0,
            L0_COMPACTION_TRIGGER,
        );

        // Correctness: every key must still read back. Pre-fix this assertion
        // passes even with broken compaction because L0 still holds the data;
        // we include it as a sanity check on the structural assertions above.
        for i in 0u64..NUM_KEYS {
            let key = format!("key_{:08}", i);
            let got = engine.get(&key);
            assert_eq!(
                got.as_deref().map(|v| v.len()),
                Some(VALUE_SIZE),
                "missing or wrong-sized value for {}",
                key,
            );
        }
    }
}
