//! Range scan over the engine.
//!
//! Reuses the existing merge-iterator machinery from `compaction`: build one
//! `KvIterator` per source (each memtable, each overlapping SSTable), feed
//! them all to `MergingIterator`, then layer a `ScanFilter` on top that
//! implements the read-visible semantics — dedupe by user key (the newest
//! version wins, since `MergingIterator` emits seq-DESC within a key), drop
//! tombstones, honour the upper bound and the limit.

use std::io;
use std::ops::Bound;
use std::path::Path;

use crate::compaction::{MergingIterator, SsTableIterator};
use crate::core::{KvIterator, Record};
use crate::engine::EngineCore;
use crate::memtable::MemTable;
use crate::versioning::sst_path;

/// Reads the engine at the current snapshot and returns up to `limit`
/// live `(user_key, value)` pairs whose keys fall in `[start, end)` (with
/// `Bound` flexibility on both ends), in ascending key order. Deletes are
/// filtered out; only the newest version of each key is returned.
pub(super) fn scan_impl(
    core: &EngineCore,
    start: Bound<&str>,
    end: Bound<&str>,
    limit: usize,
) -> io::Result<Vec<(String, Vec<u8>)>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    // 1. Snapshot memtables + SSTable version. Each Arc clone pins the
    //    relevant storage for the duration of this scan; concurrent flushes
    //    and compactions may run, but their effects on the snapshot are
    //    deferred (memtables stay alive; SstFileGuards keep files on disk).
    let memtables = core.state.snapshot_memtables();
    let version = core.current_version();

    // 2. Build one KvIterator per source. Memtables already implement
    //    range-aware iteration; SSTable iterators currently walk the entire
    //    file, and `ScanFilter` discards entries outside `[start, end)`.
    let mem_start = bound_to_owned(start);
    let mem_end = bound_to_owned(end);
    let mut iterators: Vec<Box<dyn KvIterator>> = Vec::new();

    for table in memtables {
        iterators.push(table.get_iterator(mem_start.clone(), mem_end.clone()));
    }

    // SSTable file collection. L0 ranges overlap, so include every L0 file
    // whose key range intersects the scan range. L1+ are non-overlapping per
    // level — `VersionState::overlapping_files` returns the intersecting
    // files for each level.
    for meta in version.files_at_level(0) {
        if file_overlaps_scan(&meta.smallest_key, &meta.largest_key, start, end) {
            push_sstable_iterator(&mut iterators, &sst_path(&core.data_dir, meta.file_id))?;
        }
    }
    for level in 1..7usize {
        let (lo, hi) = match (start, end) {
            // overlapping_files works on inclusive bounds. For our purposes,
            // a missing lower bound is conservatively treated as "" (the
            // smallest possible string) and a missing upper bound as the
            // string of every byte 0xFF, but in practice
            // overlapping_files's check (`smallest_key <= hi && largest_key >= lo`)
            // tolerates a generous range.
            _ => (start_str(start), end_str(end)),
        };
        for meta in version.overlapping_files(level, lo, hi) {
            push_sstable_iterator(&mut iterators, &sst_path(&core.data_dir, meta.file_id))?;
        }
    }

    // 3. Merge + filter.
    let merging = MergingIterator::new(iterators);
    let filter = ScanFilter {
        inner: merging,
        start: bound_to_owned(start),
        end: bound_to_owned(end),
        last_key: None,
        yielded: 0,
        limit,
    };

    Ok(filter.collect_vec())
}

/// Adapter over `MergingIterator` that produces the read-visible scan
/// output: one live `(key, value)` per user_key (newest version), bounded by
/// `end` and `limit`. Tombstones are consumed silently and suppress that
/// key entirely.
struct ScanFilter {
    inner: MergingIterator,
    start: Bound<String>,
    end: Bound<String>,
    last_key: Option<String>,
    yielded: usize,
    limit: usize,
}

impl ScanFilter {
    fn collect_vec(mut self) -> Vec<(String, Vec<u8>)> {
        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        while self.yielded < self.limit {
            match self.inner.next() {
                None => break,
                Some((key, record, _seq)) => {
                    // Upper-bound check first — entries are emitted in
                    // ascending order, so once we cross `end` we're done.
                    if past_end(&key, &self.end) {
                        break;
                    }
                    // Lower-bound check. Memtable iterators are already
                    // bounded, but SSTable iterators walk the whole file —
                    // so without this filter, sub-start keys from SSTables
                    // leak through. We skip without touching `last_key`
                    // because no key < start can ever be one we'd yield
                    // later (entries arrive in ascending order).
                    if before_start(&key, &self.start) {
                        continue;
                    }
                    // Dedup: skip older versions of an already-emitted key.
                    if self.last_key.as_deref() == Some(key.as_str()) {
                        continue;
                    }
                    self.last_key = Some(key.clone());
                    // Drop tombstones — they suppress the key from output.
                    match record {
                        Record::Put(value) => {
                            out.push((key, value));
                            self.yielded += 1;
                        }
                        Record::Delete => {
                            // Tombstone consumed; `last_key` was bumped above
                            // so the next-older Put for the same key is also
                            // skipped on its own iteration.
                        }
                    }
                }
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bound_to_owned(b: Bound<&str>) -> Bound<String> {
    match b {
        Bound::Included(s) => Bound::Included(s.to_string()),
        Bound::Excluded(s) => Bound::Excluded(s.to_string()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn past_end(key: &str, end: &Bound<String>) -> bool {
    match end {
        Bound::Included(e) => key > e.as_str(),
        Bound::Excluded(e) => key >= e.as_str(),
        Bound::Unbounded => false,
    }
}

fn before_start(key: &str, start: &Bound<String>) -> bool {
    match start {
        Bound::Included(s) => key < s.as_str(),
        Bound::Excluded(s) => key <= s.as_str(),
        Bound::Unbounded => false,
    }
}

/// String form of the lower bound used by `version.overlapping_files`.
/// `overlapping_files` filters by `smallest_key <= hi && largest_key >= lo`;
/// for an `Unbounded` lower we want the lowest possible string, "".
fn start_str(b: Bound<&str>) -> &str {
    match b {
        Bound::Included(s) | Bound::Excluded(s) => s,
        Bound::Unbounded => "",
    }
}

/// String form of the upper bound. For `Unbounded` upper we use the highest
/// reachable string for our keys, which we approximate with "\u{10FFFF}"
/// (every key compares less). This is only used to *pre-filter* SSTables;
/// `ScanFilter` does the precise bound check on each emitted key, so a few
/// extra files getting scanned is at worst a perf cost.
fn end_str(b: Bound<&str>) -> &str {
    match b {
        Bound::Included(s) | Bound::Excluded(s) => s,
        Bound::Unbounded => "\u{10FFFF}",
    }
}

/// Returns true when `[smallest, largest]` intersects the scan range
/// `[start, end)`. Used to prune L0 files that lie entirely outside the
/// scan range — `version.overlapping_files` covers the L1+ case via the
/// version state.
fn file_overlaps_scan(
    smallest: &str,
    largest: &str,
    start: Bound<&str>,
    end: Bound<&str>,
) -> bool {
    let after_start = match start {
        Bound::Included(s) => largest >= s,
        Bound::Excluded(s) => largest > s,
        Bound::Unbounded => true,
    };
    let before_end = match end {
        Bound::Included(e) => smallest <= e,
        Bound::Excluded(e) => smallest < e,
        Bound::Unbounded => true,
    };
    after_start && before_end
}

fn push_sstable_iterator(
    iterators: &mut Vec<Box<dyn KvIterator>>,
    path: &Path,
) -> io::Result<()> {
    match SsTableIterator::open(path) {
        Ok(iter) => {
            iterators.push(Box::new(iter));
            Ok(())
        }
        Err(e) => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("scan: failed to open SSTable {:?}: {}", path, e),
        )),
    }
}
