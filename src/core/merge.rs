//! K-way merge over the [`KvIterator`] trait.
//!
//! [`MergingIterator`] consumes any number of pre-sorted `KvIterator`
//! sources and yields their union in sorted order. The merge respects
//! [`InternalKey`]'s ordering — `user_key` ascending, `seq_num` descending
//! for the same user_key — so the newest version of a duplicated key
//! always emerges first. Callers layer their own dedup / tombstone logic
//! on top: compaction in `src/compaction/mod.rs` drops shadowed entries
//! and (at the bottommost level) tombstones; scans use `ScanFilter` in
//! `src/engine/scan.rs` for the same purpose.
//!
//! This module lives in `core` rather than alongside any one consumer
//! because the merge is format-agnostic — it doesn't know whether its
//! sources are SSTable iterators, memtable iterators, or anything else
//! implementing `KvIterator`. The sole concrete type, `MergingIterator`,
//! is `pub(crate)` so it's reachable from `compaction` and `engine`
//! without leaking outside the crate.
//!
//! Implementation: a [`BinaryHeap`](std::collections::BinaryHeap) of
//! `HeapEntry` per source, with the `Ord` impl reversed so the smallest
//! `InternalKey` has highest priority — turning Rust's max-heap into a
//! min-heap without wrapping every entry in `Reverse`.

use std::{cmp::Ordering, collections::BinaryHeap};

use crate::core::{InternalKey, KvIterator, Record};

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

pub(crate) struct MergingIterator {
    iterators: Vec<Box<dyn KvIterator>>,
    heap: BinaryHeap<HeapEntry>,
}

impl MergingIterator {
    pub(crate) fn new(mut iterators: Vec<Box<dyn KvIterator>>) -> Self {
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

#[cfg(test)]
mod tests {
    use crate::core::{KvIterator, Record, merge::MergingIterator};

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
}