// ---------------------------------------------------------------------------
// MergingIterator — K-way merge using a min-heap
// ---------------------------------------------------------------------------

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

/// Merges N sorted iterators into one sorted stream using a binary min-heap.
/// Used by the compactor and by `LsmEngine::scan`.
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
mod test {
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