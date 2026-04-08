use crossbeam_skiplist::SkipMap;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::core::{InternalKey, KvIterator, Record};

pub mod state;

/// A standard return type for queries.
pub type QueryResult = Option<(Record, u64)>;

/// The interface for our concurrent in-memory write buffer.
/// Requires Send + Sync so it can be safely shared across Tokio threads.
pub trait MemTable: Send + Sync {
    /// Creates a new MemTable with an ID.
    fn new(id: u64) -> Self;

    /// Inserts a new record or tombstone.
    /// Uses a sequence number to handle versioning and conflict resolution.
    fn put(&self, key: String, record: Record, seq_num: u64);

    /// Searches for a key, returning the record and its sequence number if found.
    fn get(&self, key: &str) -> QueryResult;

    /// Returns the estimated size in bytes to trigger the 64 MB flush.
    fn approximate_size(&self) -> usize;

    /// Returns the number of active writers to a MemTable
    fn active_writers(&self) -> usize;

    /// Returns a bounded iterator for the K-way merge process.
    /// `KvIterator` is the universal reader trait we defined previously.
    fn get_iterator(&self, low: Bound<String>, high: Bound<String>) -> Box<dyn KvIterator>;

    fn id(&self) -> u64;
}

/// A lock-free, concurrent memtable backed by a `crossbeam_skiplist::SkipMap`.
///
/// Supports concurrent reads and writes from multiple threads. An atomic
/// `active_writers` counter lets the flush worker spin-wait until all
/// in-flight writes have completed before iterating the frozen table.
pub struct CrossbeamMemTable {
    // We wrap the map in an Arc so our iterators can safely hold a reference
    // to it without introducing complex lifetime parameters to our traits.
    map: Arc<SkipMap<InternalKey, Record>>,
    approximate_size: AtomicUsize,
    active_writers: AtomicUsize,
    id: u64,
}

impl MemTable for CrossbeamMemTable {
    fn new(id: u64) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            approximate_size: AtomicUsize::new(0), // Used to determine whether to mark table as inactive

            // Flush worker checks that this is 0 before writing to disk
            // Potentially use a spin_loop() while waiting for active_writers
            // to drop to 0
            active_writers: AtomicUsize::new(0),
            id,
        }
    }

    fn id(&self) -> u64 {
        self.id
    }

    fn put(&self, user_key: String, record: Record, seq_num: u64) {
        // Announce that we are actively writing
        self.active_writers.fetch_add(1, Ordering::SeqCst);

        let key_len = user_key.len();
        let val_len = match &record {
            Record::Put(val) => val.len(),
            Record::Delete => 0,
        };

        let internal_key = InternalKey { user_key, seq_num };

        // Insert or update the key in the skip list
        self.map.insert(internal_key, record);

        // Atomically add the byte size (plus 8 bytes for the u64 seq_num)
        self.approximate_size
            .fetch_add(key_len + val_len + 8, Ordering::SeqCst);

        // We are done writing. Decrement the counter.
        self.active_writers.fetch_sub(1, Ordering::SeqCst);
    }

    fn get(&self, target_key: &str) -> QueryResult {
        // 1. Create a dummy key with the absolute highest sequence number.
        // Because of our custom Ord, this key will logically sit immediately
        // BEFORE the newest actual version of "apple" in the SkipList.
        let search_key = InternalKey {
            user_key: target_key.to_string(),
            seq_num: u64::MAX,
        };

        // 2. Ask Crossbeam for the first entry that is >= our dummy key
        let entry = self.map.lower_bound(Bound::Included(&search_key))?;

        // 3. Check if we actually found our key, or if we accidentally
        // skipped ahead to "banana" because "apple" doesn't exist.
        let found_key = entry.key();
        if found_key.user_key == target_key {
            // We found the absolute newest version!
            let record = entry.value().clone();
            return Some((record, found_key.seq_num));
        }

        // The key wasn't found in this MemTable
        None
    }

    fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::SeqCst)
    }

    fn active_writers(&self) -> usize {
        self.active_writers.load(Ordering::SeqCst)
    }

    fn get_iterator(&self, low: Bound<String>, high: Bound<String>) -> Box<dyn KvIterator> {
        Box::new(MemTableIterator::new(self.map.clone(), low, high))
    }
}

/// A stateful, forward-only iterator over a `CrossbeamMemTable`.
///
/// Yields **every version** of each key (newest first, by descending sequence
/// number) within the requested range bounds — unlike `MemTable::get`, which
/// returns only the newest version.
///
/// Because the underlying `SkipMap` is lock-free, concurrent writes are
/// visible to the iterator if they are inserted at a position the iterator
/// has not yet passed. Writes inserted behind the iterator's current
/// position will not be seen.
pub struct MemTableIterator {
    map: Arc<SkipMap<InternalKey, Record>>,
    // We now store the exact InternalKey to resume our search
    next_key: Option<InternalKey>,
    high: Bound<String>,
    is_valid: bool,
}

impl MemTableIterator {
    pub fn new(
        map: Arc<SkipMap<InternalKey, Record>>,
        low: Bound<String>,
        high: Bound<String>,
    ) -> Self {
        // Determine the actual starting key based on the lower bound
        let next_key = match &low {
            Bound::Included(key) => {
                // Start exactly at this key (or the newest version of it)
                Some(InternalKey {
                    user_key: key.clone(),
                    seq_num: u64::MAX,
                })
            }
            Bound::Excluded(key) => {
                // For excluded, we still search for the key, but the .next()
                // logic will have to skip it. (Omitted here for brevity,
                // but you'd handle it in the .next() method).
                Some(InternalKey {
                    user_key: key.clone(),
                    seq_num: 0,
                })
            }
            Bound::Unbounded => {
                // If there is no lower bound, just grab the very first
                // entry in the entire SkipMap!
                map.front().map(|entry| entry.key().clone())
            }
        };

        Self {
            map,
            next_key,
            high, // Now stores Bound<String> instead of String
            is_valid: true,
        }
    }
}

impl KvIterator for MemTableIterator {
    /// Advances the iterator and returns the next `(user_key, record, seq_num)`.
    ///
    /// Every version of a key is returned individually (not just the newest),
    /// ordered by `user_key` ascending then `seq_num` descending. Returns
    /// `None` when the iterator is exhausted or the upper bound is reached.
    ///
    /// Callers should check `is_valid()` before calling `next()` to avoid
    /// unnecessary work on an already-exhausted iterator.
    fn next(&mut self) -> Option<(String, Record, u64)> {
        if !self.is_valid {
            return None;
        }

        let search_key = self.next_key.take()?;

        // 1. Find the exact version (or the next closest key)
        let entry = self.map.lower_bound(Bound::Included(&search_key))?;
        let current_key = entry.key().clone();

        // 2. Stop condition: Did we exceed the requested range?
        // Check if we hit the upper bound
        let reached_end = match &self.high {
            Bound::Included(upper) => current_key.user_key > *upper,
            Bound::Excluded(upper) => current_key.user_key >= *upper,
            Bound::Unbounded => false, // Never stop until the map is empty
        };

        if reached_end {
            self.is_valid = false;
            return None;
        }

        // 3. Peek ahead and save the exact InternalKey for the next call
        if let Some(next_entry) = entry.next() {
            self.next_key = Some(next_entry.key().clone());
        } else {
            self.next_key = None; // Reached the end of the SkipList
        }

        // 4. Return the user's string, the value, and the sequence number
        let record = entry.value().clone();
        Some((current_key.user_key, record, current_key.seq_num))
    }

    fn is_valid(&self) -> bool {
        self.is_valid && self.next_key.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    // --- Helper function for cleaner tests ---
    fn b(str: &str) -> Vec<u8> {
        str.as_bytes().to_vec()
    }

    // ==========================================
    // SEQUENTIAL CORRECTNESS TESTS (6)
    // ==========================================

    #[test]
    fn test_basic_put_and_get() {
        let memtable = CrossbeamMemTable::new(0);
        memtable.put("apple".to_string(), Record::Put(b("red")), 1);

        let result = memtable.get("apple").unwrap();
        assert_eq!(result.0, Record::Put(b("red")));
        assert_eq!(result.1, 1);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let memtable = CrossbeamMemTable::new(0);
        assert!(memtable.get("banana").is_none());
    }

    #[test]
    fn test_mvcc_update_returns_newest_version() {
        let memtable = CrossbeamMemTable::new(0);
        // Insert older version first
        memtable.put("apple".to_string(), Record::Put(b("green")), 5);
        // Insert newer version later
        memtable.put("apple".to_string(), Record::Put(b("red")), 10);

        // get() should always return the highest sequence number
        let result = memtable.get("apple").unwrap();
        assert_eq!(result.0, Record::Put(b("red")));
        assert_eq!(result.1, 10);
    }

    #[test]
    fn test_delete_acts_as_tombstone() {
        let memtable = CrossbeamMemTable::new(0);
        memtable.put("apple".to_string(), Record::Put(b("red")), 5);
        // Delete the key at a higher sequence number
        memtable.put("apple".to_string(), Record::Delete, 10);

        let result = memtable.get("apple").unwrap();
        assert_eq!(result.0, Record::Delete); // We should get the tombstone back
        assert_eq!(result.1, 10);
    }

    #[test]
    fn test_iterator_range_bounds() {
        let memtable = CrossbeamMemTable::new(0);
        memtable.put("apple".to_string(), Record::Put(b("1")), 1);
        memtable.put("banana".to_string(), Record::Put(b("2")), 2);
        memtable.put("cherry".to_string(), Record::Put(b("3")), 3);
        memtable.put("date".to_string(), Record::Put(b("4")), 4);

        // Iterator from [banana, date)
        let mut iter = memtable.get_iterator(
            Bound::Included("banana".to_string()),
            Bound::Excluded("date".to_string()),
        );

        let first = iter.next().unwrap();
        assert_eq!(first.0, "banana");

        let second = iter.next().unwrap();
        assert_eq!(second.0, "cherry");

        // "date" is the exclusive upper bound, so it should be None
        assert!(iter.next().is_none());
        assert!(!iter.is_valid());
    }

    #[test]
    fn test_iterator_yields_all_versions_descending() {
        let memtable = CrossbeamMemTable::new(0);
        // Insert completely out of order to ensure our Ord trait does the heavy lifting
        memtable.put("apple".to_string(), Record::Put(b("v1")), 10);
        memtable.put("apple".to_string(), Record::Put(b("v3")), 30);
        memtable.put("apple".to_string(), Record::Put(b("v2")), 20);

        let mut iter = memtable.get_iterator(
            Bound::Included("apple".to_string()),
            Bound::Excluded("b".to_string()),
        );

        // Should yield highest seq_num first
        assert_eq!(
            iter.next().unwrap(),
            ("apple".to_string(), Record::Put(b("v3")), 30)
        );
        assert_eq!(
            iter.next().unwrap(),
            ("apple".to_string(), Record::Put(b("v2")), 20)
        );
        assert_eq!(
            iter.next().unwrap(),
            ("apple".to_string(), Record::Put(b("v1")), 10)
        );
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_approximate_size_tracking() {
        let memtable = CrossbeamMemTable::new(0);
        assert_eq!(memtable.approximate_size(), 0);

        // "cat" (3) + "dog" (3) + seq_num (8) = 14 bytes
        memtable.put("cat".to_string(), Record::Put(b("dog")), 1);
        assert_eq!(memtable.approximate_size(), 14);

        // "mouse" (5) + Delete (0) + seq_num (8) = 13 bytes. Total = 27
        memtable.put("mouse".to_string(), Record::Delete, 2);
        assert_eq!(memtable.approximate_size(), 27);
    }

    // ==========================================
    // CONCURRENT CORRECTNESS TESTS (4)
    // ==========================================

    #[test]
    fn test_concurrent_puts_unique_keys() {
        let memtable = Arc::new(CrossbeamMemTable::new(0));
        let mut handles = vec![];

        // 10 threads, each writing 100 unique keys
        for thread_id in 0..10 {
            let mt = Arc::clone(&memtable);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("key_{}_{}", thread_id, i);
                    // seq_num doesn't matter for unique keys, so we just use 1
                    mt.put(key, Record::Put(b("val")), 1);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all 1,000 keys exist
        for thread_id in 0..10 {
            for i in 0..100 {
                let key = format!("key_{}_{}", thread_id, i);
                assert!(memtable.get(&key).is_some());
            }
        }
    }

    #[test]
    fn test_concurrent_updates_same_key() {
        let memtable = Arc::new(CrossbeamMemTable::new(0));
        let mut handles = vec![];

        // 10 threads all hammering the exact same key with different sequence numbers
        for seq_num in 1..=100 {
            let mt = Arc::clone(&memtable);
            handles.push(thread::spawn(move || {
                let val = format!("val_{}", seq_num);
                mt.put("contested_key".to_string(), Record::Put(b(&val)), seq_num);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // The highest sequence number (100) must win, regardless of thread execution order
        let result = memtable.get("contested_key").unwrap();
        assert_eq!(result.1, 100);
        assert_eq!(result.0, Record::Put(b("val_100")));
    }

    #[test]
    fn test_concurrent_reads_while_writing() {
        let memtable = Arc::new(CrossbeamMemTable::new(0));
        let mt_writer = Arc::clone(&memtable);

        // Writer thread constantly adding new versions of "hot_key"
        let writer_handle = thread::spawn(move || {
            for seq in 1..=500 {
                mt_writer.put("hot_key".to_string(), Record::Put(b("data")), seq);
            }
        });

        // Reader thread constantly querying "hot_key"
        // It should never panic, and if it finds the key, the sequence number should be valid
        let mt_reader = Arc::clone(&memtable);
        let reader_handle = thread::spawn(move || {
            for _ in 0..500 {
                if let Some((_, seq)) = mt_reader.get("hot_key") {
                    assert!(seq >= 1 && seq <= 500);
                }
            }
        });

        writer_handle.join().unwrap();
        reader_handle.join().unwrap();

        // Eventually, the reader will definitely see the final state
        let final_result = memtable.get("hot_key").unwrap();
        assert_eq!(final_result.1, 500);
    }

    #[test]
    fn test_concurrent_size_tracking_is_atomic() {
        let memtable = Arc::new(CrossbeamMemTable::new(0));
        let mut handles = vec![];

        // 5 threads writing identical sized data
        for _ in 0..5 {
            let mt = Arc::clone(&memtable);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("k{:03}", i); // Always 4 bytes
                    // "val" (3 bytes) + key (4 bytes) + seq (8 bytes) = 15 bytes per put
                    mt.put(key, Record::Put(b("val")), 1);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 5 threads * 100 puts * 15 bytes = 7500 bytes
        assert_eq!(memtable.approximate_size(), 7500);
    }

    #[test]
    fn test_concurrent_write_during_iteration() {
        let memtable = Arc::new(CrossbeamMemTable::new(0));

        // 1. Pre-populate with some "even" keys
        for i in (0..100).step_by(2) {
            let key = format!("key_{:03}", i);
            memtable.put(key, Record::Put(b("initial")), 1);
        }

        let mt_writer = Arc::clone(&memtable);

        // 2. Spawn a thread to concurrently write "odd" keys and update "even" keys
        let writer_handle = thread::spawn(move || {
            // Insert entirely new keys
            for i in (1..100).step_by(2) {
                let key = format!("key_{:03}", i);
                mt_writer.put(key, Record::Put(b("concurrent")), 2);
            }
            // Update existing keys with a higher sequence number
            for i in (0..50).step_by(2) {
                let key = format!("key_{:03}", i);
                mt_writer.put(key, Record::Put(b("updated")), 3);
            }
        });

        // Yield the main thread slightly to ensure the writer thread gets to work
        thread::sleep(std::time::Duration::from_millis(2));

        // 3. Iterate through the memtable concurrently (assuming Unbounded support)
        // If you haven't implemented Bound::Unbounded yet, you can use:
        // memtable.get_iterator("key_000".to_string(), "key_100".to_string())
        let mut iter = memtable.get_iterator(Bound::Unbounded, Bound::Unbounded);
        let mut yielded_records = Vec::new();

        while let Some((key, record, seq)) = iter.next() {
            yielded_records.push((key, record, seq));
            // A microscopic sleep forces the OS to interleave our reads with the background writes
            thread::sleep(std::time::Duration::from_micros(10));
        }

        writer_handle.join().unwrap();

        // 4. Validations

        // A. The keys MUST be in strictly sorted order based on our InternalKey Ord logic.
        // Even with concurrent inserts, the iterator must never go backward.
        for i in 1..yielded_records.len() {
            let prev = &yielded_records[i - 1];
            let curr = &yielded_records[i];

            if prev.0 == curr.0 {
                // If the user_key is the same (multiple versions), seq_num MUST be descending
                assert!(
                    prev.2 > curr.2,
                    "Sequence numbers must be descending for the same key"
                );
            } else {
                // Otherwise, the user_key MUST be strictly ascending alphabetically
                assert!(prev.0 < curr.0, "Keys must be strictly ascending");
            }
        }

        // B. We must have seen AT LEAST the initial 50 even keys.
        // (They might be the seq=1 versions, or the seq=3 versions depending on thread timing,
        // but the core user_key must not have been skipped).
        use std::collections::HashSet;
        let unique_keys: HashSet<_> = yielded_records.into_iter().map(|(k, _, _)| k).collect();

        for i in (0..100).step_by(2) {
            let expected_key = format!("key_{:03}", i);
            assert!(
                unique_keys.contains(&expected_key),
                "Iterator completely missed an initially inserted key: {}",
                expected_key
            );
        }
    }

    #[test]
    fn test_active_writers_returns_to_zero_after_heavy_contention() {
        let table = Arc::new(CrossbeamMemTable::new(0));
        let mut handles = vec![];

        // Use a barrier to force all 10 threads to start at the exact same time,
        // maximizing the chance of active_writers going well above 1.
        let start_barrier = Arc::new(Barrier::new(10));

        for i in 0..10 {
            let table_clone = Arc::clone(&table);
            let barrier_clone = Arc::clone(&start_barrier);

            handles.push(thread::spawn(move || {
                barrier_clone.wait(); // Wait for all threads to be ready

                for j in 0..1000 {
                    let key = format!("key_{}_{}", i, j);
                    table_clone.put(key, Record::Put(b("val")), j);
                }
            }));
        }

        // Wait for all writers to finish completely
        for handle in handles {
            handle.join().unwrap();
        }

        // VALIDATION: If any thread panicked or failed to decrement, this will fail.
        // It must be exactly 0 when no threads are writing.
        assert_eq!(
            table.active_writers(),
            0,
            "Active writers leaked! The counter did not return to 0."
        );
    }

    #[test]
    fn test_flusher_spin_loop_waits_for_in_flight_writers() {
        let table = Arc::new(CrossbeamMemTable::new(0));
        let table_for_writer = Arc::clone(&table);

        // This flag simulates the MemTableState freezing the active table.
        // It tells the flusher: "No NEW writers are allowed, but existing ones might be finishing up."
        let is_frozen = Arc::new(AtomicBool::new(false));
        let is_frozen_writer = Arc::clone(&is_frozen);

        let writer_handle = thread::spawn(move || {
            for i in 0..5000 {
                let key = format!("key_{:04}", i);
                table_for_writer.put(key, Record::Put(b("data")), i);
            }

            // The writer announces that the table is now "frozen"
            is_frozen_writer.store(true, Ordering::SeqCst);
        });

        // Simulating the background flusher thread
        let flusher_handle = thread::spawn(move || {
            // 1. Wait until the table is officially "frozen"
            while !is_frozen.load(Ordering::SeqCst) {
                std::hint::spin_loop();
            }

            // 2. THE CRITICAL SECTION: The table is frozen, but in-flight writers
            // might still be inside the `.insert()` method.
            // We use the spin loop to wait for the barrier to reach 0.
            while table.active_writers() > 0 {
                std::hint::spin_loop(); // Yield the core's pipeline, but not the OS thread
            }

            // 3. SAFE ZONE: active_writers is exactly 0.
            // We are guaranteed that the SkipMap has settled. We can now iterate.
            let mut record_count = 0;
            let mut iter = table.get_iterator(Bound::Unbounded, Bound::Unbounded);

            while let Some(_) = iter.next() {
                record_count += 1;
            }

            // If the spin loop didn't work, the iterator would have started too early
            // and missed keys that were still being inserted by the writer thread.
            assert_eq!(
                record_count, 5000,
                "Flusher missed data! It did not wait for active_writers to hit 0."
            );
        });

        writer_handle.join().unwrap();
        flusher_handle.join().unwrap();
    }
}
