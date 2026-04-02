use std::sync::{Arc, RwLock};

use crate::memtable::{CrossbeamMemTable, MemTable, QueryResult, Record};

#[derive(Clone)]
struct InnerState {
    active: Arc<CrossbeamMemTable>,
    immutable: Vec<Arc<CrossbeamMemTable>>,
    max_immutable_tables: usize,
}

// Holds state for all memtables
pub struct MemTableState {
    inner: RwLock<Arc<InnerState>>,
    max_memtable_size: usize,
}

impl MemTableState {
    pub fn new(max_immutable_tables: usize, max_memtable_size: usize) -> Self {
        let initial_state = InnerState {
            active: Arc::new(CrossbeamMemTable::new()),
            immutable: Vec::new(),
            max_immutable_tables
        };
        Self {
            inner: RwLock::new(Arc::new(initial_state)),
            max_memtable_size,
        }
    }

    /// Searches the active MemTable first, then the immutable queue.
    pub fn get(&self, key: &str) -> QueryResult {
        let guard = self.inner.read().unwrap();
        // 1. Check the active, mutable MemTable
        if let Some(result) = guard.active.get(key) {
            return Some(result);
        }

        // 2. Check the immutable MemTables in reverse chronological order
        // (Assuming the vector is appended to, we iterate in reverse)
        for table in guard.immutable.iter().rev() {
            if let Some(result) = table.get(key) {
                return Some(result);
            }
        }

        // Not found in any MemTable
        None
    }

    pub fn put(&self, key: String, record: Record, seq_num: u64) -> bool {
        // 1. Get the current active table (readers drop the lock instantly)
        let active_table = {
            let guard = self.inner.read().unwrap();
            Arc::clone(&guard.active)
        };

        // 2. Write the data (lock-free!)
        active_table.put(key, record, seq_num);

        // 3. Internal capacity check
        if active_table.approximate_size() >= self.max_memtable_size {
            // 4. Handle its own freezing process internally!
            self.freeze_active(&active_table) 
        } else {
            false
        }
    }

    /// Rotates the active MemTable into the immutable queue.
    /// Handles its own swapping and locking internally.
    fn freeze_active(&self, expected_active: &Arc<CrossbeamMemTable>) -> bool {
        let mut guard = self.inner.write().unwrap();
        
        if Arc::ptr_eq(&guard.active, expected_active) {
            let mut new_state = (**guard).clone();
            new_state.immutable.push(new_state.active.clone());
            new_state.active = Arc::new(CrossbeamMemTable::new());
            
            *guard = Arc::new(new_state);
            true // We successfully rotated
        } else {
            false // Another thread beat us
        }
    }

    /// Checks if we need to stall writes because the disk is too slow.
    pub fn is_flush_falling_behind(&self) -> bool {
        let guard = self.inner.read().unwrap();
        guard.immutable.len() >= guard.max_immutable_tables
    }

    /// Returns a clone of the Arc pointing to the oldest immutable MemTable.
    /// Returns None if there are no immutable tables waiting to be flushed.
    pub fn get_oldest_immutable(&self) -> Option<Arc<CrossbeamMemTable>> {
        let guard = self.inner.read().unwrap();
        
        // The oldest table is at the front of the vector (index 0)
        guard.immutable.first().map(|table_arc| Arc::clone(table_arc))
    }

    /// Safely removes a specific MemTable from the immutable list using CoW.
    /// This should ONLY be called after the table is safely flushed to disk.
    pub fn drop_immutable(&self, table_to_drop: &Arc<CrossbeamMemTable>) {
        let mut guard = self.inner.write().unwrap();

        // 1. Check if the oldest table is actually the one we just flushed
        if let Some(oldest) = guard.immutable.first() {
            if Arc::ptr_eq(oldest, table_to_drop) {
                
                // 2. The CoW Mutation
                let mut new_state = (**guard).clone();
                
                // Remove the flushed table from the front of the queue
                new_state.immutable.remove(0); 
                
                // 3. The Swap
                *guard = Arc::new(new_state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    // --- Helper function for cleaner tests ---
    fn b(str: &str) -> Vec<u8> {
        str.as_bytes().to_vec()
    }

    // ==========================================
    // SEQUENTIAL CORRECTNESS TESTS (6)
    // ==========================================

    #[test]
    fn test_new_state_is_empty() {
        let state = MemTableState::new(4, 1024);
        assert!(state.get("apple").is_none());
    }

    #[test]
    fn test_put_and_get_active_table() {
        let state = MemTableState::new(4, 1024);
        
        let needs_flush = state.put("apple".to_string(), Record::Put(b("red")), 1);
        assert!(!needs_flush, "Should not require flush well under capacity");

        let result = state.get("apple").unwrap();
        assert_eq!(result.0, Record::Put(b("red")));
        assert_eq!(result.1, 1);
    }

    #[test]
    fn test_automatic_freeze_on_capacity() {
        // Set a tiny capacity: 50 bytes
        let state = MemTableState::new(4, 50);
        
        // Write #1: "apple" (5) + "red" (3) + seq (8) = 16 bytes. (Under capacity)
        let flush1 = state.put("apple".to_string(), Record::Put(b("red")), 1);
        assert!(!flush1);

        // Write #2: "banana" (6) + "yellow" (6) + seq (8) = 20 bytes. (Total 36, Under capacity)
        let flush2 = state.put("banana".to_string(), Record::Put(b("yellow")), 2);
        assert!(!flush2);

        // Write #3: "cherry" (6) + "red" (3) + seq (8) = 17 bytes. (Total 53, OVER capacity)
        let flush3 = state.put("cherry".to_string(), Record::Put(b("red")), 3);
        assert!(flush3, "Crossing the 50-byte threshold should trigger a flush");

        // Write #4: "date" (4) + "brown" (5) + seq (8) = 17 bytes. (New table, under capacity)
        let flush4 = state.put("date".to_string(), Record::Put(b("brown")), 4);
        assert!(!flush4, "The new active table should be empty and not trigger a flush");
    }

    #[test]
    fn test_get_traverses_immutable_tables() {
        let state = MemTableState::new(4, 50);
        
        // Put in active, then force it to freeze
        state.put("old_key".to_string(), Record::Put(b("v1")), 1);
        
        // Push it over the limit to freeze it
        state.put("filler".to_string(), Record::Put(b("data_that_takes_up_lots_of_space_to_force_freeze")), 2);

        // Put a new key in the NEW active table
        state.put("new_key".to_string(), Record::Put(b("v2")), 3);

        // We should be able to get both
        assert_eq!(state.get("new_key").unwrap().0, Record::Put(b("v2")));
        assert_eq!(state.get("old_key").unwrap().0, Record::Put(b("v1"))); // Found in immutable!
    }

    #[test]
    fn test_get_returns_newest_version_across_tables() {
        let state = MemTableState::new(4, 50);
        
        // Version 1 goes into the first table
        state.put("apple".to_string(), Record::Put(b("green")), 1);
        
        // Force a freeze
        state.put("filler".to_string(), Record::Put(b("data_that_takes_up_lots_of_space_to_force_freeze")), 2);

        // Version 2 goes into the new active table
        state.put("apple".to_string(), Record::Put(b("red")), 3);

        // get() checks the active table first, so it should find "red" and never see "green"
        let result = state.get("apple").unwrap();
        assert_eq!(result.0, Record::Put(b("red")));
        assert_eq!(result.1, 3);
    }

    #[test]
    fn test_get_returns_tombstone_masking_immutable_data() {
        let state = MemTableState::new(4, 50);
        
        // Insert value, force freeze
        state.put("apple".to_string(), Record::Put(b("red")), 1);
        state.put("filler".to_string(), Record::Put(b("data_that_takes_up_lots_of_space_to_force_freeze")), 2);

        // Delete the key in the new active table
        state.put("apple".to_string(), Record::Delete, 3);

        // The active table's tombstone should mask the immutable table's value
        let result = state.get("apple").unwrap();
        assert_eq!(result.0, Record::Delete);
        assert_eq!(result.1, 3);
    }

    // ==========================================
    // CONCURRENT CORRECTNESS TESTS (4)
    // ==========================================

    #[test]
    fn test_concurrent_puts_no_freeze() {
        let state = Arc::new(MemTableState::new(4, 1_000_000)); // Huge capacity
        let mut handles = vec![];

        for i in 0..10 {
            let state_clone = Arc::clone(&state);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key_{}_{}", i, j);
                    let needs_flush = state_clone.put(key, Record::Put(b("val")), 1);
                    assert!(!needs_flush);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all 1,000 keys made it into the active table
        for i in 0..10 {
            for j in 0..100 {
                let key = format!("key_{}_{}", i, j);
                assert!(state.get(&key).is_some());
            }
        }
    }

    #[test]
    fn test_concurrent_readers_and_writers_isolation() {
        let state = Arc::new(MemTableState::new(4, 100_000));
        state.put("shared_key".to_string(), Record::Put(b("v1")), 1);

        let state_writer = Arc::clone(&state);
        let writer_handle = thread::spawn(move || {
            for seq in 2..=500 {
                state_writer.put("shared_key".to_string(), Record::Put(b("v_new")), seq);
            }
        });

        let state_reader = Arc::clone(&state);
        let reader_handle = thread::spawn(move || {
            for _ in 0..500 {
                // The reader should never panic or deadlock while getting data, 
                // thanks to the RwLock + Arc Copy-on-Write pattern.
                let result = state_reader.get("shared_key");
                assert!(result.is_some());
            }
        });

        writer_handle.join().unwrap();
        reader_handle.join().unwrap();
        
        let final_result = state.get("shared_key").unwrap();
        assert_eq!(final_result.1, 500);
    }

    #[test]
    fn test_thundering_herd_freeze_exactly_once() {
        // We set capacity to 1000 bytes. 
        // We will fire 10 threads that each write 200 bytes simultaneously.
        // Total bytes written will be ~2000. 
        // The threshold should be crossed exactly once (or maybe twice depending on thread scheduling, 
        // but the key is that multiple threads don't ALL return true for a single crossing).
        
        let state = Arc::new(MemTableState::new(4, 1000));
        let mut handles = vec![];

        for i in 0..10 {
            let state_clone = Arc::clone(&state);
            handles.push(thread::spawn(move || {
                // A large 200 byte value
                let large_val = vec![0u8; 200]; 
                let key = format!("key_{}", i);
                
                // Returns true if THIS thread was the one to successfully rotate the state
                state_clone.put(key, Record::Put(large_val), 1)
            }));
        }

        let mut total_flushes_triggered = 0;
        for handle in handles {
            if handle.join().unwrap() {
                total_flushes_triggered += 1;
            }
        }

        // Because of the double-checked locking, even if 5 threads realize the size is > 1000
        // at the exact same time, only ONE of them will win the pointer swap and return true.
        // (It's possible it triggers twice if threads interleave perfectly across the 2000 byte total,
        // but it will absolutely not be 10).
        assert!(
            total_flushes_triggered >= 1 && total_flushes_triggered <= 2,
            "Thundering herd failed! Flushes triggered: {}", 
            total_flushes_triggered
        );
    }

    #[test]
    fn test_state_snapshot_isolation_during_freeze() {
        let state = Arc::new(MemTableState::new(4, 50));
        
        // Insert initial data
        state.put("keep_me".to_string(), Record::Put(b("v1")), 1);

        // Manually simulate a reader grabbing the inner state directly 
        // (This tests the CoW architecture's memory safety guarantees)
        let snapshot = {
            let guard = state.inner.read().unwrap();
            Arc::clone(&*guard)
        };

        // Writer thread forces a freeze and pushes the state way past the old limits
        let state_writer = Arc::clone(&state);
        let writer = thread::spawn(move || {
            state_writer.put("force_freeze_1".to_string(), Record::Put(vec![0; 100]), 2);
            state_writer.put("force_freeze_2".to_string(), Record::Put(vec![0; 100]), 3);
        });

        writer.join().unwrap();

        // VALIDATION:
        // The reader's snapshot should STILL point to the original memory layout.
        // It should have 1 active table (which was never frozen from the snapshot's perspective)
        // and 0 immutable tables.
        assert_eq!(snapshot.immutable.len(), 0, "Snapshot should be completely isolated from the writer's freeze");
        
        // The global state, however, should have moved that active table to immutables
        let global_state = state.inner.read().unwrap();
        assert!(global_state.immutable.len() >= 1, "Global state should reflect the freeze");
    }

    #[test]
    fn test_get_oldest_immutable_empty_state() {
        // Initialize state with a small 50-byte limit
        let state = MemTableState::new(4, 50);
        
        // If there are no frozen tables, it should safely return None
        assert!(state.get_oldest_immutable().is_none());
    }

    #[test]
    fn test_get_and_drop_oldest_immutable_standard_flow() {
        let state = MemTableState::new(4, 50);
        
        // 1. Force a freeze by inserting a large record
        let is_frozen = state.put("big_key".to_string(), Record::Put(vec![0; 100]), 1);
        assert!(is_frozen, "Table should have frozen due to capacity");

        // 2. Flusher wakes up and gets the oldest immutable table
        let table_to_flush = state.get_oldest_immutable()
            .expect("Should have one immutable table");

        // Internally verify the vector has 1 element
        assert_eq!(state.inner.read().unwrap().immutable.len(), 1);

        // 3. Flusher finishes writing to disk and drops it
        state.drop_immutable(&table_to_flush);

        // 4. Verify it was safely removed from RAM
        assert_eq!(state.inner.read().unwrap().immutable.len(), 0);
        assert!(state.get_oldest_immutable().is_none());
    }

    #[test]
    fn test_drop_immutable_double_checked_locking_safety() {
        let state = MemTableState::new(4, 50);
        
        // Force TWO freezes to create a queue of immutable tables
        state.put("key_1".to_string(), Record::Put(vec![0; 100]), 1); // Oldest (Index 0)
        state.put("key_2".to_string(), Record::Put(vec![0; 100]), 2); // Newest Immutable (Index 1)

        assert_eq!(state.inner.read().unwrap().immutable.len(), 2);

        // Flusher grabs the oldest one
        let oldest_table = state.get_oldest_immutable().unwrap();

        // SIMULATE A MISTAKE OR ROGUE THREAD: 
        // Create an entirely unrelated table that is NOT in the list
        let random_table = Arc::new(CrossbeamMemTable::new());
        state.drop_immutable(&random_table);

        // VALIDATION 1: The state should ignore the drop because `ptr_eq` failed
        assert_eq!(
            state.inner.read().unwrap().immutable.len(), 
            2, 
            "State dropped a table it shouldn't have!"
        );

        // Now do the correct drop
        state.drop_immutable(&oldest_table);

        // VALIDATION 2: The oldest was dropped, length is now 1
        assert_eq!(state.inner.read().unwrap().immutable.len(), 1);
    }

    #[test]
    fn test_concurrent_idempotent_drops() {
        // This tests the edge case where multiple background threads (or a retried task)
        // attempt to drop the exact same table at the same time.
        let state = Arc::new(MemTableState::new(4, 50));
        
        state.put("key_1".to_string(), Record::Put(vec![0; 100]), 1); // Freeze 1
        state.put("key_2".to_string(), Record::Put(vec![0; 100]), 2); // Freeze 2

        let table_to_drop = state.get_oldest_immutable().unwrap();
        
        let mut handles = vec![];

        // Spawn 10 threads that all try to drop the exact same table simultaneously
        for _ in 0..10 {
            let state_clone = Arc::clone(&state);
            let table_clone = Arc::clone(&table_to_drop);
            
            handles.push(thread::spawn(move || {
                state_clone.drop_immutable(&table_clone);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // VALIDATION: Because `drop_immutable` uses `Arc::ptr_eq` against the front
        // of the vector, only the FIRST thread to acquire the lock will succeed. 
        // The other 9 threads will see that the table at index 0 is no longer the 
        // one they are trying to drop, and will safely do nothing.
        assert_eq!(
            state.inner.read().unwrap().immutable.len(), 
            1, 
            "Concurrent drops caused too many tables to be removed!"
        );
    }
}