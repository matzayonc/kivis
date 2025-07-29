/*!
 * Hash Key Tests
 *
 * This test file demonstrates how to create records where the key is derived from
 * the hash of the record's content. This is useful for:
 *
 * - Content-addressable storage
 * - Deduplication (identical content gets the same key)
 * - Deterministic keys based on content
 *
 * The implementation uses:
 * - Custom key type (ContentHashKey) that wraps a hash value
 * - DeriveKey trait implementation that computes the hash
 * - Database.insert() method for records with derived keys
 */

use kivis::{Database, DeriveKey, KeyBytes, MemoryStorage, RecordKey, DatabaseEntry};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A hash key that uses the hash of the record content as the key
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ContentHashKey(pub u64);

impl RecordKey for ContentHashKey {
    type Record = ContentRecord;
}

// ContentHashKey automatically implements KeyBytes through the blanket implementation

/// A record that uses its content hash as the key
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct ContentRecord {
    data: String,
    value: u32,
}

impl DeriveKey for ContentRecord {
    type Key = ContentHashKey;

    fn key(record: &<Self::Key as RecordKey>::Record) -> Self::Key {
        let mut hasher = DefaultHasher::new();
        record.hash(&mut hasher);
        ContentHashKey(hasher.finish())
    }
}

impl DatabaseEntry for ContentRecord {
    const SCOPE: u8 = 100;
    type Key = ContentHashKey;

    fn index_keys(&self) -> Vec<(u8, &dyn KeyBytes)> {
        vec![]
    }
}

#[test]
fn test_hash_key_storage_and_retrieval() {
    let mut store = Database::new(MemoryStorage::new());

    // Create a record
    let record = ContentRecord {
        data: "hello world".to_string(),
        value: 42,
    };

    // Calculate the expected hash key
    let expected_key = ContentRecord::key(&record);

    // Store the record using insert() for hash-based keys
    let stored_key = store.insert(record.clone()).unwrap();

    // The stored key should match our expected hash
    assert_eq!(stored_key, expected_key);

    // Retrieve the record using the key
    let retrieved = store.get(&stored_key).unwrap();
    assert_eq!(retrieved, Some(record.clone()));

    // Test that the same content always produces the same hash
    let record2 = ContentRecord {
        data: "hello world".to_string(),
        value: 42,
    };
    let key2 = ContentRecord::key(&record2);
    assert_eq!(stored_key, key2);
}

#[test]
fn test_hash_key_uniqueness() {
    let mut store = Database::new(MemoryStorage::new());

    // Create two different records
    let record1 = ContentRecord {
        data: "first record".to_string(),
        value: 1,
    };

    let record2 = ContentRecord {
        data: "second record".to_string(),
        value: 2,
    };

    // Store both records using insert() for hash-based keys
    let key1 = store.insert(record1.clone()).unwrap();
    let key2 = store.insert(record2.clone()).unwrap();

    // Keys should be different
    assert_ne!(key1, key2);

    // Both records should be retrievable
    assert_eq!(store.get(&key1).unwrap(), Some(record1));
    assert_eq!(store.get(&key2).unwrap(), Some(record2));
}

#[test]
fn test_hash_key_removal() {
    let mut store = Database::new(MemoryStorage::new());

    let record = ContentRecord {
        data: "test removal".to_string(),
        value: 999,
    };

    let key = store.insert(record.clone()).unwrap();

    // Verify record exists
    assert_eq!(store.get(&key).unwrap(), Some(record));

    // Remove the record
    store.remove(&key).unwrap();

    // Verify record no longer exists
    assert_eq!(store.get(&key).unwrap(), None);
}

#[test]
fn test_hash_deterministic() {
    // Test that the same content always produces the same hash across multiple runs
    let record = ContentRecord {
        data: "deterministic test".to_string(),
        value: 777,
    };

    let hash1 = ContentRecord::key(&record);
    let hash2 = ContentRecord::key(&record);
    let hash3 = ContentRecord::key(&record);

    assert_eq!(hash1, hash2);
    assert_eq!(hash2, hash3);
}
