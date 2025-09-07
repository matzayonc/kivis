// Example showing how to use the AtomicStorage trait
// This example requires the "atomic" feature to be enabled

#[cfg(feature = "atomic")]
fn atomic_storage_example() {
    use kivis::{AtomicStorage, Storage};
    use std::{collections::BTreeMap, cmp::Reverse, fmt::Display, ops::Range};

    // Define a custom error type
    #[derive(Debug, PartialEq, Eq)]
    struct MyError(String);

    impl Display for MyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MyError: {}", self.0)
        }
    }

    // A storage implementation that supports atomic operations
    struct MyAtomicStorage {
        data: BTreeMap<Reverse<Vec<u8>>, Vec<u8>>,
    }

    impl MyAtomicStorage {
        fn new() -> Self {
            Self {
                data: BTreeMap::new(),
            }
        }
    }

    // First implement the basic Storage trait
    impl Storage for MyAtomicStorage {
        type StoreError = MyError;

        fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError> {
            self.data.insert(Reverse(key), value);
            Ok(())
        }

        fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
            Ok(self.data.get(&Reverse(key)).cloned())
        }

        fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
            Ok(self.data.remove(&Reverse(key)))
        }

        fn iter_keys(
            &self,
            range: Range<Vec<u8>>,
        ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError> {
            let reverse_range = Reverse(range.end)..Reverse(range.start);
            let iter = self.data.range(reverse_range);
            Ok(iter.map(|(k, _v)| Ok(k.0.clone())))
        }
    }

    // Then implement the AtomicStorage trait
    impl AtomicStorage for MyAtomicStorage {
        fn batch_insert(&mut self, operations: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), Self::StoreError> {
            // In a real implementation, this would be atomic
            // For example, using database transactions or write-ahead logging
            for (key, value) in operations {
                self.data.insert(Reverse(key), value);
            }
            Ok(())
        }

        fn batch_remove(&mut self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>, Self::StoreError> {
            // In a real implementation, this would be atomic
            let mut removed = Vec::new();
            for key in keys {
                removed.push(self.data.remove(&Reverse(key)));
            }
            Ok(removed)
        }

        // batch_mixed has a default implementation that calls batch_remove then batch_insert
        // You can override it if you need different behavior
    }

    // Usage example
    let mut storage = MyAtomicStorage::new();

    // Perform atomic batch operations
    let operations = vec![
        (b"user1".to_vec(), b"Alice".to_vec()),
        (b"user2".to_vec(), b"Bob".to_vec()),
        (b"user3".to_vec(), b"Charlie".to_vec()),
    ];

    // All insertions happen atomically
    storage.batch_insert(operations).unwrap();

    // All removals happen atomically
    let keys_to_remove = vec![b"user1".to_vec(), b"user3".to_vec()];
    let removed = storage.batch_remove(keys_to_remove).unwrap();

    println!("Removed values: {:?}", removed);
}

fn main() {
    #[cfg(feature = "atomic")]
    atomic_storage_example();
    
    #[cfg(not(feature = "atomic"))]
    println!("This example requires the 'atomic' feature to be enabled. Run with: cargo run --example atomic_storage --features atomic");
}
