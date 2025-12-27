// Example showing how to use the AtomicStorage trait
// This example requires the "atomic" feature to be enabled

#[cfg(feature = "atomic")]
fn atomic_storage_example() -> anyhow::Result<()> {
    use bincode::{
        config::Configuration,
        error::{DecodeError, EncodeError},
    };
    use kivis::{AtomicStorage, Storage};
    use std::{cmp::Reverse, collections::BTreeMap, fmt::Display, ops::Range};

    // Define a custom error type
    #[derive(Debug, PartialEq, Eq)]
    enum MyError {
        Serialization,
        Deserialization,
    }

    impl Display for MyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Serialization => write!(f, "Serialization error"),
                Self::Deserialization => write!(f, "Deserialization error"),
            }
        }
    }

    impl From<EncodeError> for MyError {
        fn from(_: EncodeError) -> Self {
            Self::Serialization
        }
    }

    impl From<DecodeError> for MyError {
        fn from(_: DecodeError) -> Self {
            Self::Deserialization
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

    // Implement the Storage trait
    impl Storage for MyAtomicStorage {
        type Serializer = Configuration;
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
        ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError>
        {
            let reverse_range = Reverse(range.end)..Reverse(range.start);
            let iter = self.data.range(reverse_range);
            Ok(iter.map(|(k, _v)| Ok(k.0.clone())))
        }
    }

    // Then implement the AtomicStorage trait
    impl AtomicStorage for MyAtomicStorage {
        fn batch_mixed(
            &mut self,
            inserts: Vec<(Vec<u8>, Vec<u8>)>,
            removes: Vec<Vec<u8>>,
        ) -> Result<Vec<Option<Vec<u8>>>, Self::StoreError> {
            let mut removed = Vec::new();
            for (key, value) in inserts {
                self.data.insert(Reverse(key), value);
            }
            for key in removes {
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
    storage.batch_mixed(operations, Vec::new()).unwrap();

    // All removals happen atomically
    let keys_to_remove = vec![b"user1".to_vec(), b"user3".to_vec()];
    let removed = storage.batch_mixed(Vec::new(), keys_to_remove).unwrap();

    println!("Removed values: {:?}", removed);
    Ok(())
}

fn main() -> anyhow::Result<()> {
    #[cfg(feature = "atomic")]
    atomic_storage_example()?;

    #[cfg(not(feature = "atomic"))]
    println!(
        "This example requires the 'atomic' feature to be enabled. Run with: cargo run --example atomic_storage --features atomic"
    );

    Ok(())
}
