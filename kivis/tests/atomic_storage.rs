// Example demonstrating the Storage trait with batch_mixed
#[cfg(feature = "atomic")]
#[cfg(test)]
mod atomic_storage_example {
    use bincode::{
        config::Configuration,
        error::{DecodeError, EncodeError},
    };
    use kivis::Storage;
    use std::{cmp::Reverse, collections::BTreeMap, error::Error, fmt::Display, ops::Range};

    // Example error type for our mock atomic storage
    #[derive(Debug, PartialEq, Eq)]
    pub enum MockAtomicError {
        Storage(String),
        Serialization,
        Deserialization,
    }

    impl Display for MockAtomicError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Storage(s) => write!(f, "MockAtomicError: {s}"),
                Self::Serialization => write!(f, "Serialization error"),
                Self::Deserialization => write!(f, "Deserialization error"),
            }
        }
    }

    impl From<EncodeError> for MockAtomicError {
        fn from(_: EncodeError) -> Self {
            Self::Serialization
        }
    }

    impl From<DecodeError> for MockAtomicError {
        fn from(_: DecodeError) -> Self {
            Self::Deserialization
        }
    }

    impl Error for MockAtomicError {}

    // Mock atomic storage implementation
    pub struct MockAtomicStorage {
        data: BTreeMap<Reverse<Vec<u8>>, Vec<u8>>,
        fail_next: bool, // For testing failure scenarios
    }

    impl MockAtomicStorage {
        pub fn new() -> Self {
            Self {
                data: BTreeMap::new(),
                fail_next: false,
            }
        }

        pub fn set_fail_next(&mut self, fail: bool) {
            self.fail_next = fail;
        }
    }

    impl Storage for MockAtomicStorage {
        type Serializer = Configuration;
        type StoreError = MockAtomicError;

        fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::StoreError> {
            if self.fail_next {
                self.fail_next = false;
                return Err(MockAtomicError::Storage("Simulated failure".to_string()));
            }
            self.data.insert(Reverse(key.to_vec()), value.to_vec());
            Ok(())
        }

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::StoreError> {
            Ok(self.data.get(&Reverse(key.to_vec())).cloned())
        }

        fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::StoreError> {
            if self.fail_next {
                self.fail_next = false;
                return Err(MockAtomicError::Storage("Simulated failure".to_string()));
            }
            Ok(self.data.remove(&Reverse(key.to_vec())))
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

        // Override batch_mixed for atomic behavior
        fn batch_mixed(
            &mut self,
            inserts: Vec<(&[u8], &[u8])>,
            removes: Vec<&[u8]>,
        ) -> Result<Vec<Option<Vec<u8>>>, Self::StoreError> {
            if self.fail_next {
                return Err(MockAtomicError::Storage(
                    "Batch mixed operation failed".to_string(),
                ));
            }

            // In a real implementation, this would be atomic
            // First collect removed values
            let mut removed = Vec::new();
            for key in removes {
                removed.push(self.data.remove(&Reverse(key.to_vec())));
            }

            // Then insert new values
            for (key, value) in inserts {
                self.data.insert(Reverse(key.to_vec()), value.to_vec());
            }

            Ok(removed)
        }
    }

    #[test]
    fn test_atomic_batch_mixed_inserts_only() -> anyhow::Result<()> {
        let mut storage = MockAtomicStorage::new();

        let inserts = vec![
            (&b"key1"[..], &b"value1"[..]),
            (&b"key2"[..], &b"value2"[..]),
            (&b"key3"[..], &b"value3"[..]),
        ];

        // Test batch mixed with only inserts
        let removed = storage.batch_mixed(inserts, vec![])?;
        assert!(removed.is_empty());

        // Verify all values were inserted
        assert_eq!(storage.get(&b"key1"[..])?, Some(b"value1".to_vec()));
        assert_eq!(storage.get(&b"key2"[..])?, Some(b"value2".to_vec()));
        assert_eq!(storage.get(&b"key3"[..])?, Some(b"value3".to_vec()));
        Ok(())
    }

    #[test]
    fn test_atomic_batch_mixed_removes_only() -> anyhow::Result<()> {
        let mut storage = MockAtomicStorage::new();

        // Insert some test data
        storage.insert(&b"key1"[..], &b"value1"[..])?;
        storage.insert(&b"key2"[..], &b"value2"[..])?;
        storage.insert(&b"key3"[..], &b"value3"[..])?;

        let keys_to_remove = vec![&b"key1"[..], &b"key2"[..], &b"nonexistent"[..]];

        // Test batch mixed with only removes
        let removed = storage.batch_mixed(vec![], keys_to_remove)?;

        // Verify return values
        assert_eq!(removed[0], Some(b"value1".to_vec()));
        assert_eq!(removed[1], Some(b"value2".to_vec()));
        assert_eq!(removed[2], None); // nonexistent key

        // Verify keys were actually removed
        assert_eq!(storage.get(&b"key1"[..])?, None);
        assert_eq!(storage.get(&b"key2"[..])?, None);
        assert_eq!(storage.get(&b"key3"[..])?, Some(b"value3".to_vec()));
        Ok(())
    }

    #[test]
    fn test_atomic_batch_mixed() -> anyhow::Result<()> {
        let mut storage = MockAtomicStorage::new();

        // Insert some initial data
        storage.insert(&b"existing1"[..], &b"value1"[..])?;
        storage.insert(&b"existing2"[..], &b"value2"[..])?;

        let inserts = vec![
            (&b"new1"[..], &b"newvalue1"[..]),
            (&b"new2"[..], &b"newvalue2"[..]),
        ];

        let removes = vec![&b"existing1"[..], &b"nonexistent"[..]];

        // Test mixed operations
        let removed = storage.batch_mixed(inserts, removes)?;

        // Verify removals
        assert_eq!(removed[0], Some(b"value1".to_vec()));
        assert_eq!(removed[1], None);

        // Verify inserts
        assert_eq!(storage.get(&b"new1"[..])?, Some(b"newvalue1".to_vec()));
        assert_eq!(storage.get(&b"new2"[..])?, Some(b"newvalue2".to_vec()));

        // Verify removes
        assert_eq!(storage.get(&b"existing1"[..])?, None);
        assert_eq!(storage.get(&b"existing2"[..])?, Some(b"value2".to_vec()));
        Ok(())
    }

    #[test]
    fn test_atomic_failure_handling() -> anyhow::Result<()> {
        let mut storage = MockAtomicStorage::new();

        storage.set_fail_next(true);

        let inserts = vec![(&b"key1"[..], &b"value1"[..])];

        // Test that failure is properly returned
        let result = storage.batch_mixed(inserts, vec![]);
        assert!(result.is_err());
        assert_eq!(
            result,
            Err(MockAtomicError::Storage(
                "Batch mixed operation failed".to_string()
            ))
        );
        Ok(())
    }
}
