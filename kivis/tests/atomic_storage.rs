// Example demonstrating the AtomicStorage trait
#[cfg(feature = "atomic")]
#[cfg(test)]
mod atomic_storage_example {
    use kivis::{AtomicStorage, Storage};
    use std::{cmp::Reverse, collections::BTreeMap, fmt::Display, ops::Range};

    // Example error type for our mock atomic storage
    #[derive(Debug, PartialEq, Eq)]
    pub struct MockAtomicError(String);

    impl Display for MockAtomicError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockAtomicError: {}", self.0)
        }
    }

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
        type StoreError = MockAtomicError;

        fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError> {
            if self.fail_next {
                self.fail_next = false;
                return Err(MockAtomicError("Simulated failure".to_string()));
            }
            self.data.insert(Reverse(key), value);
            Ok(())
        }

        fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
            Ok(self.data.get(&Reverse(key)).cloned())
        }

        fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
            if self.fail_next {
                self.fail_next = false;
                return Err(MockAtomicError("Simulated failure".to_string()));
            }
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

    impl AtomicStorage for MockAtomicStorage {
        fn batch_mixed(
            &mut self,
            inserts: Vec<(Vec<u8>, Vec<u8>)>,
            removes: Vec<Vec<u8>>,
        ) -> Result<Vec<Option<Vec<u8>>>, Self::StoreError> {
            if self.fail_next {
                return Err(MockAtomicError("Batch mixed operation failed".to_string()));
            }

            // In a real implementation, this would be atomic
            // First collect removed values
            let mut removed = Vec::new();
            for key in removes {
                removed.push(self.data.remove(&Reverse(key)));
            }

            // Then insert new values
            for (key, value) in inserts {
                self.data.insert(Reverse(key), value);
            }

            Ok(removed)
        }
    }

    #[test]
    fn test_atomic_batch_mixed_inserts_only() {
        let mut storage = MockAtomicStorage::new();

        let inserts = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        // Test batch mixed with only inserts
        let removed = storage.batch_mixed(inserts, vec![]).unwrap();
        assert!(removed.is_empty());

        // Verify all values were inserted
        assert_eq!(
            storage.get(b"key1".to_vec()).unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            storage.get(b"key2".to_vec()).unwrap(),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            storage.get(b"key3".to_vec()).unwrap(),
            Some(b"value3".to_vec())
        );
    }

    #[test]
    fn test_atomic_batch_mixed_removes_only() {
        let mut storage = MockAtomicStorage::new();

        // Insert some test data
        storage
            .insert(b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage
            .insert(b"key2".to_vec(), b"value2".to_vec())
            .unwrap();
        storage
            .insert(b"key3".to_vec(), b"value3".to_vec())
            .unwrap();

        let keys_to_remove = vec![b"key1".to_vec(), b"key2".to_vec(), b"nonexistent".to_vec()];

        // Test batch mixed with only removes
        let removed = storage.batch_mixed(vec![], keys_to_remove).unwrap();

        // Verify return values
        assert_eq!(removed[0], Some(b"value1".to_vec()));
        assert_eq!(removed[1], Some(b"value2".to_vec()));
        assert_eq!(removed[2], None); // nonexistent key

        // Verify keys were actually removed
        assert_eq!(storage.get(b"key1".to_vec()).unwrap(), None);
        assert_eq!(storage.get(b"key2".to_vec()).unwrap(), None);
        assert_eq!(
            storage.get(b"key3".to_vec()).unwrap(),
            Some(b"value3".to_vec())
        );
    }

    #[test]
    fn test_atomic_batch_mixed() {
        let mut storage = MockAtomicStorage::new();

        // Insert some initial data
        storage
            .insert(b"existing1".to_vec(), b"value1".to_vec())
            .unwrap();
        storage
            .insert(b"existing2".to_vec(), b"value2".to_vec())
            .unwrap();

        let inserts = vec![
            (b"new1".to_vec(), b"newvalue1".to_vec()),
            (b"new2".to_vec(), b"newvalue2".to_vec()),
        ];

        let removes = vec![b"existing1".to_vec(), b"nonexistent".to_vec()];

        // Test mixed operations
        let removed = storage.batch_mixed(inserts, removes).unwrap();

        // Verify removals
        assert_eq!(removed[0], Some(b"value1".to_vec()));
        assert_eq!(removed[1], None);

        // Verify inserts
        assert_eq!(
            storage.get(b"new1".to_vec()).unwrap(),
            Some(b"newvalue1".to_vec())
        );
        assert_eq!(
            storage.get(b"new2".to_vec()).unwrap(),
            Some(b"newvalue2".to_vec())
        );

        // Verify removes
        assert_eq!(storage.get(b"existing1".to_vec()).unwrap(), None);
        assert_eq!(
            storage.get(b"existing2".to_vec()).unwrap(),
            Some(b"value2".to_vec())
        );
    }

    #[test]
    fn test_atomic_failure_handling() {
        let mut storage = MockAtomicStorage::new();

        storage.set_fail_next(true);

        let inserts = vec![(b"key1".to_vec(), b"value1".to_vec())];

        // Test that failure is properly returned
        let result = storage.batch_mixed(inserts, vec![]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            MockAtomicError("Batch mixed operation failed".to_string())
        );
    }
}
