#[cfg(feature = "atomic")]
#[cfg(test)]
mod tests {
    use std::{cmp::Reverse, collections::BTreeMap, ops::Range};

    use serde::{Deserialize, Serialize};

    use kivis::{manifest, AtomicStorage, DatabaseTransaction, Record, Storage};

    #[derive(Debug, Record, Serialize, Deserialize)]
    pub struct MockRecord(u64);

    manifest![Manifest: MockRecord];

    // Mock atomic storage implementation
    pub struct MockAtomicStorage {
        data: BTreeMap<Reverse<Vec<u8>>, Vec<u8>>,
    }

    impl MockAtomicStorage {
        pub fn new() -> Self {
            Self {
                data: BTreeMap::new(),
            }
        }
    }

    impl Storage for MockAtomicStorage {
        type StoreError = String;

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

    impl AtomicStorage for MockAtomicStorage {
        fn batch_mixed(
            &mut self,
            inserts: Vec<(Vec<u8>, Vec<u8>)>,
            removes: Vec<Vec<u8>>,
        ) -> Result<Vec<Option<Vec<u8>>>, Self::StoreError> {
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
    fn test_transaction_new() {
        let tx = DatabaseTransaction::<Manifest>::new_with_serialization_config(Default::default());
        assert!(tx.is_empty());
        assert_eq!(tx.write_count(), 0);
        assert_eq!(tx.delete_count(), 0);
    }

    #[test]
    fn test_transaction_write() {
        let mut tx =
            DatabaseTransaction::<Manifest>::new_with_serialization_config(Default::default());

        tx.write(b"key1".to_vec(), b"value1".to_vec());
        tx.write(b"key2".to_vec(), b"value2".to_vec());

        assert!(!tx.is_empty());
        assert_eq!(tx.write_count(), 2);
        assert_eq!(tx.delete_count(), 0);
    }

    #[test]
    fn test_transaction_delete() {
        let mut tx =
            DatabaseTransaction::<Manifest>::new_with_serialization_config(Default::default());

        tx.delete(b"key1".to_vec());
        tx.delete(b"key2".to_vec());

        assert!(!tx.is_empty());
        assert_eq!(tx.write_count(), 0);
        assert_eq!(tx.delete_count(), 2);
    }

    #[test]
    fn test_transaction_write_overrides_delete() {
        let mut tx =
            DatabaseTransaction::<Manifest>::new_with_serialization_config(Default::default());

        // Delete first, then write
        tx.delete(b"key1".to_vec());
        tx.write(b"key1".to_vec(), b"value1".to_vec());

        // Write should override delete
        assert_eq!(tx.write_count(), 1);
        assert_eq!(tx.delete_count(), 0);
    }

    #[test]
    fn test_transaction_delete_ignored_after_write() {
        let mut tx =
            DatabaseTransaction::<Manifest>::new_with_serialization_config(Default::default());

        // Write first, then delete
        tx.write(b"key1".to_vec(), b"value1".to_vec());
        tx.delete(b"key1".to_vec());

        // Delete should be ignored since key is being written
        assert_eq!(tx.write_count(), 1);
        assert_eq!(tx.delete_count(), 0);
    }

    #[test]
    fn test_transaction_multiple_writes_same_key() {
        let mut tx =
            DatabaseTransaction::<Manifest>::new_with_serialization_config(Default::default());

        tx.write(b"key1".to_vec(), b"value1".to_vec());
        tx.write(b"key1".to_vec(), b"value2".to_vec());
        tx.write(b"key1".to_vec(), b"value3".to_vec());

        // Should only have one write (last value)
        assert_eq!(tx.write_count(), 1);

        let writes: Vec<_> = tx.pending_writes().collect();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].1, b"value3".to_vec());
    }

    #[test]
    fn test_transaction_commit() {
        let mut storage = MockAtomicStorage::new();
        let mut tx =
            DatabaseTransaction::<Manifest>::new_with_serialization_config(Default::default());

        // Add some operations
        tx.write(b"key1".to_vec(), b"value1".to_vec());
        tx.write(b"key2".to_vec(), b"value2".to_vec());
        tx.delete(b"key3".to_vec());

        // Commit the transaction
        let result = tx.commit(&mut storage);
        assert!(result.is_ok());

        // Verify the writes were applied
        assert_eq!(
            storage.get(b"key1".to_vec()).unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            storage.get(b"key2".to_vec()).unwrap(),
            Some(b"value2".to_vec())
        );
    }

    #[test]
    fn test_transaction_rollback() {
        let mut tx =
            DatabaseTransaction::<Manifest>::new_with_serialization_config(Default::default());

        tx.write(b"key1".to_vec(), b"value1".to_vec());
        tx.delete(b"key2".to_vec());

        assert!(!tx.is_empty());

        // Rollback should consume the transaction
        tx.rollback();

        // No way to verify rollback directly since transaction is consumed,
        // but this tests that rollback doesn't panic
    }

    #[test]
    fn test_empty_transaction_commit() {
        let mut storage = MockAtomicStorage::new();
        let tx = DatabaseTransaction::<Manifest>::new_with_serialization_config(Default::default());

        // Empty transaction should succeed and return empty vector
        let result = tx.commit(&mut storage).unwrap();
        assert!(result.is_empty());
    }
}
