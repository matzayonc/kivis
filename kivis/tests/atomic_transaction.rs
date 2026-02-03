// Note: test functions return Result and avoid using `unwrap()`
#[cfg(test)]
mod tests {
    use std::{cmp::Reverse, collections::BTreeMap, fmt::Display, ops::Range};

    use bincode::{
        config::Configuration,
        error::{DecodeError, EncodeError},
    };
    use serde::{Deserialize, Serialize};

    use kivis::{
        BatchOp, BufferOverflowError, Database, DatabaseTransaction, OpsIter, Record, Storage,
        manifest,
    };

    #[derive(Debug, PartialEq, Eq)]
    pub enum MockError {
        Serialization,
        Deserialization,
        BufferOverflow,
    }

    impl Display for MockError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Serialization => write!(f, "Serialization error"),
                Self::Deserialization => write!(f, "Deserialization error"),
                Self::BufferOverflow => write!(f, "Buffer overflow error"),
            }
        }
    }

    impl From<EncodeError> for MockError {
        fn from(_: EncodeError) -> Self {
            Self::Serialization
        }
    }

    impl From<DecodeError> for MockError {
        fn from(_: DecodeError) -> Self {
            Self::Deserialization
        }
    }

    impl From<BufferOverflowError> for MockError {
        fn from(_: BufferOverflowError) -> Self {
            Self::BufferOverflow
        }
    }

    #[derive(Debug, Record, PartialEq, Eq, Serialize, Deserialize)]
    pub struct MockRecord(#[key] u8, char);

    manifest![Manifest: MockRecord];

    // Mock atomic storage implementation
    #[derive(Debug)]
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
        type Serializer = Configuration;
        type StoreError = MockError;

        fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::StoreError> {
            self.data.insert(Reverse(key.to_vec()), value.to_vec());
            Ok(())
        }

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::StoreError> {
            Ok(self.data.get(&Reverse(key.to_vec())).cloned())
        }

        fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::StoreError> {
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

        // Override the default batch_mixed implementation for better performance
        fn batch_mixed<'a>(
            &mut self,
            operations: OpsIter<'a, Self::Serializer>,
        ) -> Result<Vec<Option<Vec<u8>>>, Self::StoreError> {
            // In a real implementation, this could be atomic
            let mut deleted = Vec::new();
            for op in operations {
                match op {
                    BatchOp::Insert { key, value } => {
                        self.data.insert(Reverse(key.to_vec()), value.to_vec());
                    }
                    BatchOp::Delete { key } => {
                        deleted.push(self.data.remove(&Reverse(key.to_vec())));
                    }
                }
            }

            Ok(deleted)
        }
    }

    #[test]
    fn test_transaction_new() -> anyhow::Result<()> {
        let tx = DatabaseTransaction::<Manifest, _>::new_with_serializer(Configuration::default());
        assert!(tx.is_empty());
        Ok(())
    }

    #[test]
    fn test_transaction_write() -> anyhow::Result<()> {
        let mut db = Database::<MockAtomicStorage, Manifest>::new(MockAtomicStorage::new())?;
        let mut tx = db.create_transaction();

        tx.insert(MockRecord(1, 'a'))?;
        tx.insert(MockRecord(2, 'b'))?;

        assert!(!tx.is_empty());

        // Verify by committing and checking the database
        db.commit(tx)?;
        assert_eq!(db.get(&MockRecordKey(1))?, Some(MockRecord(1, 'a')));
        assert_eq!(db.get(&MockRecordKey(2))?, Some(MockRecord(2, 'b')));

        Ok(())
    }

    #[test]
    fn test_transaction_delete() -> anyhow::Result<()> {
        let mut db = Database::<MockAtomicStorage, Manifest>::new(MockAtomicStorage::new())?;

        // First insert some records
        db.insert(MockRecord(1, 'a'))?;
        db.insert(MockRecord(2, 'b'))?;

        // Now delete them in a transaction
        let mut tx = db.create_transaction();
        tx.remove(&MockRecordKey(1), &MockRecord(1, 'a'))?;
        tx.remove(&MockRecordKey(2), &MockRecord(2, 'b'))?;

        assert!(!tx.is_empty());

        // Verify by committing and checking the database
        db.commit(tx)?;
        assert_eq!(db.get(&MockRecordKey(1))?, None);
        assert_eq!(db.get(&MockRecordKey(2))?, None);

        Ok(())
    }

    #[test]
    fn test_transaction_commit() -> anyhow::Result<()> {
        let mut db = Database::<MockAtomicStorage, Manifest>::new(MockAtomicStorage::new())?;
        let mut tx = db.create_transaction();

        // Add some operations
        tx.insert(MockRecord(1, 'a'))?;
        tx.insert(MockRecord(2, 'b'))?;

        // Commit the transaction
        let result = db.commit(tx);
        assert!(result.is_ok());

        // Verify the writes were applied
        assert_eq!(db.get(&MockRecordKey(1))?, Some(MockRecord(1, 'a')));
        assert_eq!(db.get(&MockRecordKey(2))?, Some(MockRecord(2, 'b')));
        Ok(())
    }

    #[test]
    fn test_transaction_rollback() -> anyhow::Result<()> {
        let db = Database::<MockAtomicStorage, Manifest>::new(MockAtomicStorage::new())?;
        let mut tx = db.create_transaction();

        tx.insert(MockRecord(1, 'a'))?;
        tx.remove(&MockRecordKey(2), &MockRecord(2, 'b'))?;

        assert!(!tx.is_empty());

        // Rollback should consume the transaction
        tx.rollback();

        // No way to verify rollback directly since transaction is consumed,
        // but this tests that rollback doesn't panic
        Ok(())
    }

    #[test]
    fn test_empty_transaction_commit() -> anyhow::Result<()> {
        let mut storage = MockAtomicStorage::new();
        let tx = DatabaseTransaction::<Manifest, _>::new_with_serializer(Default::default());

        // Empty transaction should succeed and return empty vector
        let result = tx.commit(&mut storage)?;
        assert!(result.is_empty());
        Ok(())
    }
}
