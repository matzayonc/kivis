use std::{cmp::Reverse, collections::BTreeMap, ops::Range};

use bincode::{
    config::Configuration,
    error::{DecodeError, EncodeError},
};
use kivis::{
    ApplyError, BufferOverflowError, Database, DatabaseTransaction, Record, Repository, Storage,
    manifest,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MockError {
    #[error("Serialization error")]
    Serialization(#[from] EncodeError),
    #[error("Deserialization error")]
    Deserialization(#[from] DecodeError),
    #[error("Buffer overflow error")]
    BufferOverflow(#[from] BufferOverflowError),
    #[error("Write limit reached")]
    WriteLimit,
}

#[derive(Debug, Clone, Record, PartialEq, Eq, Serialize, Deserialize)]
pub struct MockRecord(#[key] u8, char);

manifest![Manifest: MockRecord];

// Mock atomic storage implementation
#[derive(Debug)]
pub struct MockAtomicStorage {
    data: BTreeMap<Reverse<Vec<u8>>, Vec<u8>>,
    /// Fail on the (N+1)th write inside `apply`. Defaults to `usize::MAX` (never fails).
    fail_after: usize,
}

impl Default for MockAtomicStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MockAtomicStorage {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            fail_after: usize::MAX,
        }
    }
}

impl Storage for MockAtomicStorage {
    type Repo = Self;
    type Unifiers = (Configuration, Configuration);
    fn repository(&self) -> &Self::Repo {
        self
    }

    fn repository_mut(&mut self) -> &mut Self::Repo {
        self
    }
}

impl Repository for MockAtomicStorage {
    type K = Vec<u8>;
    type V = Vec<u8>;
    type Error = MockError;

    fn insert_entry(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.data.insert(Reverse(key.to_vec()), value.to_vec());
        Ok(())
    }

    fn get_entry(&self, key: &[u8]) -> Result<Option<Self::V>, Self::Error> {
        Ok(self.data.get(&Reverse(key.to_vec())).cloned())
    }

    fn remove_entry(&mut self, key: &[u8]) -> Result<Option<Self::V>, Self::Error> {
        Ok(self.data.remove(&Reverse(key.to_vec())))
    }

    fn scan_range(
        &self,
        range: Range<Self::K>,
    ) -> Result<impl Iterator<Item = Result<Self::K, Self::Error>>, Self::Error> {
        let reverse_range = Reverse(range.end)..Reverse(range.start);
        let iter = self.data.range(reverse_range);
        Ok(iter.map(|(k, _v)| Ok(k.0.clone())))
    }

    fn apply<U, E>(
        &mut self,
        operations: impl Iterator<Item = Result<kivis::BatchOp<U>, E>>,
    ) -> Result<(), ApplyError<E, Self::Error>>
    where
        U: kivis::UnifierPair,
        U::KeyUnifier: kivis::Unifier<D = Self::K>,
        U::ValueUnifier: kivis::Unifier<D = Self::V>,
    {
        let snapshot = self.data.clone();
        let mut writes = 0usize;

        let result = (|| {
            for op in operations {
                let op = op.map_err(ApplyError::Serialization)?;
                if writes >= self.fail_after {
                    return Err(ApplyError::Application(MockError::WriteLimit));
                }
                match op {
                    kivis::BatchOp::Insert { key, value } => {
                        self.data.insert(Reverse(key), value);
                    }
                    kivis::BatchOp::Delete { key } => {
                        self.data.remove(&Reverse(key));
                    }
                }
                writes += 1;
            }
            Ok(())
        })();

        if result.is_err() {
            self.data = snapshot;
        }
        result
    }
}

#[test]
fn test_transaction_new() -> anyhow::Result<()> {
    let tx = DatabaseTransaction::<Manifest, (Configuration, Configuration)>::new((
        Configuration::default(),
        Configuration::default(),
    ));
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
    let tx =
        DatabaseTransaction::<Manifest, (Configuration, Configuration)>::new(Default::default());

    // Empty transaction should succeed
    tx.commit(&mut storage)?;
    Ok(())
}

#[test]
fn test_atomicity_failure_leaves_no_partial_writes() -> anyhow::Result<()> {
    let mut db = Database::<MockAtomicStorage, Manifest>::new(MockAtomicStorage::new())?;
    db.insert(MockRecord(1, 'a'))?;
    db.insert(MockRecord(2, 'b'))?;
    db.insert(MockRecord(3, 'c'))?;

    let mut tx = db.create_transaction();
    tx.remove(&MockRecordKey(1), &MockRecord(1, 'a'))?;
    tx.insert(MockRecord(4, 'd'))?;
    tx.remove(&MockRecordKey(2), &MockRecord(2, 'b'))?;

    // Allow only 1 write before faulting, then reconstruct the DB.
    let mut storage = db.dissolve();
    storage.fail_after = 1;
    let mut db = Database::<MockAtomicStorage, Manifest>::new(storage)?;

    let result = db.commit(tx);
    assert!(result.is_err(), "commit should fail due to write limit");

    // All original records must still be present — no partial writes.
    assert_eq!(db.get(&MockRecordKey(1))?, Some(MockRecord(1, 'a')));
    assert_eq!(db.get(&MockRecordKey(2))?, Some(MockRecord(2, 'b')));
    assert_eq!(db.get(&MockRecordKey(3))?, Some(MockRecord(3, 'c')));
    assert_eq!(db.get(&MockRecordKey(4))?, None);

    Ok(())
}

#[test]
fn test_atomicity_success_applies_all_writes() -> anyhow::Result<()> {
    let mut db = Database::<MockAtomicStorage, Manifest>::new(MockAtomicStorage::new())?;
    db.insert(MockRecord(1, 'a'))?;
    db.insert(MockRecord(2, 'b'))?;
    db.insert(MockRecord(3, 'c'))?;

    let mut tx = db.create_transaction();
    tx.remove(&MockRecordKey(1), &MockRecord(1, 'a'))?;
    tx.insert(MockRecord(4, 'd'))?;
    tx.remove(&MockRecordKey(2), &MockRecord(2, 'b'))?;

    db.commit(tx)?;

    assert_eq!(db.get(&MockRecordKey(1))?, None);
    assert_eq!(db.get(&MockRecordKey(2))?, None);
    assert_eq!(db.get(&MockRecordKey(3))?, Some(MockRecord(3, 'c')));
    assert_eq!(db.get(&MockRecordKey(4))?, Some(MockRecord(4, 'd')));

    Ok(())
}

#[test]
fn test_atomicity_fails_on_first_write() -> anyhow::Result<()> {
    let mut db = Database::<MockAtomicStorage, Manifest>::new(MockAtomicStorage::new())?;
    db.insert(MockRecord(1, 'a'))?;

    // Overwrite key 1 in a transaction, but fault before any write is applied.
    let mut tx = db.create_transaction();
    tx.insert(MockRecord(1, 'z'))?;

    let mut storage = db.dissolve();
    storage.fail_after = 0;
    let mut db = Database::<MockAtomicStorage, Manifest>::new(storage)?;

    assert!(db.commit(tx).is_err());

    // The overwrite must not have taken effect.
    assert_eq!(db.get(&MockRecordKey(1))?, Some(MockRecord(1, 'a')));

    Ok(())
}

#[test]
fn test_atomicity_partial_write_rolled_back() -> anyhow::Result<()> {
    let db = Database::<MockAtomicStorage, Manifest>::new(MockAtomicStorage::new())?;

    // Two inserts in one transaction; fault after the first write.
    let mut tx = db.create_transaction();
    tx.insert(MockRecord(1, 'a'))?;
    tx.insert(MockRecord(2, 'b'))?;

    let mut storage = db.dissolve();
    storage.fail_after = 1;
    let mut db = Database::<MockAtomicStorage, Manifest>::new(storage)?;

    assert!(db.commit(tx).is_err());

    // The partial write of key 1 must be rolled back — neither record is visible.
    assert_eq!(db.get(&MockRecordKey(1))?, None);
    assert_eq!(db.get(&MockRecordKey(2))?, None);

    Ok(())
}
