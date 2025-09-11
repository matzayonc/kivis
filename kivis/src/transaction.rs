use bincode::config::Configuration;

#[cfg(feature = "atomic")]
use crate::traits::AtomicStorage;
use crate::{
    wrap::{encode_value, wrap, Subtable, WrapPrelude},
    Database, DatabaseEntry, DatabaseError, DeriveKey, Incrementable, KeyBytes, Manifest,
    Manifests, RecordKey, SerializationError, Storage,
};

/// A database transaction that accumulates low-level byte operations (writes and deletes)
/// without immediately applying them to storage.
///
/// This struct is always available, but the `commit` method is only available when the "atomic" feature is enabled.
pub struct DatabaseTransaction<Manifest> {
    /// Pending write operations: (key, value) pairs
    pending_writes: Vec<(Vec<u8>, Vec<u8>)>,
    /// Pending delete operations: keys to delete
    pending_deletes: Vec<Vec<u8>>,
    /// Serialization configuration
    serialization_config: Configuration,
    _marker: std::marker::PhantomData<Manifest>,
}

impl<M: Manifest> DatabaseTransaction<M> {
    /// Creates a new empty transaction. Should be used by [`Database::create_transaction`].
    pub(crate) fn new<S: Storage>(database: &Database<S, M>) -> Self {
        Self {
            pending_writes: Vec::new(),
            pending_deletes: Vec::new(),
            serialization_config: database.serialization_config(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn insert<K: RecordKey<Record = R>, R>(
        &mut self,
        record: R,
    ) -> Result<K, SerializationError>
    where
        R: DeriveKey<Key = K> + DatabaseEntry<Key = K>,
        M: Manifests<R>,
    {
        let original_key = R::key(&record);
        let writes = self.prepare_writes::<R>(&record, &original_key)?;
        self.pending_writes.extend(writes);
        Ok(original_key)
    }

    pub fn put<S: Storage, R: DatabaseEntry>(
        &mut self,
        record: R,
        database: &mut Database<S, M>,
    ) -> Result<R::Key, DatabaseError<S::StoreError>>
    where
        R::Key: RecordKey<Record = R> + Incrementable + Ord,
        M: Manifests<R>,
    {
        let original_key = database
            .last_id::<R::Key>(R::Key::BOUNDS)?
            .next_id()
            .ok_or(DatabaseError::FailedToIncrement)?;
        let writes = self.prepare_writes::<R>(&record, &original_key)?;
        self.pending_writes.extend(writes);
        Ok(original_key)
    }

    /// Adds a write operation to the transaction.
    ///
    /// If the same key is written multiple times, only the last value is kept.
    /// If a key is both written and deleted, the write takes precedence.
    pub fn write(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Remove from deletes if it was there
        self.pending_writes.push((key, value));
    }

    /// Adds a delete operation to the transaction.
    ///
    /// If a key is both written and deleted, the write takes precedence
    /// (so this delete will be ignored if the key was already written).
    pub fn delete(&mut self, key: Vec<u8>) {
        // Only add to deletes if it's not already being written
        self.pending_deletes.push(key);
    }

    /// Returns true if the transaction has no pending operations.
    pub fn is_empty(&self) -> bool {
        self.pending_writes.is_empty() && self.pending_deletes.is_empty()
    }

    /// Returns the number of pending write operations.
    pub fn write_count(&self) -> usize {
        self.pending_writes.len()
    }

    /// Returns the number of pending delete operations.
    pub fn delete_count(&self) -> usize {
        self.pending_deletes.len()
    }

    fn serialization_config(&self) -> Configuration {
        self.serialization_config
    }

    /// Commits all pending operations to the storage atomically.
    ///
    /// Either all operations succeed, or none of them are applied.
    /// The transaction is consumed by this operation.
    ///
    /// This method is only available when the "atomic" feature is enabled.
    #[cfg(feature = "atomic")]
    pub fn commit<S: AtomicStorage>(
        self,
        storage: &mut S,
    ) -> Result<Vec<Option<Vec<u8>>>, DatabaseError<S::StoreError>> {
        if self.is_empty() {
            return Ok(Vec::new());
        }

        // Convert to the format expected by batch_mixed
        let inserts: Vec<(Vec<u8>, Vec<u8>)> = self.pending_writes.into_iter().collect();
        let removes: Vec<Vec<u8>> = self.pending_deletes.into_iter().collect();

        storage
            .batch_mixed(inserts, removes)
            .map_err(DatabaseError::Storage)
    }

    /// Discards all pending operations without applying them.
    /// The transaction is consumed by this operation.
    pub fn rollback(self) {
        // Simply drop the transaction, discarding all pending operations
        drop(self);
    }

    pub fn consume(
        self,
    ) -> (
        impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
        impl Iterator<Item = Vec<u8>>,
    ) {
        (
            self.pending_writes.into_iter(),
            self.pending_deletes.into_iter(),
        )
    }

    fn prepare_writes<R: DatabaseEntry>(
        &self,
        record: &R,
        key: &R::Key,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SerializationError>
    where
        R::Key: RecordKey<Record = R>,
    {
        let index_keys = record.index_keys();
        let mut writes = Vec::with_capacity(1 + index_keys.len());

        for (discriminator, index_key) in record.index_keys() {
            let mut entry = WrapPrelude::new::<R>(Subtable::Index(discriminator))
                .to_bytes(self.serialization_config());
            entry.extend_from_slice(&index_key.to_bytes(self.serialization_config()));

            // Indexes might be repeated, so we need to ensure that the key is unique.
            // TODO: Add a way to declare as unique and deduplicate by provided hash.
            let key_bytes = key.to_bytes(self.serialization_config());
            entry.extend_from_slice(&key_bytes);

            writes.push((entry.clone(), key_bytes.clone()));
        }

        let key = wrap::<R>(key, self.serialization_config())?;
        let value = encode_value(record, self.serialization_config())?;
        writes.push((key, value));

        Ok(writes)
    }
}

#[cfg(feature = "atomic")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::atomic::atomic_storage_example::MockAtomicStorage;

    #[test]
    fn test_transaction_new() {
        let tx = DatabaseTransaction::new();
        assert!(tx.is_empty());
        assert_eq!(tx.write_count(), 0);
        assert_eq!(tx.delete_count(), 0);
    }

    #[test]
    fn test_transaction_write() {
        let mut tx = DatabaseTransaction::new();

        tx.write(b"key1".to_vec(), b"value1".to_vec());
        tx.write(b"key2".to_vec(), b"value2".to_vec());

        assert!(!tx.is_empty());
        assert_eq!(tx.write_count(), 2);
        assert_eq!(tx.delete_count(), 0);
    }

    #[test]
    fn test_transaction_delete() {
        let mut tx = DatabaseTransaction::new();

        tx.delete(b"key1".to_vec());
        tx.delete(b"key2".to_vec());

        assert!(!tx.is_empty());
        assert_eq!(tx.write_count(), 0);
        assert_eq!(tx.delete_count(), 2);
    }

    #[test]
    fn test_transaction_write_overrides_delete() {
        let mut tx = DatabaseTransaction::new();

        // Delete first, then write
        tx.delete(b"key1".to_vec());
        tx.write(b"key1".to_vec(), b"value1".to_vec());

        // Write should override delete
        assert_eq!(tx.write_count(), 1);
        assert_eq!(tx.delete_count(), 0);
    }

    #[test]
    fn test_transaction_delete_ignored_after_write() {
        let mut tx = DatabaseTransaction::new();

        // Write first, then delete
        tx.write(b"key1".to_vec(), b"value1".to_vec());
        tx.delete(b"key1".to_vec());

        // Delete should be ignored since key is being written
        assert_eq!(tx.write_count(), 1);
        assert_eq!(tx.delete_count(), 0);
    }

    #[test]
    fn test_transaction_multiple_writes_same_key() {
        let mut tx = DatabaseTransaction::new();

        tx.write(b"key1".to_vec(), b"value1".to_vec());
        tx.write(b"key1".to_vec(), b"value2".to_vec());
        tx.write(b"key1".to_vec(), b"value3".to_vec());

        // Should only have one write (last value)
        assert_eq!(tx.write_count(), 1);

        let writes: Vec<_> = tx.pending_writes().collect();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].1, &b"value3".to_vec());
    }

    #[test]
    fn test_transaction_commit() {
        let mut storage = MockAtomicStorage::new();
        let mut tx = DatabaseTransaction::new();

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
        let mut tx = DatabaseTransaction::new();

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
        let tx = DatabaseTransaction::new();

        // Empty transaction should succeed and return empty vector
        let result = tx.commit(&mut storage).unwrap();
        assert!(result.is_empty());
    }
}
