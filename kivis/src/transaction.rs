use bincode::config::Configuration;

#[cfg(feature = "atomic")]
use crate::traits::AtomicStorage;
use crate::{
    wrap::{encode_value, wrap, Subtable, WrapPrelude},
    Database, DatabaseEntry, DatabaseError, DeriveKey, Incrementable, KeyBytes, Manifest,
    Manifests, RecordKey, SerializationError, Storage,
};

type Write = (Vec<u8>, Vec<u8>);

/// A database transaction that accumulates low-level byte operations (writes and deletes)
/// without immediately applying them to storage.
///
/// This struct is always available, but the `commit` method is only available when the "atomic" feature is enabled.
pub struct DatabaseTransaction<Manifest> {
    /// Pending write operations: (key, value) pairs
    pending_writes: Vec<Write>,
    /// Pending delete operations: keys to delete
    pending_deletes: Vec<Vec<u8>>,
    /// Serialization configuration
    serialization_config: Configuration,
    _marker: std::marker::PhantomData<Manifest>,
}

impl<M: Manifest> DatabaseTransaction<M> {
    /// Creates a new empty transaction. Should be used by [`Database::create_transaction`].
    pub fn new<S: Storage>(database: &Database<S, M>) -> Self {
        Self {
            pending_writes: Vec::new(),
            pending_deletes: Vec::new(),
            serialization_config: database.serialization_config(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn new_with_serialization_config(serialization_config: Configuration) -> Self {
        Self {
            pending_writes: Vec::new(),
            pending_deletes: Vec::new(),
            serialization_config,
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
        for (k, v) in writes {
            self.write(k, v);
        }
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
        // let original_key = TableHeads::last_id::<R::Key, S>(database)?
        //     .next_id()
        //     .ok_or(DatabaseError::FailedToIncrement)?;
        let last_key = database.manifest.last();
        let new_key = last_key
            .as_ref()
            .map(|k| k.next_id().unwrap())
            .unwrap_or_default();

        let writes = self.prepare_writes::<R>(&record, &new_key)?;
        for (k, v) in writes {
            self.write(k, v);
        }
        last_key.replace(new_key.clone());
        Ok(new_key)
    }

    pub fn remove<R: DatabaseEntry>(
        &mut self,
        key: &R::Key,
        record: &R,
    ) -> Result<(), SerializationError>
    where
        R::Key: RecordKey<Record = R>,
        M: Manifests<R>,
    {
        let deletes = self.prepare_deletes::<R>(record, key)?;
        for d in deletes {
            self.delete(d);
        }
        Ok(())
    }

    /// Adds a write operation to the transaction.
    ///
    /// If the same key is written multiple times, only the last value is kept.
    /// If a key is both written and deleted, the write takes precedence.
    fn write(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.pending_writes.retain(|(k, _)| k != &key);
        self.pending_deletes.retain(|k| k != &key);
        // Remove from deletes if it was there
        self.pending_writes.push((key, value));
    }

    /// Adds a delete operation to the transaction.
    ///
    /// If a key is both written and deleted, the write takes precedence
    /// (so this delete will be ignored if the key was already written).
    fn delete(&mut self, key: Vec<u8>) {
        // Only add to deletes if it's not already being written
        if !self.pending_writes.iter().any(|(k, _)| k == &key) {
            self.pending_deletes.push(key);
        }
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

    /// Returns an iterator over the pending write operations.
    pub fn pending_writes(&self) -> impl Iterator<Item = &Write> {
        self.pending_writes.iter()
    }

    /// Returns an iterator over the pending delete keys.
    pub fn pending_deletes(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.pending_deletes.iter()
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
        let inserts: Vec<Write> = self.pending_writes.into_iter().collect();
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

    pub fn consume(self) -> (impl Iterator<Item = Write>, impl Iterator<Item = Vec<u8>>) {
        (
            self.pending_writes.into_iter(),
            self.pending_deletes.into_iter(),
        )
    }

    fn prepare_writes<R: DatabaseEntry>(
        &self,
        record: &R,
        key: &R::Key,
    ) -> Result<Vec<Write>, SerializationError>
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

    fn prepare_deletes<R: DatabaseEntry>(
        &self,
        record: &R,
        key: &R::Key,
    ) -> Result<Vec<Vec<u8>>, SerializationError>
    where
        R::Key: RecordKey<Record = R>,
    {
        let index_keys = record.index_keys();
        let mut deletes = Vec::with_capacity(1 + index_keys.len());

        for (discriminator, index_key) in record.index_keys() {
            let mut entry = WrapPrelude::new::<R>(Subtable::Index(discriminator))
                .to_bytes(self.serialization_config());
            entry.extend_from_slice(&index_key.to_bytes(self.serialization_config()));
            let key_bytes = key.to_bytes(self.serialization_config());
            entry.extend_from_slice(&key_bytes);

            deletes.push(entry.clone());
        }

        let key = wrap::<R>(key, self.serialization_config())?;
        deletes.push(key);

        Ok(deletes)
    }
}
