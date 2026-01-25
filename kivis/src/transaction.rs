use crate::{
    Database, DatabaseEntry, DatabaseError, DeriveKey, Incrementable, IndexBuilder, Indexer,
    Manifest, Manifests, RecordKey, Storage, Unifier, UnifierData,
    wrap::{Subtable, WrapPrelude, wrap},
};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::marker::PhantomData;

pub enum Op {
    Write {
        key_start: usize,
        key_end: usize,
        value_start: usize,
        value_end: usize,
    },
    Delete {
        key_start: usize,
        key_end: usize,
    },
}

type PreparedWrites<U> = Vec<(
    <<U as Unifier>::K as UnifierData>::Owned,
    <<U as Unifier>::V as UnifierData>::Owned,
)>;

/// A database transaction that accumulates low-level byte operations (writes and deletes)
/// without immediately applying them to storage.
///
/// This struct is always available, but the `commit` method is only available when the "atomic" feature is enabled.
pub struct DatabaseTransaction<Manifest, U: Unifier> {
    /// Pending operations: writes and deletes
    pending_ops: Vec<Op>,
    /// Key data buffer
    key_data: <U::K as UnifierData>::Owned,
    /// Value data buffer
    value_data: <U::V as UnifierData>::Owned,
    /// Serialization configuration
    serializer: U,
    _marker: PhantomData<Manifest>,
}

impl<M: Manifest, U: Unifier + Copy> DatabaseTransaction<M, U> {
    /// Creates a new empty transaction. Should be used by [`Database::create_transaction`].
    pub fn new<S: Storage<Serializer = U>>(database: &Database<S, M>) -> Self {
        Self {
            pending_ops: Vec::new(),
            key_data: <U::K as UnifierData>::Owned::default(),
            value_data: <U::V as UnifierData>::Owned::default(),
            serializer: database.serializer,
            _marker: PhantomData,
        }
    }

    /// Creates a new empty transaction with the specified serialization configuration.
    #[must_use]
    pub fn new_with_serializer(serializer: U) -> Self {
        Self {
            pending_ops: Vec::new(),
            key_data: <U::K as UnifierData>::Owned::default(),
            value_data: <U::V as UnifierData>::Owned::default(),
            serializer,
            _marker: PhantomData,
        }
    }

    /// # Errors
    ///
    /// Returns a [`U::SerError`] if serializing keys or values fails while preparing the writes.
    pub fn insert<K: RecordKey<Record = R>, R>(&mut self, record: R) -> Result<K, U::SerError>
    where
        R: DeriveKey<Key = K> + DatabaseEntry<Key = K>,
        M: Manifests<R>,
        IndexBuilder<U>: Indexer<Error = U::SerError>,
    {
        let original_key = R::key(&record);
        let writes = self.prepare_writes::<R>(record, &original_key)?;
        for (k, v) in writes {
            self.write(k, v);
        }
        Ok(original_key)
    }

    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if writing to the underlying storage fails.
    ///
    pub fn put<S: Storage<Serializer = U>, R: DatabaseEntry>(
        &mut self,
        record: R,
        database: &mut Database<S, M>,
    ) -> Result<R::Key, DatabaseError<S>>
    where
        R::Key: RecordKey<Record = R> + Incrementable + Ord,
        M: Manifests<R>,
        IndexBuilder<U>: Indexer<Error = U::SerError>,
    {
        let last_key = database.manifest.last();
        let new_key = if let Some(k) = last_key {
            k.next_id().ok_or(DatabaseError::FailedToIncrement)?
        } else {
            Default::default()
        };

        let writes = self
            .prepare_writes::<R>(record, &new_key)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        for (k, v) in writes {
            self.write(k, v);
        }
        last_key.replace(new_key.clone());
        Ok(new_key)
    }

    /// # Errors
    ///
    /// Returns a [`U::SerError`] if serializing keys to delete fails.
    pub fn remove<R: DatabaseEntry>(&mut self, key: &R::Key, record: &R) -> Result<(), U::SerError>
    where
        R::Key: RecordKey<Record = R>,
        M: Manifests<R>,
        IndexBuilder<U>: Indexer<Error = U::SerError>,
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
    fn write(&mut self, key: <U::K as UnifierData>::Owned, value: <U::V as UnifierData>::Owned) {
        let (key_start, key_end) = U::K::buffer(&mut self.key_data, key);
        let (value_start, value_end) = U::V::buffer(&mut self.value_data, value);

        // Add the new write operation
        self.pending_ops.push(Op::Write {
            key_start,
            key_end,
            value_start,
            value_end,
        });
    }

    /// Adds a delete operation to the transaction.
    ///
    /// If a key is both written and deleted, the write takes precedence
    /// (so this delete will be ignored if the key was already written).
    fn delete(&mut self, key: <U::K as UnifierData>::Owned) {
        let (key_start, key_end) = U::K::buffer(&mut self.key_data, key);

        self.pending_ops.push(Op::Delete { key_start, key_end });
    }

    /// Returns true if the transaction has no pending operations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending_ops.is_empty()
    }

    fn serializer(&self) -> U {
        self.serializer
    }

    /// Commits all pending operations to the storage.
    ///
    /// Either all operations succeed, or none of them are applied.
    /// The transaction is consumed by this operation.
    ///
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if any storage operation fails.
    pub fn commit<S>(self, storage: &mut S) -> Result<crate::Deleted<S>, DatabaseError<S>>
    where
        S: Storage<Serializer = U>,
    {
        if self.is_empty() {
            return Ok(Vec::with_capacity(0));
        }

        // Convert to the format expected by batch_mixed
        let mut inserts = Vec::new();
        let mut removes = Vec::new();

        for op in self.pending_ops {
            match op {
                Op::Write {
                    key_start,
                    key_end,
                    value_start,
                    value_end,
                } => {
                    // Extract key and value from buffers using extract_range
                    let key = U::K::extract_range(&self.key_data, key_start, key_end);
                    let value = U::V::extract_range(&self.value_data, value_start, value_end);
                    inserts.push((key, value));
                }
                Op::Delete { key_start, key_end } => {
                    // Extract key from buffer using extract_range
                    let key = U::K::extract_range(&self.key_data, key_start, key_end);
                    removes.push(key);
                }
            }
        }

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

    /// Consumes the transaction and returns the raw operation data.
    ///
    /// Returns the operations list, key buffer, and value buffer.
    /// This allows custom processing of the transaction data without assuming specific types.
    pub fn consume(
        self,
    ) -> (
        Vec<Op>,
        <U::K as UnifierData>::Owned,
        <U::V as UnifierData>::Owned,
    ) {
        (self.pending_ops, self.key_data, self.value_data)
    }

    fn prepare_writes<R: DatabaseEntry>(
        &self,
        record: R,
        key: &R::Key,
    ) -> Result<PreparedWrites<U>, U::SerError>
    where
        R::Key: RecordKey<Record = R>,
        IndexBuilder<U>: Indexer<Error = U::SerError>,
    {
        let mut writes = Vec::with_capacity(R::INDEX_COUNT_HINT + 1);

        let mut indexer = IndexBuilder::new(self.serializer());
        record.index_keys(&mut indexer)?;

        let key_hash = self.serializer().serialize_key_ref(key)?;
        let key_value = self.serializer().serialize_value_ref(key)?;

        for (discriminator, index_key) in indexer.into_index_keys() {
            let mut entry = self
                .serializer()
                .serialize_key(WrapPrelude::new::<R>(Subtable::Index(discriminator)))?;
            U::K::combine(&mut entry, index_key);

            // Indexes might be repeated, so we need to ensure that the key is unique.
            // TODO: Add a way to declare as unique and deduplicate by provided hash.
            U::K::combine(&mut entry, U::K::duplicate(&key_hash));

            // Index entries store the primary key as the value (serialized as a value type)
            writes.push((entry, U::V::duplicate(&key_value)));
        }

        let key = wrap::<R, U>(key, &self.serializer())?;
        let value = self.serializer().serialize_value(record)?;
        writes.push((key, value));

        Ok(writes)
    }

    fn prepare_deletes<R: DatabaseEntry>(
        &self,
        record: &R,
        key: &R::Key,
    ) -> Result<Vec<<U::K as UnifierData>::Owned>, U::SerError>
    where
        R::Key: RecordKey<Record = R>,
        IndexBuilder<U>: Indexer<Error = U::SerError>,
    {
        let mut deletes = Vec::with_capacity(R::INDEX_COUNT_HINT + 1);

        let mut indexer = IndexBuilder::new(self.serializer());
        record.index_keys(&mut indexer)?;

        for (discriminator, index_key) in indexer.into_index_keys() {
            let mut entry = self
                .serializer()
                .serialize_key(WrapPrelude::new::<R>(Subtable::Index(discriminator)))?;
            U::K::combine(&mut entry, index_key);
            let key_bytes = self.serializer().serialize_key_ref(key)?;
            U::K::combine(&mut entry, key_bytes);

            deletes.push(entry);
        }

        let key = wrap::<R, _>(key, &self.serializer())?;
        deletes.push(key);

        Ok(deletes)
    }
}
