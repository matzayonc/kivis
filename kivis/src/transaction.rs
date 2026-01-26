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
        self.prepare_writes::<R>(record, &original_key)?;
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
            R::Key::default()
        };

        self.prepare_writes::<R>(record, &new_key)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
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
        self.prepare_deletes::<R>(record, key)
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
        &mut self,
        record: R,
        key: &R::Key,
    ) -> Result<(), U::SerError>
    where
        R::Key: RecordKey<Record = R>,
        IndexBuilder<U>: Indexer<Error = U::SerError>,
    {
        let mut indexer = IndexBuilder::new(self.serializer());
        record.index_keys(&mut indexer)?;

        // Track serialized key hash and value positions, lazily initialized on first iteration
        let mut key_hash_range: Option<(usize, usize)> = None;
        let mut key_value_range: Option<(usize, usize)> = None;

        for (discriminator, index_key) in indexer.into_index_keys() {
            // Write index entry directly to buffers
            let key_start = U::K::len(&self.key_data);

            let mut prelude_buffer = <U::K as UnifierData>::Owned::default();
            self.serializer().serialize_key(
                &mut prelude_buffer,
                WrapPrelude::new::<R>(Subtable::Index(discriminator)),
            )?;

            U::K::extend(&mut self.key_data, prelude_buffer.as_ref());
            U::K::extend(&mut self.key_data, index_key.as_ref());

            // Serialize key hash on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_hash_range {
                // Reuse previously serialized key hash
                let key_hash = U::K::extract_range(&self.key_data, start, end);
                let key_hash_owned = U::K::to_owned(key_hash);
                U::K::extend(&mut self.key_data, key_hash_owned.as_ref());
            } else {
                // First iteration: serialize key hash and save indices
                let start = U::K::len(&self.key_data);
                self.serializer()
                    .serialize_key_ref(&mut self.key_data, key)?;
                let end = U::K::len(&self.key_data);
                key_hash_range = Some((start, end));
            }

            let key_end = U::K::len(&self.key_data);

            let value_start = U::V::len(&self.value_data);

            // Serialize key value on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_value_range {
                // Reuse previously serialized key value
                let key_value = U::V::extract_range(&self.value_data, start, end);
                let key_value_owned = U::V::to_owned(key_value);
                U::V::extend(&mut self.value_data, key_value_owned.as_ref());
            } else {
                // First iteration: serialize key value and save indices
                let start = U::V::len(&self.value_data);
                self.serializer()
                    .serialize_value_ref(&mut self.value_data, key)?;
                let end = U::V::len(&self.value_data);
                key_value_range = Some((start, end));
            }

            let value_end = U::V::len(&self.value_data);

            self.pending_ops.push(Op::Write {
                key_start,
                key_end,
                value_start,
                value_end,
            });
        }

        // Write main record directly to buffers
        let key_start = U::K::len(&self.key_data);
        wrap::<R, U>(key, &self.serializer(), &mut self.key_data)?;
        let key_end = U::K::len(&self.key_data);

        let value_start = U::V::len(&self.value_data);
        self.serializer()
            .serialize_value(&mut self.value_data, record)?;
        let value_end = U::V::len(&self.value_data);

        self.pending_ops.push(Op::Write {
            key_start,
            key_end,
            value_start,
            value_end,
        });

        Ok(())
    }

    fn prepare_deletes<R: DatabaseEntry>(
        &mut self,
        record: &R,
        key: &R::Key,
    ) -> Result<(), U::SerError>
    where
        R::Key: RecordKey<Record = R>,
        IndexBuilder<U>: Indexer<Error = U::SerError>,
    {
        let mut indexer = IndexBuilder::new(self.serializer());
        record.index_keys(&mut indexer)?;

        let index_keys = indexer.into_index_keys();

        // Track serialized key position, lazily initialized on first iteration
        let mut key_bytes_range: Option<(usize, usize)> = None;

        for (discriminator, index_key) in index_keys {
            // Write index delete key directly to buffer
            let key_start = U::K::len(&self.key_data);

            let mut prelude_buffer = <U::K as UnifierData>::Owned::default();
            self.serializer().serialize_key(
                &mut prelude_buffer,
                WrapPrelude::new::<R>(Subtable::Index(discriminator)),
            )?;

            U::K::extend(&mut self.key_data, prelude_buffer.as_ref());
            U::K::extend(&mut self.key_data, index_key.as_ref());

            // Serialize key on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_bytes_range {
                // Reuse previously serialized key
                let key_bytes = U::K::extract_range(&self.key_data, start, end);
                let key_bytes_owned = U::K::to_owned(key_bytes);
                U::K::extend(&mut self.key_data, key_bytes_owned.as_ref());
            } else {
                // First iteration: serialize key and save indices
                let start = U::K::len(&self.key_data);
                self.serializer()
                    .serialize_key_ref(&mut self.key_data, key)?;
                let end = U::K::len(&self.key_data);
                key_bytes_range = Some((start, end));
            }

            let key_end = U::K::len(&self.key_data);
            self.pending_ops.push(Op::Delete { key_start, key_end });
        }

        // Delete main record - write directly to buffer
        let key_start = U::K::len(&self.key_data);
        // TODO: Use directly
        wrap::<R, _>(key, &self.serializer(), &mut self.key_data)?;
        let key_end = U::K::len(&self.key_data);
        self.pending_ops.push(Op::Delete { key_start, key_end });

        Ok(())
    }
}
