use crate::{
    Database, DatabaseEntry, DatabaseError, DeriveKey, Incrementable, Manifest, Manifests,
    RecordKey, Storage, Unifier, transaction::buffer::DatabaseTransactionBuffer,
    transaction::errors::TransactionError,
};

use core::marker::PhantomData;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// A database transaction that accumulates low-level byte operations (writes and deletes)
/// without immediately applying them to storage.
///
/// This struct is always available, but the `commit` method is only available when the "atomic" feature is enabled.
pub struct DatabaseTransaction<Manifest, KU: Unifier, VU: Unifier> {
    buffer: DatabaseTransactionBuffer<KU, VU>,
    _marker: PhantomData<Manifest>,
}

impl<M: Manifest, KU: Unifier + Copy, VU: Unifier + Copy> DatabaseTransaction<M, KU, VU> {
    /// Creates a new empty transaction. Should be used by [`Database::create_transaction`].
    pub fn new<S>(database: &Database<S, M>) -> Self
    where
        S: Storage<KeyUnifier = KU, ValueUnifier = VU>,
    {
        Self {
            buffer: DatabaseTransactionBuffer::new(
                database.key_serializer,
                database.value_serializer,
            ),
            _marker: PhantomData,
        }
    }

    /// Creates a new empty transaction with the specified serialization configuration.
    #[must_use]
    pub fn new_with_serializers(key_serializer: KU, value_serializer: VU) -> Self {
        Self {
            buffer: DatabaseTransactionBuffer::new(key_serializer, value_serializer),
            _marker: PhantomData,
        }
    }

    /// # Errors
    ///
    /// Returns a [`TransactionError`] if serializing keys or values fails while preparing the writes.
    pub fn insert<K: RecordKey<Record = R>, R>(
        &mut self,
        record: R,
    ) -> Result<K, TransactionError<KU::SerError, VU::SerError>>
    where
        R: DeriveKey<Key = K> + DatabaseEntry<Key = K>,
        M: Manifests<R>,
    {
        let original_key = R::key(&record);
        self.buffer.prepare_writes::<R>(record, &original_key)?;
        Ok(original_key)
    }

    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if writing to the underlying storage fails.
    ///
    pub fn put<S, R: DatabaseEntry>(
        &mut self,
        record: R,
        database: &mut Database<S, M>,
    ) -> Result<R::Key, DatabaseError<S>>
    where
        S: Storage<KeyUnifier = KU, ValueUnifier = VU>,
        R::Key: RecordKey<Record = R> + Incrementable + Ord,
        M: Manifests<R>,
    {
        let last_key = database.manifest.last();
        let new_key = if let Some(k) = last_key {
            k.next_id().ok_or(DatabaseError::FailedToIncrement)?
        } else {
            R::Key::default()
        };

        self.buffer
            .prepare_writes::<R>(record, &new_key)
            .map_err(DatabaseError::from_transaction_error)?;
        last_key.replace(new_key.clone());
        Ok(new_key)
    }

    /// # Errors
    ///
    /// Returns a [`TransactionError`] if serializing keys to delete fails.
    pub fn remove<R: DatabaseEntry>(
        &mut self,
        key: &R::Key,
        record: &R,
    ) -> Result<(), TransactionError<KU::SerError, VU::SerError>>
    where
        R::Key: RecordKey<Record = R>,
        M: Manifests<R>,
    {
        self.buffer.prepare_deletes::<R>(record, key)
    }

    /// Returns true if the transaction has no pending operations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
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
        S: Storage<KeyUnifier = KU, ValueUnifier = VU>,
    {
        if self.is_empty() {
            return Ok(Vec::with_capacity(0));
        }

        let iter = self.buffer.iter();

        storage.batch_mixed(iter).map_err(DatabaseError::Storage)
    }

    /// Discards all pending operations without applying them.
    /// The transaction is consumed by this operation.
    pub fn rollback(self) {
        // Simply drop the transaction, discarding all pending operations
        drop(self);
    }
}
