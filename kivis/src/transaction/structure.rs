use crate::{
    BufferOpsContainer, DatabaseEntry, DatabaseError, DeriveKey, Incrementable, Manifest,
    Manifests, RecordKey, Repository, Storage, UnifierPair,
    transaction::{
        buffer::DatabaseTransactionBuffer, errors::TransactionError, pre_buffer::PreBufferOps,
    },
};

use super::pre_buffer::PreTransactionBuffer;
use core::marker::PhantomData;

/// A database transaction that accumulates low-level byte operations (writes and deletes)
/// without immediately applying them to storage.
///
/// This struct is always available, but the `commit` method is only available when the "atomic" feature is enabled.
pub struct DatabaseTransaction<M: Manifest, UP: UnifierPair, C: BufferOpsContainer> {
    pre_buffer: PreTransactionBuffer<M>,
    post_buffer: DatabaseTransactionBuffer<UP, C>,
    _marker: PhantomData<M>,
}

impl<M: Manifest, U: UnifierPair, C: BufferOpsContainer> DatabaseTransaction<M, U, C> {
    /// Creates a new empty transaction with the specified serialization configuration.
    #[must_use]
    pub fn new(unifiers: U) -> Self {
        Self {
            pre_buffer: PreTransactionBuffer::<M>::empty(),
            post_buffer: DatabaseTransactionBuffer::new(unifiers),
            _marker: PhantomData,
        }
    }

    /// # Errors
    ///
    /// Returns a [`TransactionError`] if serializing keys or values fails while preparing the writes.
    pub fn insert<K, R>(&mut self, record: R) -> Result<K, TransactionError<U>>
    where
        K: RecordKey<Record = R> + 'static,
        R: DeriveKey<Key = K> + DatabaseEntry<Key = K> + Clone + 'static,
        for<'f> &'f (K, R): Into<M::Record<'f>>,
        M: Manifests<R>,
    {
        let original_key = R::key(&record);
        self.pre_buffer
            .push(PreBufferOps::Insert, (original_key.clone(), record.clone()));
        self.post_buffer
            .prepare_writes::<R>(record, &original_key)?;
        Ok(original_key)
    }

    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if writing to the underlying storage fails.
    ///
    pub fn put<S, R>(&mut self, record: R, manifest: &mut M) -> Result<R::Key, DatabaseError<S>>
    where
        S: Storage<Container = C, Unifiers = U>,
        R: DatabaseEntry + Clone + 'static,
        R::Key: RecordKey<Record = R> + Incrementable + Ord + 'static,
        for<'f> &'f (R::Key, R): Into<M::Record<'f>>,
        M: Manifests<R>,
    {
        let last_key = manifest.last();
        let new_key = if let Some(k) = last_key {
            k.next_id().ok_or(DatabaseError::FailedToIncrement)?
        } else {
            R::Key::default()
        };

        self.pre_buffer
            .push(PreBufferOps::Put, (new_key.clone(), record.clone()));
        self.post_buffer
            .prepare_writes::<R>(record, &new_key)
            .map_err(DatabaseError::from_transaction_error)?;
        last_key.replace(new_key.clone());
        Ok(new_key)
    }

    /// # Errors
    ///
    /// Returns a [`TransactionError`] if serializing keys to delete fails.
    pub fn remove<R>(&mut self, key: &R::Key, record: &R) -> Result<(), TransactionError<U>>
    where
        R: DatabaseEntry + Clone + 'static,
        R::Key: RecordKey<Record = R> + Clone + 'static,
        for<'f> &'f (R::Key, R): Into<M::Record<'f>>,
        M: Manifests<R>,
    {
        self.pre_buffer
            .push(PreBufferOps::Delete, (key.clone(), record.clone()));
        self.post_buffer.prepare_deletes::<R>(record, key)
    }

    /// Returns true if the transaction has no pending operations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.post_buffer.is_empty()
    }

    /// Commits all pending operations to the storage.
    ///
    /// Either all operations succeed, or none of them are applied.
    /// The transaction is consumed by this operation.
    ///
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if any storage operation fails.
    pub fn commit<S>(self, storage: &mut S) -> Result<(), DatabaseError<S>>
    where
        S: Storage<Container = C, Unifiers = U>,
    {
        if self.is_empty() {
            return Ok(());
        }

        let iter = self.post_buffer.iter();

        storage
            .repository_mut()
            .apply(iter)
            .map_err(DatabaseError::Storage)
    }

    /// Discards all pending operations without applying them.
    /// The transaction is consumed by this operation.
    pub fn rollback(self) {
        // Simply drop the transaction, discarding all pending operations
        drop(self);
    }
}
