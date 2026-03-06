use crate::{
    DatabaseEntry, DatabaseError, DeriveKey, Incrementable, Manifest, Manifests, RecordKey,
    Repository, Storage, TryApplyError, UnifierPair,
    transaction::{buffer::PreBufferOps, errors::TransactionError},
};

use super::buffer::TransactionBuffer;

/// A database transaction that accumulates typed records in a pre-buffer and serializes
/// them one at a time directly to storage on commit.
///
/// This struct is always available, but the `commit` method is only available when the "atomic" feature is enabled.
pub struct DatabaseTransaction<M: Manifest<U>, U: UnifierPair + 'static> {
    pre_buffer: TransactionBuffer<M, U>,
    unifiers: U,
}

impl<M: Manifest<U>, U: UnifierPair + 'static> DatabaseTransaction<M, U> {
    /// Creates a new empty transaction with the specified serialization configuration.
    #[must_use]
    pub fn new(unifiers: U) -> Self {
        Self {
            pre_buffer: TransactionBuffer::<M, U>::empty(),
            unifiers,
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
            .push(PreBufferOps::Insert, (original_key.clone(), record));
        Ok(original_key)
    }

    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if writing to the underlying storage fails.
    ///
    pub fn put<S, R>(&mut self, record: R, manifest: &mut M) -> Result<R::Key, DatabaseError<S>>
    where
        S: Storage<Unifiers = U>,
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
            .push(PreBufferOps::Put, (new_key.clone(), record));
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
        Ok(())
    }

    /// Returns true if the transaction has no pending operations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pre_buffer.is_empty()
    }

    /// Commits all pending operations to the storage.
    ///
    /// Each record is serialized and written to storage individually — no accumulated byte buffer.
    /// The transaction is consumed by this operation.
    ///
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if any storage operation fails.
    pub fn commit<S>(self, storage: &mut S) -> Result<(), DatabaseError<S>>
    where
        S: Storage<Unifiers = U>,
    {
        if self.is_empty() {
            return Ok(());
        }

        let unifiers = self.unifiers;
        self.pre_buffer.process(|op, record| {
            storage
                .repository_mut()
                .try_apply(M::record_ops(op, &record, unifiers))
                .map_err(|e| match e {
                    TryApplyError::Iterator(te) => DatabaseError::<S>::from_transaction_error(te),
                    TryApplyError::Storage(se) => DatabaseError::Storage(se),
                })
        })
    }

    /// Discards all pending operations without applying them.
    /// The transaction is consumed by this operation.
    pub fn rollback(self) {
        drop(self);
    }
}
