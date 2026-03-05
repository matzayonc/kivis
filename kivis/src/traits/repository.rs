use core::ops::Range;
use core::{error::Error, fmt::Debug};

extern crate alloc;
use alloc::vec::Vec;

use crate::{BatchOp, BufferOverflowError, TryApplyError, Unifier, UnifierData, UnifierPair};

/// A trait defining a repository backend decoupled from serialization.
///
/// The repository is responsible for storing and retrieving key-value pairs
/// without knowledge of how they are serialized. This allows for better
/// separation of concerns between data storage and serialization logic.
pub trait Repository {
    /// Key type for the repository (the buffer type, e.g., Vec<u8> or String).
    type K: UnifierData;

    /// Value type for the repository (the buffer type, e.g., Vec<u8> or String).
    type V: UnifierData;

    /// Error type returned by repository operations.
    type Error: Debug + Error + From<BufferOverflowError>;

    /// Insert a key-value pair into the repository.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails to insert the key-value pair.
    fn insert_entry(
        &mut self,
        key: <Self::K as UnifierData>::View<'_>,
        value: <Self::V as UnifierData>::View<'_>,
    ) -> Result<(), Self::Error>;

    /// Retrieve the value associated with the given key from the repository.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while retrieving the value.
    fn get_entry(
        &self,
        key: <Self::K as UnifierData>::View<'_>,
    ) -> Result<Option<Self::V>, Self::Error>;

    /// Remove the value associated with the given key from the repository.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while removing the value.
    fn remove_entry(
        &mut self,
        key: <Self::K as UnifierData>::View<'_>,
    ) -> Result<Option<Self::V>, Self::Error>;
    /// Iterate over the keys in the repository that are in range.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails during iteration.
    fn scan_range(
        &self,
        range: Range<Self::K>,
    ) -> Result<impl Iterator<Item = IterationItem<Self::K, Self::Error>>, Self::Error>;

    /// Execute mixed insert and delete operations from a fallible iterator.
    ///
    /// All operations are validated before any write is applied. If the iterator
    /// yields an error, no writes are performed.
    ///
    /// # Errors
    ///
    /// Returns [`TryApplyError::Iterator`] if the iterator yields an error, or
    /// [`TryApplyError::Storage`] if the underlying storage fails.
    fn try_apply<U, E>(
        &mut self,
        operations: impl Iterator<Item = Result<BatchOp<U>, E>>,
    ) -> Result<(), TryApplyError<E, Self::Error>>
    where
        U: UnifierPair,
        U::KeyUnifier: Unifier<D = Self::K>,
        U::ValueUnifier: Unifier<D = Self::V>,
    {
        let ops: Vec<_> = operations
            .collect::<Result<_, _>>()
            .map_err(TryApplyError::Iterator)?;
        self.apply(ops.into_iter()).map_err(TryApplyError::Storage)
    }

    /// Execute mixed insert and delete operations.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the insert or remove operations fail.
    fn apply<U>(&mut self, operations: impl Iterator<Item = BatchOp<U>>) -> Result<(), Self::Error>
    where
        U: UnifierPair,
        U::KeyUnifier: Unifier<D = Self::K>,
        U::ValueUnifier: Unifier<D = Self::V>,
    {
        for op in operations {
            match op {
                BatchOp::Insert { key, value } => {
                    self.insert_entry(key.as_view(), value.as_view())?;
                }
                BatchOp::Delete { key } => {
                    self.remove_entry(key.as_view())?;
                }
            }
        }

        Ok(())
    }
}

type IterationItem<K, E> = Result<K, E>;
