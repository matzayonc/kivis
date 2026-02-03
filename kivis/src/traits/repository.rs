#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::Range;
use std::{error::Error, fmt::Debug};

use crate::{BatchOp, BufferOverflowError, UnifierData};

/// A trait defining a repository backend decoupled from serialization.
///
/// The repository is responsible for storing and retrieving key-value pairs
/// without knowledge of how they are serialized. This allows for better
/// separation of concerns between data storage and serialization logic.
pub trait Repository {
    /// Key type for the repository.
    type K: UnifierData + ?Sized;

    /// Value type for the repository.
    type V: UnifierData + ?Sized;

    /// Error type returned by repository operations.
    type Error: Debug + Error + Eq + PartialEq + From<BufferOverflowError>;

    /// Insert a key-value pair into the repository.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails to insert the key-value pair.
    fn insert(
        &mut self,
        key: <Self::K as UnifierData>::View<'_>,
        value: <Self::V as UnifierData>::View<'_>,
    ) -> Result<(), Self::Error>;

    /// Retrieve the value associated with the given key from the repository.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while retrieving the value.
    fn get(
        &self,
        key: <Self::K as UnifierData>::View<'_>,
    ) -> Result<Option<<Self::V as UnifierData>::Owned>, Self::Error>;

    /// Remove the value associated with the given key from the repository.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while removing the value.
    fn remove(
        &mut self,
        key: <Self::K as UnifierData>::View<'_>,
    ) -> Result<Option<<Self::V as UnifierData>::Owned>, Self::Error>;
    /// Iterate over the keys in the repository that are in range.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails during iteration.
    fn iter_keys(
        &self,
        range: Range<<Self::K as UnifierData>::Owned>,
    ) -> Result<impl Iterator<Item = IterationItem<Self::K, Self::Error>>, Self::Error>;

    /// Execute mixed insert and delete operations.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the insert or remove operations fail.
    fn batch_mixed<'a>(
        &mut self,
        operations: impl Iterator<Item = BatchOp<'a, Self::K, Self::V>>,
    ) -> Result<BatchMixedResult<Self::V>, Self::Error> {
        let mut deleted = Vec::new();
        for op in operations {
            match op {
                BatchOp::Insert { key, value } => {
                    self.insert(key, value)?;
                }
                BatchOp::Delete { key } => {
                    deleted.push(self.remove(key)?);
                }
            }
        }

        Ok(deleted)
    }
}

type BatchMixedResult<V> = Vec<Option<<V as UnifierData>::Owned>>;
type IterationItem<K, E> = Result<<K as UnifierData>::Owned, E>;
