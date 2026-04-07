use core::ops::Range;
use core::{error::Error, fmt::Debug};

use crate::{ApplyError, BatchOp, BufferOverflowError, Unified, Unifier, UnifierPair};

/// A trait defining a repository backend decoupled from serialization.
///
/// The repository is responsible for storing and retrieving key-value pairs
/// without knowledge of how they are serialized. This allows for better
/// separation of concerns between data storage and serialization logic.
pub trait Repository {
    /// Key type for the repository (the buffer type, e.g., Vec<u8> or String).
    type K: Unified;

    /// Value type for the repository (the buffer type, e.g., Vec<u8> or String).
    type V: Unified;

    /// Error type returned by repository operations.
    type Error: Debug + Error + From<BufferOverflowError>;

    /// Insert a key-value pair into the repository.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails to insert the key-value pair.
    fn insert_entry(
        &mut self,
        key: <Self::K as Unified>::View<'_>,
        value: <Self::V as Unified>::View<'_>,
    ) -> Result<(), Self::Error>;

    /// Retrieve the value associated with the given key from the repository.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while retrieving the value.
    fn get_entry(
        &self,
        key: <Self::K as Unified>::View<'_>,
    ) -> Result<Option<Self::V>, Self::Error>;

    /// Remove the value associated with the given key from the repository.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while removing the value.
    fn remove_entry(
        &mut self,
        key: <Self::K as Unified>::View<'_>,
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
    /// Iterator errors are converted into `Self::Error` via [`From`]. Storage errors
    /// are returned directly.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` if the iterator yields an error (via `From<E>`) or if
    /// the underlying storage fails.
    fn apply<U, E>(
        &mut self,
        operations: impl Iterator<Item = Result<BatchOp<U>, E>>,
    ) -> Result<(), ApplyError<E, Self::Error>>
    where
        U: UnifierPair,
        U::KeyUnifier: Unifier<D = Self::K>,
        U::ValueUnifier: Unifier<D = Self::V>,
    {
        for op in operations {
            let op = op.map_err(ApplyError::Serialization)?;
            match op {
                BatchOp::Insert { key, value } => {
                    self.insert_entry(key.as_view(), value.as_view())
                        .map_err(ApplyError::Application)?;
                }
                BatchOp::Delete { key } => {
                    self.remove_entry(key.as_view())
                        .map_err(ApplyError::Application)?;
                }
            }
        }

        Ok(())
    }
}

type IterationItem<K, E> = Result<K, E>;
