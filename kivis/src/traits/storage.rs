#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::{fmt::Display, ops::Range};

use super::Debug;

/// A trait defining a storage backend for the database.
///
/// The storage backend is responsible for storing and retrieving records and their associated indexes.
/// It defines methods for inserting, getting, removing, and iterating over keys in the storage.
/// All storage operations are defined over `Vec<u8>` keys and values.
pub trait Storage {
    /// Error type returned by storage operations.
    type StoreError: Debug + Display + Eq + PartialEq;

    /// Should insert the given key-value pair into the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails to insert the key-value pair.
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError>;
    /// Should retrieve the value associated with the given key from the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while retrieving the value.
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError>;
    /// Should remove the value associated with the given key from the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while removing the value.
    fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError>;
    /// Should iterate over the keys in the storage that are in range.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storages fails during iteration.
    fn iter_keys(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError>
    where
        Self: Sized;
}
