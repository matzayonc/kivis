#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::{fmt::Display, ops::Range};

use crate::Unifier;

use super::Debug;

pub trait Storage: StorageInner<<Self::Serializer as Unifier>::D> {
    type Serializer: Unifier + Default + Copy;
}

/// A trait defining a storage backend for the database.
///
/// The storage backend is responsible for storing and retrieving records and their associated indexes.
/// It defines methods for inserting, getting, removing, and iterating over keys in the storage.
/// All storage operations are defined over `Vec<u8>` keys and values.
pub trait StorageInner<D = Vec<u8>> {
    /// Error type returned by storage operations.
    type StoreError: Debug + Display + Eq + PartialEq;

    /// Should insert the given key-value pair into the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails to insert the key-value pair.
    fn insert(&mut self, key: D, value: D) -> Result<(), Self::StoreError>;
    /// Should retrieve the value associated with the given key from the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while retrieving the value.
    fn get(&self, key: D) -> Result<Option<D>, Self::StoreError>;
    /// Should remove the value associated with the given key from the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while removing the value.
    fn remove(&mut self, key: D) -> Result<Option<D>, Self::StoreError>;
    /// Should iterate over the keys in the storage that are in range.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storages fails during iteration.
    fn iter_keys(
        &self,
        range: Range<D>,
    ) -> Result<impl Iterator<Item = Result<D, Self::StoreError>>, Self::StoreError>
    where
        Self: Sized;
}
