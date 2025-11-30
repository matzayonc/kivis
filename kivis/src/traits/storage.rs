#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::{fmt::Display, ops::Range};

use crate::Unifier;

use super::Debug;

type KeysIteratorItem<S> =
    Result<<<S as Storage>::Serializer as Unifier>::D, <S as Storage>::StoreError>;

/// A trait defining a storage backend for the database.
///
/// The storage backend is responsible for storing and retrieving records and their associated indexes.
/// It defines methods for inserting, getting, removing, and iterating over keys in the storage.
/// All storage operations are defined over serialized byte data.
pub trait Storage {
    /// Serializer type used to convert data to/from bytes.
    type Serializer: Unifier + Default + Copy;

    /// Error type returned by storage operations.
    /// Must be able to represent serialization and deserialization errors.
    type StoreError: Debug
        + Display
        + Eq
        + PartialEq
        + From<<<Self as Storage>::Serializer as Unifier>::SerError>
        + From<<<Self as Storage>::Serializer as Unifier>::DeError>;

    /// Should insert the given key-value pair into the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails to insert the key-value pair.
    fn insert(
        &mut self,
        key: <Self::Serializer as Unifier>::D,
        value: <Self::Serializer as Unifier>::D,
    ) -> Result<(), Self::StoreError>;

    /// Should retrieve the value associated with the given key from the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while retrieving the value.
    fn get(
        &self,
        key: <Self::Serializer as Unifier>::D,
    ) -> Result<Option<<Self::Serializer as Unifier>::D>, Self::StoreError>;

    /// Should remove the value associated with the given key from the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while removing the value.
    fn remove(
        &mut self,
        key: <Self::Serializer as Unifier>::D,
    ) -> Result<Option<<Self::Serializer as Unifier>::D>, Self::StoreError>;

    /// Should iterate over the keys in the storage that are in range.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storages fails during iteration.
    fn iter_keys(
        &self,
        range: Range<<Self::Serializer as Unifier>::D>,
    ) -> Result<impl Iterator<Item = KeysIteratorItem<Self>>, Self::StoreError>
    where
        Self: Sized;
}
