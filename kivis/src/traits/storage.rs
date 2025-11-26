#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use bincode::config::Configuration;
use core::{fmt::Display, ops::Range};
use serde::{de::DeserializeOwned, Serialize};

use super::Debug;

/// A trait defining a storage backend for the database.
///
/// The storage backend is responsible for storing and retrieving records and their associated indexes.
/// It defines methods for inserting, getting, removing, and iterating over keys in the storage.
/// All storage operations are defined over `Vec<u8>` keys and values.
pub trait BinaryStorage {
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

pub trait Storage {
    type StoreError: Debug + Display + Eq + PartialEq;

    fn insert(
        &mut self,
        key: impl Serialize + Sized,
        value: impl Serialize + Sized,
    ) -> Result<(), Self::StoreError>;

    fn get<V: DeserializeOwned + Sized>(
        &self,
        key: impl Serialize + Sized,
    ) -> Result<Option<V>, Self::StoreError>;

    fn remove<V: DeserializeOwned + Sized>(
        &mut self,
        key: impl Serialize + Sized,
    ) -> Result<Option<V>, Self::StoreError>;

    fn iter_keys<V: DeserializeOwned + Sized>(
        &self,
        range: Range<impl Serialize + Sized>,
    ) -> Result<impl Iterator<Item = Result<V, Self::StoreError>>, Self::StoreError>
    where
        Self: Sized;
}

impl<T> Storage for T
where
    T: BinaryStorage,
    T::StoreError: From<bincode::error::EncodeError> + From<bincode::error::DecodeError>,
{
    type StoreError = T::StoreError;

    fn insert(
        &mut self,
        key: impl Serialize,
        value: impl Serialize,
    ) -> Result<(), Self::StoreError> {
        let key = bincode::serde::encode_to_vec::<_, Configuration>(key, Configuration::default())?;
        let value =
            bincode::serde::encode_to_vec::<_, Configuration>(value, Configuration::default())?;
        <Self as BinaryStorage>::insert(self, key, value)
    }

    fn get<V: DeserializeOwned>(&self, key: impl Serialize) -> Result<Option<V>, Self::StoreError> {
        let key = bincode::serde::encode_to_vec::<_, Configuration>(key, Configuration::default())?;
        let Some(value) = <Self as BinaryStorage>::get(self, key)? else {
            return Ok(None);
        };
        let (deserialized, _rem) = bincode::serde::decode_from_slice::<_, Configuration>(
            &value,
            Configuration::default(),
        )?;

        Ok(Some(deserialized))
    }

    fn remove<V: DeserializeOwned>(
        &mut self,
        key: impl Serialize,
    ) -> Result<Option<V>, Self::StoreError> {
        let key = bincode::serde::encode_to_vec::<_, Configuration>(key, Configuration::default())?;
        let Some(value) = <Self as BinaryStorage>::remove(self, key)? else {
            return Ok(None);
        };
        let (deserialized, _rem) = bincode::serde::decode_from_slice::<_, Configuration>(
            &value,
            Configuration::default(),
        )?;

        Ok(Some(deserialized))
    }

    fn iter_keys<V: DeserializeOwned>(
        &self,
        range: Range<impl Serialize>,
    ) -> Result<impl Iterator<Item = Result<V, Self::StoreError>>, Self::StoreError>
    where
        Self: Sized,
    {
        let start = bincode::serde::encode_to_vec::<_, Configuration>(
            range.start,
            Configuration::default(),
        )?;
        let end =
            bincode::serde::encode_to_vec::<_, Configuration>(range.end, Configuration::default())?;
        let iter = <Self as BinaryStorage>::iter_keys(self, start..end)?;

        Ok(iter.map(|res| {
            res.and_then(|value| {
                let (deserialized, _rem) = bincode::serde::decode_from_slice::<_, Configuration>(
                    &value,
                    Configuration::default(),
                )?;
                Ok(deserialized)
            })
        }))
    }
}
