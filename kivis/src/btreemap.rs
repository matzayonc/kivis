use std::{
    cmp::Reverse,
    collections::BTreeMap,
    error::Error,
    fmt::{Debug, Display},
    ops::Range,
};

use bincode::{
    config::Configuration,
    error::{DecodeError, EncodeError},
    serde::{decode_from_slice, encode_to_vec},
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{Storage, Unifier};

/// A memory-based storage implementation using a [`BTreeMap`].
///
/// This storage backend keeps all data in memory and uses reverse-ordered keys
/// for efficient range queries. Implements the [`Storage`] trait to be used as a storage backend.
pub type MemoryStorage = BTreeMap<Reverse<Vec<u8>>, Vec<u8>>;

#[derive(Clone, Copy, Default)]
pub struct BincodeSerializer(Configuration);

// Manual `Debug` impl is necessary because `Configuration` does not implement `Debug`.
impl Debug for BincodeSerializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BincodeSerializer")
    }
}

impl Unifier for BincodeSerializer {
    type D = Vec<u8>;
    type SerError = EncodeError;
    type DeError = DecodeError;

    fn serialize_key(&self, data: impl Serialize) -> Result<Self::D, Self::SerError> {
        encode_to_vec(data, self.0)
    }

    fn deserialize_key<T: DeserializeOwned>(&self, data: &Self::D) -> Result<T, Self::DeError> {
        Ok(decode_from_slice(data, self.0)?.0)
    }
}

/// Error type for [`MemoryStorage`] operations.
#[derive(Debug)]
pub enum MemoryStorageError {
    /// Serialization error
    Serialization(EncodeError),
    /// Deserialization error
    Deserialization(DecodeError),
}

impl Display for MemoryStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization(e) => write!(f, "Serialization error: {e:?}"),
            Self::Deserialization(e) => write!(f, "Deserialization error: {e:?}"),
        }
    }
}
impl Error for MemoryStorageError {}

impl PartialEq for MemoryStorageError {
    fn eq(&self, other: &Self) -> bool {
        // Compare based on variant only, not the actual error content
        matches!(
            (self, other),
            (Self::Serialization(_), Self::Serialization(_))
                | (Self::Deserialization(_), Self::Deserialization(_))
        )
    }
}

impl Eq for MemoryStorageError {}

impl From<EncodeError> for MemoryStorageError {
    fn from(e: EncodeError) -> Self {
        Self::Serialization(e)
    }
}

impl From<DecodeError> for MemoryStorageError {
    fn from(e: DecodeError) -> Self {
        Self::Deserialization(e)
    }
}

impl Storage for MemoryStorage {
    type Serializer = BincodeSerializer;
    type StoreError = MemoryStorageError;

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError> {
        self.insert(Reverse(key), value);
        Ok(())
    }

    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.get(&Reverse(key)).cloned())
    }

    fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.remove(&Reverse(key)))
    }

    fn scan_keys(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError> {
        let reverse_range = Reverse(range.end)..Reverse(range.start);

        let iter = self.range(reverse_range);
        Ok(iter.map(|(k, _v)| Ok(k.0.clone())))
    }
}
