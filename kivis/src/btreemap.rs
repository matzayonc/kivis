use std::{cmp::Reverse, collections::BTreeMap, fmt::Display, ops::Range};

use bincode::{
    config::Configuration,
    error::{DecodeError, EncodeError},
};

use crate::Storage;

/// A memory-based storage implementation using a [`BTreeMap`].
///
/// This storage backend keeps all data in memory and uses reverse-ordered keys
/// for efficient range queries. Implements the [`Storage`] trait to be used as a storage backend.
pub type MemoryStorage = BTreeMap<Reverse<Vec<u8>>, Vec<u8>>;

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

impl PartialEq for MemoryStorageError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Serialization(a), Self::Serialization(b)) => a.to_string() == b.to_string(),
            (Self::Deserialization(a), Self::Deserialization(b)) => a.to_string() == b.to_string(),
            _ => false,
        }
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
    type Serializer = Configuration;
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

    fn iter_keys(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError> {
        let reverse_range = Reverse(range.end)..Reverse(range.start);

        let iter = self.range(reverse_range);
        Ok(iter.map(|(k, _v)| Ok(k.0.clone())))
    }
}
