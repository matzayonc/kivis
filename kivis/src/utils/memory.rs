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
};

use crate::{BufferOverflowError, Repository, Storage};

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
    /// Buffer overflow error
    BufferOverflow,
}

impl Error for MemoryStorageError {}

impl Display for MemoryStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization(e) => write!(f, "Serialization error: {e:?}"),
            Self::Deserialization(e) => write!(f, "Deserialization error: {e:?}"),
            Self::BufferOverflow => write!(f, "Buffer overflow error"),
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

impl From<BufferOverflowError> for MemoryStorageError {
    fn from(_: BufferOverflowError) -> Self {
        Self::BufferOverflow
    }
}

impl Storage for MemoryStorage {
    type Repo = Self;
    type KeyUnifier = Configuration;
    type ValueUnifier = Configuration;

    fn repository(&self) -> &Self::Repo {
        self
    }

    fn repository_mut(&mut self) -> &mut Self::Repo {
        self
    }
}

impl Repository for MemoryStorage {
    type K = [u8];
    type V = [u8];
    type Error = MemoryStorageError;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.insert(Reverse(key.to_vec()), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.get(&Reverse(key.to_vec())).cloned())
    }

    fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.remove(&Reverse(key.to_vec())))
    }

    fn iter_keys(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::Error>>, Self::Error> {
        let reverse_range = Reverse(range.end)..Reverse(range.start);

        let iter = self.range(reverse_range);
        Ok(iter.map(|(k, _v)| Ok(k.0.clone())))
    }
}
