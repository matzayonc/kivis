use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{Debug, Display},
    ops::Range,
};

use bincode::{
    config::Configuration,
    error::{DecodeError, EncodeError},
};

use crate::{BufferOverflowError, OrderedKeyConfig, Repository, Storage};

/// A memory-based storage implementation using a [`BTreeMap`].
///
/// This storage backend keeps all data in memory, ordered by the raw key bytes.
/// Keys are encoded with [`OrderedKeyConfig`] so that byte order matches key order.
/// Implements the [`Storage`] trait to be used as a storage backend.
pub type MemoryStorage = BTreeMap<Vec<u8>, Vec<u8>>;

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
    type Unifiers = (OrderedKeyConfig, Configuration);
    fn repository(&self) -> &Self::Repo {
        self
    }

    fn repository_mut(&mut self) -> &mut Self::Repo {
        self
    }
}

impl Repository for MemoryStorage {
    type K = Vec<u8>;
    type V = Vec<u8>;
    type Error = MemoryStorageError;

    fn insert_entry(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn get_entry(&self, key: &[u8]) -> Result<Option<Self::V>, Self::Error> {
        Ok(self.get(key).cloned())
    }

    fn remove_entry(&mut self, key: &[u8]) -> Result<Option<Self::V>, Self::Error> {
        Ok(self.remove(key))
    }

    fn scan_range(
        &self,
        range: Range<Self::K>,
    ) -> Result<impl DoubleEndedIterator<Item = Result<Self::K, Self::Error>>, Self::Error> {
        Ok(self.range(range).map(|(k, _v)| Ok(k.clone())))
    }
}
