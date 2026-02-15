use core::ops::Range;
use std::error::Error;
use std::fmt::{Debug, Display};

use crate::{BufferOp, BufferOverflowError, BufferOverflowOr, Repository, Storage, Unifier};
use serde::Serialize;

/// Error type for [`SledStorage`] operations.
#[derive(Debug)]
pub enum SledStorageError {
    /// Sled database error
    Sled(sled::Error),
    /// Postcard serialization error
    Serialization(postcard::Error),
    /// Postcard deserialization error
    Deserialization(postcard::Error),
    /// Buffer overflow error
    BufferOverflow,
}

impl Display for SledStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sled(e) => write!(f, "Sled error: {e}"),
            Self::Serialization(e) => write!(f, "Serialization error: {e}"),
            Self::Deserialization(e) => write!(f, "Deserialization error: {e}"),
            Self::BufferOverflow => write!(f, "Buffer overflow"),
        }
    }
}

impl Error for SledStorageError {}

impl From<sled::Error> for SledStorageError {
    fn from(e: sled::Error) -> Self {
        Self::Sled(e)
    }
}

impl From<postcard::Error> for SledStorageError {
    fn from(e: postcard::Error) -> Self {
        Self::Serialization(e)
    }
}

impl From<BufferOverflowError> for SledStorageError {
    fn from(_: BufferOverflowError) -> Self {
        Self::BufferOverflow
    }
}

/// Postcard unifier for sled storage with Vec<u8> buffers
#[derive(Debug, Clone, Copy, Default)]
pub struct PostcardUnifier;

impl Unifier for PostcardUnifier {
    type D = Vec<u8>;
    type SerError = postcard::Error;
    type DeError = postcard::Error;

    fn serialize(
        &self,
        buffer: &mut Self::D,
        data: impl Serialize,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        let start = buffer.len();
        let serialized = postcard::to_allocvec(&data)?;
        buffer.extend_from_slice(&serialized);
        Ok((start, buffer.len()))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(
        &self,
        data: &Self::D,
    ) -> Result<T, Self::DeError> {
        postcard::from_bytes(data)
    }
}

/// A sled-based storage implementation.
///
/// This storage backend uses the sled embedded database with postcard serialization.
/// Sled is a modern embedded database with ACID guarantees, suitable for production use.
#[derive(Debug, Clone)]
pub struct SledStorage {
    db: sled::Db,
}

impl SledStorage {
    /// Create a new in-memory sled storage (for testing)
    /// Create a new in-memory sled storage (for testing)
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be created.
    pub fn new() -> Result<Self, SledStorageError> {
        let db = sled::Config::new().temporary(true).open()?;
        Ok(Self { db })
    }

    /// Open a sled database at the specified path
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, SledStorageError> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    /// Create a sled database with custom configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn with_config(config: &sled::Config) -> Result<Self, SledStorageError> {
        let db = config.open()?;
        Ok(Self { db })
    }

    /// Get a reference to the underlying sled database
    #[must_use]
    pub fn db(&self) -> &sled::Db {
        &self.db
    }
}

impl Storage for SledStorage {
    type Repo = Self;
    type KeyUnifier = PostcardUnifier;
    type ValueUnifier = PostcardUnifier;
    type Container = Vec<BufferOp>;

    fn repository(&self) -> &Self::Repo {
        self
    }

    fn repository_mut(&mut self) -> &mut Self::Repo {
        self
    }
}

impl Repository for SledStorage {
    type K = Vec<u8>;
    type V = Vec<u8>;
    type Error = SledStorageError;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.db.insert(key, value)?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Self::V>, Self::Error> {
        match self.db.get(key)? {
            Some(ivec) => Ok(Some(ivec.to_vec())),
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: &[u8]) -> Result<Option<Self::V>, Self::Error> {
        match self.db.remove(key)? {
            Some(ivec) => Ok(Some(ivec.to_vec())),
            None => Ok(None),
        }
    }

    fn iter_keys(
        &self,
        range: Range<Self::K>,
    ) -> Result<impl Iterator<Item = Result<Self::K, Self::Error>>, Self::Error> {
        // Sled uses forward iteration, but kivis expects reverse order
        // Collect all keys in range and reverse them
        let keys: Vec<_> = self
            .db
            .range(range.start..range.end)
            .filter_map(Result::ok)
            .map(|(k, _)| k.to_vec())
            .collect();

        Ok(keys.into_iter().rev().map(Ok))
    }

    fn batch_mixed<'a>(
        &mut self,
        operations: impl Iterator<Item = crate::BatchOp<'a, Self::K, Self::V>>,
    ) -> Result<(), Self::Error> {
        let mut batch = sled::Batch::default();

        for op in operations {
            match op {
                crate::BatchOp::Insert { key, value } => {
                    batch.insert(key, value);
                }
                crate::BatchOp::Delete { key } => {
                    batch.remove(key);
                }
            }
        }

        self.db.apply_batch(batch)?;
        Ok(())
    }
}
