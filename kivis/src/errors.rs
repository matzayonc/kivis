use core::{
    error::Error,
    fmt::{self, Debug},
};

use bincode::config::Configuration;

use crate::{BufferOverflowError, BufferOverflowOr, Storage, Unifier};

/// Errors that can occur while interacting with the database.
///
/// These errors can be caused by issues with the storage backend (including serialization/deserialization)
/// or internal database logic errors.
#[derive(Debug)]
pub enum DatabaseError<S: Storage> {
    /// Storage errors that occur while interacting with the storage backend.
    /// This includes IO errors, serialization errors, and deserialization errors.
    Storage(S::StoreError),
    /// Errors that occur when trying to increment a key.
    FailedToIncrement,
    /// Internal errors that should never occur during normal operation of the database.
    Internal(InternalDatabaseError),
}

/// Internal errors that should never arise during normal operation of the database.
///
/// These errors indicate a bug in the database implementation or database corruption.
#[derive(Debug)]
pub enum InternalDatabaseError {
    /// An entry from another table was found when iterating over another table.
    InvalidScope,
    /// An entry from another table was found when iterating over an index.
    UnexpectedScopeInIndex,
    /// Internal error caused by a missing index entry.
    MissingIndexEntry,
    /// Internal serialization error, should never occur.
    Serialization(<Configuration as Unifier>::SerError),
    /// Internal deserialization error, most likely caused by database corruption.
    Deserialization(<Configuration as Unifier>::DeError),
}

// This cannot be a [`From`] implementation because of orphan rules.
impl<S> DatabaseError<S>
where
    S: Storage,
{
    /// Creates a new `DatabaseError::Storage` from the given storage error.
    pub(crate) fn from_buffer_overflow_or(
        e: BufferOverflowOr<<S::Serializer as Unifier>::SerError>,
    ) -> Self {
        match e.0 {
            Some(err) => DatabaseError::Storage(err.into()),
            None => DatabaseError::Storage(BufferOverflowError.into()),
        }
    }
}

impl<S: Storage> From<InternalDatabaseError> for DatabaseError<S> {
    fn from(e: InternalDatabaseError) -> Self {
        DatabaseError::Internal(e)
    }
}

impl<S: Storage> fmt::Display for DatabaseError<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Storage(ref s) => write!(f, "Storage error: {s}"),
            Self::FailedToIncrement => write!(f, "Failed to increment key value"),
            Self::Internal(ref e) => write!(f, "Internal database error: {e}"),
        }
    }
}

impl fmt::Display for InternalDatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::InvalidScope => write!(f, "Invalid scope"),
            Self::UnexpectedScopeInIndex => write!(f, "Unexpected scope in index"),
            Self::MissingIndexEntry => write!(f, "Missing index entry"),
            Self::Serialization(ref e) => write!(f, "Serialization error: {e}"),
            Self::Deserialization(ref e) => write!(f, "Deserialization error: {e}"),
        }
    }
}

impl<S: Storage + Debug> Error for DatabaseError<S> {}
