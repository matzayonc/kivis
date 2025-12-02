use core::{
    error::Error,
    fmt::{self, Debug},
};

use crate::{Storage, Unifier};

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
    Internal(InternalDatabaseError<S::Serializer>),
}

/// Internal errors that should never arise during normal operation of the database.
///
/// These errors indicate a bug in the database implementation or database corruption.
#[derive(Debug)]
pub enum InternalDatabaseError<U: Unifier> {
    /// An entry from another table was found when iterating over another table.
    InvalidScope,
    /// An entry from another table was found when iterating over an index.
    UnexpectedScopeInIndex,
    /// Internal error caused by a missing index entry.
    MissingIndexEntry,
    /// Internal serialization error, should never occur.
    Serialization(U::SerError),
    /// Internal deserialization error, most likely caused by database corruption.
    Deserialization(U::DeError),
}

impl<S: Storage> From<InternalDatabaseError<S::Serializer>> for DatabaseError<S> {
    fn from(e: InternalDatabaseError<S::Serializer>) -> Self {
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

impl<U: Unifier> fmt::Display for InternalDatabaseError<U> {
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
