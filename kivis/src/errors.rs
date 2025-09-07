use std::fmt::{self, Debug, Display};

use crate::traits::{DeserializationError, SerializationError};

/// Errors that can occur while interacting with the database.
///
/// These errors can be caused by issues with the storage backend, serialization/deserialization problems, or internal database logic errors.
#[derive(Debug)]
pub enum DatabaseError<S: Debug + Display> {
    /// Errors that occur during serialization of records.
    Serialization(SerializationError),
    /// Errors that occur during deserialization of records.
    Deserialization(DeserializationError),
    /// IO errors that occur while interacting with the storage backend.
    Io(S),
    /// Storage errors that occur during atomic operations.
    Storage(S),
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
    Serialization(SerializationError),
    /// Internal deserialization error, most likely caused by database corruption.
    Deserialization(SerializationError),
}

impl<S: Debug + Display + Eq + PartialEq> From<InternalDatabaseError> for DatabaseError<S> {
    fn from(e: InternalDatabaseError) -> Self {
        DatabaseError::Internal(e)
    }
}

impl<S: Debug + Display + Eq + PartialEq> fmt::Display for DatabaseError<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Serialization(ref e) => write!(f, "Serialization error: {e}"),
            Self::Deserialization(ref e) => write!(f, "Deserialization error: {e}"),
            Self::Io(ref s) => write!(f, "IO error: {s}"),
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
