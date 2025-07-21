use std::fmt::{self, Debug, Display};

use crate::traits::SerializationError;

/// Errors that can occur while interacting with the database.
///
/// These errors can be caused by issues with the storage backend, serialization/deserialization problems, or internal database logic errors.
#[derive(Debug)]
pub enum DatabaseError<S: Debug + Display> {
    /// Errors that occur during serialization of records.
    Serialization(SerializationError),
    /// Errors that occur during deserialization of records.
    Deserialization(SerializationError),
    /// IO errors that occur while interacting with the storage backend.
    Io(S),
    /// Errors that occur when trying to increment a key.
    FailedToIncrement,
    /// Errors that occur when an auto-increment key does not implement the required traits correctly.
    // TODO: This should be removed once the requirement for auto-increment keys is removed.
    ToAutoincrement,
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
            Self::FailedToIncrement => write!(f, "Autoincrement error"),
            Self::ToAutoincrement => write!(f, "Failed to convert to autoincrement key"),
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
