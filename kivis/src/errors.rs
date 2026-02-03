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
pub enum DatabaseError<S: Storage> {
    /// Storage errors that occur while interacting with the storage backend.
    /// This includes IO errors, serialization errors, and deserialization errors.
    Storage(S::Error),
    Serialization(<S::Serializer as Unifier>::SerError),
    Deserialization(<S::Serializer as Unifier>::DeError),
    /// Errors that occur when trying to increment a key.
    FailedToIncrement,
    /// Internal errors that should never occur during normal operation of the database.
    Internal(InternalDatabaseError),
}

impl<S: Storage> Debug for DatabaseError<S>
where
    S::Error: Debug,
    <S::Serializer as Unifier>::SerError: Debug,
    <S::Serializer as Unifier>::DeError: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(e) => f.debug_tuple("Storage").field(e).finish(),
            Self::Serialization(e) => f.debug_tuple("Serialization").field(e).finish(),
            Self::Deserialization(e) => f.debug_tuple("Deserialization").field(e).finish(),
            Self::FailedToIncrement => write!(f, "FailedToIncrement"),
            Self::Internal(e) => f.debug_tuple("Internal").field(e).finish(),
        }
    }
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
            Some(err) => DatabaseError::Serialization(err),
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
            Self::Serialization(ref e) => write!(f, "Serialization error: {e}"),
            Self::Deserialization(ref e) => write!(f, "Deserialization error: {e}"),
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
