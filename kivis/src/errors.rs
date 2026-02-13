use core::{
    error::Error,
    fmt::{self, Debug, Display},
};

use bincode::config::Configuration;

use crate::{Repository, Storage, Unifier};

#[cfg(feature = "atomic")]
use crate::transaction::TransactionError;

#[derive(Debug, PartialEq, Eq)]
pub struct BufferOverflowError;
impl Display for BufferOverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for BufferOverflowError {}

pub struct BufferOverflowOr<E>(pub Option<E>);
impl<E> BufferOverflowOr<E> {
    pub fn overflow(_: BufferOverflowError) -> Self {
        BufferOverflowOr(None)
    }
}

impl<E> From<E> for BufferOverflowOr<E> {
    fn from(e: E) -> Self {
        BufferOverflowOr(Some(e))
    }
}
impl<E: Debug> Debug for BufferOverflowOr<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(ref e) => write!(f, "Error({e:?})"),
            None => write!(f, "BufferOverflow"),
        }
    }
}
impl<E: Display> Display for BufferOverflowOr<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(ref e) => e.fmt(f),
            None => Display::fmt(&BufferOverflowError, f),
        }
    }
}
impl<E: Display + Debug> Error for BufferOverflowOr<E> {}

/// Errors that can occur while interacting with the database.
///
/// These errors can be caused by issues with the storage backend (including serialization/deserialization)
/// or internal database logic errors.
pub enum DatabaseError<S: Storage> {
    /// Storage errors that occur while interacting with the storage backend.
    /// This includes IO errors, serialization errors, and deserialization errors.
    Storage(<S::Repo as Repository>::Error),
    KeySerialization(<S::KeyUnifier as Unifier>::SerError),
    ValueSerialization(<S::ValueUnifier as Unifier>::SerError),
    KeyDeserialization(<S::KeyUnifier as Unifier>::DeError),
    ValueDeserialization(<S::ValueUnifier as Unifier>::DeError),
    /// Errors that occur when trying to increment a key.
    FailedToIncrement,
    /// Internal errors that should never occur during normal operation of the database.
    Internal(InternalDatabaseError),
}

impl<S: Storage> Debug for DatabaseError<S>
where
    <S::Repo as Repository>::Error: Debug,
    <S::KeyUnifier as Unifier>::SerError: Debug,
    <S::ValueUnifier as Unifier>::SerError: Debug,
    <S::KeyUnifier as Unifier>::DeError: Debug,
    <S::ValueUnifier as Unifier>::DeError: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(e) => f.debug_tuple("Storage").field(e).finish(),
            Self::KeySerialization(e) => f.debug_tuple("Serialization").field(e).finish(),
            Self::ValueSerialization(e) => f.debug_tuple("ValueSerialization").field(e).finish(),
            Self::KeyDeserialization(e) => f.debug_tuple("KeyDeserialization").field(e).finish(),
            Self::ValueDeserialization(e) => {
                f.debug_tuple("ValueDeserialization").field(e).finish()
            }
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
        e: BufferOverflowOr<<S::KeyUnifier as Unifier>::SerError>,
    ) -> Self {
        match e.0 {
            Some(err) => DatabaseError::KeySerialization(err),
            None => DatabaseError::Storage(BufferOverflowError.into()),
        }
    }

    /// Creates a new `DatabaseError` from a transaction error.
    #[cfg(feature = "atomic")]
    pub(crate) fn from_transaction_error(
        e: TransactionError<
            <S::KeyUnifier as Unifier>::SerError,
            <S::ValueUnifier as Unifier>::SerError,
        >,
    ) -> Self {
        match e {
            TransactionError::KeySerialization(err) => DatabaseError::KeySerialization(err),
            TransactionError::ValueSerialization(err) => DatabaseError::ValueSerialization(err),
            TransactionError::BufferOverflow => DatabaseError::Storage(BufferOverflowError.into()),
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
            Self::KeySerialization(ref e) => write!(f, "Key serialization error: {e}"),
            Self::ValueSerialization(ref e) => write!(f, "Value serialization error: {e}"),
            Self::KeyDeserialization(ref e) => write!(f, "Key deserialization error: {e}"),
            Self::ValueDeserialization(ref e) => write!(f, "Value deserialization error: {e}"),
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
