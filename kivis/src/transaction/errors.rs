use crate::{BufferOverflowOr, Unifier, UnifierPair};
use core::error::Error;
use core::fmt::{Debug, Display};

/// Error produced by [`Converter::apply`] and [`apply_record_ops`], covering both
/// serialization failures and repository write/remove failures.
pub enum ApplyError<U: UnifierPair, E> {
    /// A serialization error occurred while preparing keys or values.
    Transaction(TransactionError<U>),
    /// The underlying repository returned an error.
    Storage(E),
}

impl<U: UnifierPair, E: Debug> Debug for ApplyError<U, E>
where
    <U::KeyUnifier as Unifier>::SerError: Debug,
    <U::ValueUnifier as Unifier>::SerError: Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transaction(e) => f.debug_tuple("Transaction").field(e).finish(),
            Self::Storage(e) => f.debug_tuple("Storage").field(e).finish(),
        }
    }
}

/// Errors that can occur during transaction buffer operations
pub enum TransactionError<UP: UnifierPair> {
    KeySerialization(<UP::KeyUnifier as Unifier>::SerError),
    ValueSerialization(<UP::ValueUnifier as Unifier>::SerError),
    BufferOverflow,
}

impl<UP: UnifierPair> Debug for TransactionError<UP>
where
    <UP::KeyUnifier as Unifier>::SerError: Debug,
    <UP::ValueUnifier as Unifier>::SerError: Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::KeySerialization(e) => f.debug_tuple("KeySerialization").field(e).finish(),
            Self::ValueSerialization(e) => f.debug_tuple("ValueSerialization").field(e).finish(),
            Self::BufferOverflow => write!(f, "BufferOverflow"),
        }
    }
}

impl<UP: UnifierPair> Display for TransactionError<UP>
where
    <UP::KeyUnifier as Unifier>::SerError: Display,
    <UP::ValueUnifier as Unifier>::SerError: Display,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::KeySerialization(e) => write!(f, "Key serialization error: {e}"),
            Self::ValueSerialization(e) => write!(f, "Value serialization error: {e}"),
            Self::BufferOverflow => write!(f, "Buffer overflow"),
        }
    }
}

impl<UP: UnifierPair> Error for TransactionError<UP>
where
    <UP::KeyUnifier as Unifier>::SerError: Error + 'static,
    <UP::ValueUnifier as Unifier>::SerError: Error + 'static,
{
}

impl<UP: UnifierPair> From<BufferOverflowOr<<UP::KeyUnifier as Unifier>::SerError>>
    for TransactionError<UP>
{
    fn from(e: BufferOverflowOr<<UP::KeyUnifier as Unifier>::SerError>) -> Self {
        match e.0 {
            Some(err) => TransactionError::KeySerialization(err),
            None => TransactionError::BufferOverflow,
        }
    }
}

impl<UP: UnifierPair> TransactionError<UP> {
    pub(crate) fn from_value(e: BufferOverflowOr<<UP::ValueUnifier as Unifier>::SerError>) -> Self {
        match e.0 {
            Some(err) => TransactionError::ValueSerialization(err),
            None => TransactionError::BufferOverflow,
        }
    }
}
