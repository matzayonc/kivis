use crate::BufferOverflowOr;
use core::error::Error;
use core::fmt::{Debug, Display};

/// Errors that can occur during transaction buffer operations
pub enum TransactionError<KE, VE> {
    KeySerialization(KE),
    ValueSerialization(VE),
    BufferOverflow,
}

impl<KE: Debug, VE: Debug> Debug for TransactionError<KE, VE> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::KeySerialization(e) => f.debug_tuple("KeySerialization").field(e).finish(),
            Self::ValueSerialization(e) => f.debug_tuple("ValueSerialization").field(e).finish(),
            Self::BufferOverflow => write!(f, "BufferOverflow"),
        }
    }
}

impl<KE: Display, VE: Display> Display for TransactionError<KE, VE> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::KeySerialization(e) => write!(f, "Key serialization error: {e}"),
            Self::ValueSerialization(e) => write!(f, "Value serialization error: {e}"),
            Self::BufferOverflow => write!(f, "Buffer overflow"),
        }
    }
}

impl<KE: Error + 'static, VE: Error + 'static> Error for TransactionError<KE, VE> {}

impl<KE, VE> From<BufferOverflowOr<KE>> for TransactionError<KE, VE> {
    fn from(e: BufferOverflowOr<KE>) -> Self {
        match e.0 {
            Some(err) => TransactionError::KeySerialization(err),
            None => TransactionError::BufferOverflow,
        }
    }
}

impl<KE, VE> TransactionError<KE, VE> {
    pub(crate) fn from_value(e: BufferOverflowOr<VE>) -> Self {
        match e.0 {
            Some(err) => TransactionError::ValueSerialization(err),
            None => TransactionError::BufferOverflow,
        }
    }
}
