use std::fmt::{self, Debug, Display};

use crate::traits::SerializationError;

#[derive(Debug)]
pub enum DatabaseError<S: Debug + Display + Eq + PartialEq> {
    Serialization(SerializationError),
    Deserialization(SerializationError),
    Io(S),
    FailedToIncrement,
    ToAutoincrement,
    Internal(InternalDatabaseError),
}

#[derive(Debug)]
pub enum InternalDatabaseError {
    InvalidScope,
    UnexpectedScopeInIndex,
    Serialization(SerializationError),
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
            Self::Serialization(ref e) => write!(f, "Serialization error: {}", e),
            Self::Deserialization(ref e) => write!(f, "Deserialization error: {}", e),
            Self::Io(ref s) => write!(f, "IO error: {}", s),
            Self::FailedToIncrement => write!(f, "Autoincrement error"),
            Self::ToAutoincrement => write!(f, "Failed to convert to autoincrement key"),
            Self::Internal(ref e) => write!(f, "Internal database error: {}", e),
        }
    }
}

impl fmt::Display for InternalDatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::InvalidScope => write!(f, "Invalid scope"),
            Self::UnexpectedScopeInIndex => write!(f, "Unexpected scope in index"),
            Self::Serialization(ref e) => write!(f, "Serialization error: {}", e),
            Self::Deserialization(ref e) => write!(f, "Deserialization error: {}", e),
        }
    }
}
