use std::fmt::{self, Debug, Display};

use crate::traits::SerializationError;

#[derive(Debug)]
pub enum DatabaseError<S: Debug + Display + Eq + PartialEq> {
    Serialization(SerializationError),
    Deserialization(SerializationError),
    Io(S),
    Autoincrement,
    ToAutoincrement,
}

impl<S: Debug + Display + Eq + PartialEq> fmt::Display for DatabaseError<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Serialization(ref e) => write!(f, "Serialization error: {}", e),
            Self::Deserialization(ref e) => write!(f, "Deserialization error: {}", e),
            Self::Io(ref s) => write!(f, "IO error: {}", s),
            Self::Autoincrement => write!(f, "Autoincrement error"),
            Self::ToAutoincrement => write!(f, "Failed to convert to autoincrement key"),
        }
    }
}
