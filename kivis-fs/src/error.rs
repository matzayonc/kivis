use std::fmt::Display;

use kivis::BufferOverflowError;

/// Errors that can occur during file storage operations.
#[derive(Debug)]
pub enum FileStoreError {
    /// An I/O error occurred while reading or writing files.
    Io(std::io::Error),
    /// A CSV serialization or deserialization error occurred.
    Serialization(csv::Error),
    /// A buffer overflow occurred during serialization.
    BufferOverflow,
}

impl Display for FileStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Serialization(e) => write!(f, "Serialization error: {}", e),
            Self::BufferOverflow => write!(f, "Buffer overflow error"),
        }
    }
}

impl std::error::Error for FileStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Serialization(e) => Some(e),
            Self::BufferOverflow => None,
        }
    }
}

impl From<csv::Error> for FileStoreError {
    fn from(e: csv::Error) -> Self {
        Self::Serialization(e)
    }
}

impl From<std::io::Error> for FileStoreError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<BufferOverflowError> for FileStoreError {
    fn from(_: BufferOverflowError) -> Self {
        Self::BufferOverflow
    }
}
