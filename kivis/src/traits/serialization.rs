use bincode::{
    config::Configuration,
    serde::{decode_from_slice, encode_to_vec},
};

use super::{DeserializationError, DeserializeOwned, Serialize};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// A trait for converting keys to and from byte representations.
///
/// This trait is essential for storing keys in the database's key-value store.
/// It provides methods to serialize keys to bytes for storage and deserialize
/// them back for retrieval operations.
pub trait KeyBytes {
    /// Converts the key to bytes for storage.
    ///
    /// This method serializes the key into a byte vector that can be stored
    /// in the underlying storage backend.
    fn to_bytes(&self, serialization_config: Configuration) -> Vec<u8>;

    /// Reconstructs the key from bytes.
    ///
    /// This method deserializes a key from its byte representation, typically
    /// when retrieving data from storage.
    ///
    /// # Errors
    ///
    /// Returns a [`DeserializationError`] if the bytes cannot be deserialized
    /// into the key type.
    fn from_bytes(
        bytes: &[u8],
        serialization_config: Configuration,
    ) -> Result<Self, DeserializationError>
    where
        Self: Sized;
}

impl<T: Serialize + DeserializeOwned> KeyBytes for T {
    fn to_bytes(&self, serialization_config: Configuration) -> Vec<u8> {
        // This should never fail for well-formed types that implement Serialize
        encode_to_vec(self, serialization_config).expect("Serialization should not fail")
    }

    fn from_bytes(
        bytes: &[u8],
        serialization_config: Configuration,
    ) -> Result<Self, DeserializationError>
    where
        Self: Sized,
    {
        decode_from_slice::<Self, Configuration>(bytes, serialization_config).map(|(key, _)| key)
    }
}
