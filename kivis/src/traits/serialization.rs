use super::*;

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
    fn to_bytes(&self) -> Vec<u8>;

    /// Reconstructs the key from bytes.
    ///
    /// This method deserializes a key from its byte representation, typically
    /// when retrieving data from storage.
    ///
    /// # Errors
    ///
    /// Returns a [`SerializationError`] if the bytes cannot be deserialized
    /// into the key type.
    fn from_bytes(bytes: &[u8]) -> Result<Self, SerializationError>
    where
        Self: Sized;
}
impl<T: Serialize + DeserializeOwned> KeyBytes for T {
    fn to_bytes(&self) -> Vec<u8> {
        // This should never fail for well-formed types that implement Serialize
        bcs::to_bytes(self).expect("BCS serialization failed for key type")
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SerializationError>
    where
        Self: Sized,
    {
        bcs::from_bytes(bytes)
    }
}
