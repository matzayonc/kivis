use super::*;

/// Needed to for dyn compatibility as well as custom serializations.
pub trait KeyBytes {
    /// Converts the key to bytes for storage.
    fn to_bytes(&self) -> Vec<u8>;
    /// Reconstructs the key from bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self, SerializationError>
    where
        Self: Sized;
}
impl<T: Serialize + DeserializeOwned> KeyBytes for T {
    fn to_bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).expect("Failed to serialize key")
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SerializationError>
    where
        Self: Sized,
    {
        bcs::from_bytes(bytes)
    }
}
