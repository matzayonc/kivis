use super::*;

/// Needed to for dyn compatibility as well as custom serializations.
pub trait KeyBytes {
    fn to_bytes(&self) -> Vec<u8>;
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
