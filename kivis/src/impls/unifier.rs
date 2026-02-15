//! Implementations of the `Unifier` trait for various serialization formats.

#[cfg(all(feature = "alloc", not(feature = "std")))]
use alloc::vec::Vec;

use bincode::{
    config::Configuration,
    error::{DecodeError, EncodeError},
    serde::{decode_from_slice, encode_to_vec},
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{BufferOverflowOr, Unifier, UnifierData};

/// Implementation of `Unifier` for bincode's `Configuration`.
///
/// This is the default serializer for standard environments.
#[cfg(any(feature = "std", feature = "alloc"))]
impl Unifier for Configuration {
    type D = Vec<u8>;
    type SerError = EncodeError;
    type DeError = DecodeError;

    fn serialize(
        &self,
        buffer: &mut Vec<u8>,
        data: impl Serialize,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        let start = buffer.len();
        let serialized = encode_to_vec(data, Self::default())?;
        buffer
            .extend_from(&serialized)
            .map_err(BufferOverflowOr::overflow)?;
        Ok((start, buffer.len()))
    }

    fn deserialize<T: DeserializeOwned>(&self, data: &Vec<u8>) -> Result<T, Self::DeError> {
        Ok(decode_from_slice(data, Self::default())?.0)
    }
}
