use std::fmt::Debug;

use bincode::{
    config::Configuration,
    error::{DecodeError, EncodeError},
    serde::{decode_from_slice, encode_to_vec},
};
use kivis::Unifier;

#[derive(Clone, Copy, Default)]
pub struct BincodeSerializer(Configuration);
impl Unifier for BincodeSerializer {
    type D = Vec<u8>;
    type SerError = EncodeError;
    type DeError = DecodeError;

    fn serialize_key(&self, data: impl serde::Serialize) -> Result<Self::D, Self::SerError> {
        encode_to_vec(data, self.0)
    }

    fn deserialize_key<T: serde::de::DeserializeOwned>(
        &self,
        data: &Self::D,
    ) -> Result<T, Self::DeError> {
        Ok(decode_from_slice(data, self.0)?.0)
    }
}
impl Debug for BincodeSerializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Serializer")
    }
}
