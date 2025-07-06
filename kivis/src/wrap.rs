use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{Index, Recordable, SerializationError, errors::InternalDatabaseError};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum Subtable {
    Main,
    MetadataSingleton,
    Index(u8),
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Wrap<R> {
    scope: u8,
    subtable: Subtable,
    pub key: R,
}

pub fn wrap<R: Recordable>(item_key: &R::Key) -> Result<Vec<u8>, SerializationError> {
    let wrapped = Wrap {
        scope: R::SCOPE,
        subtable: Subtable::Main,
        key: item_key.clone(),
    };
    bcs::to_bytes(&wrapped)
}

pub fn wrap_index<R: Recordable, T: Index + Serialize>(
    key: R::Key,
    index_key: T,
) -> Result<Vec<u8>, SerializationError> {
    let wrapped = Wrap {
        scope: R::SCOPE,
        subtable: Subtable::Index(T::INDEX),
        key: (index_key, key),
    };
    bcs::to_bytes(&wrapped)
}

pub fn unwrap_index<R: Recordable, I: Index + DeserializeOwned>(
    data: &[u8],
) -> Result<R::Key, InternalDatabaseError> {
    let wrapped: Wrap<(I, R::Key)> =
        bcs::from_bytes(data).map_err(InternalDatabaseError::Deserialization)?;

    if wrapped.scope != R::SCOPE {
        return Err(InternalDatabaseError::InvalidScope);
    }

    if matches!(
        wrapped.subtable,
        Subtable::Main | Subtable::MetadataSingleton
    ) {
        return Err(InternalDatabaseError::InvalidScope);
    }

    Ok(wrapped.key.1)
}

pub(crate) fn wrap_just_index<R: Recordable, I: Index + Serialize>(
    index_key: I,
) -> Result<Vec<u8>, SerializationError> {
    let wrapped = Wrap {
        scope: R::SCOPE,
        subtable: Subtable::Index(I::INDEX),
        key: (index_key,),
    };
    bcs::to_bytes(&wrapped)
}

pub(crate) fn encode_value<R: Recordable>(record: &R) -> Result<Vec<u8>, SerializationError> {
    bcs::to_bytes(record)
}
pub(crate) fn decode_value<R: Recordable>(data: &[u8]) -> Result<R, SerializationError> {
    bcs::from_bytes(data)
}
