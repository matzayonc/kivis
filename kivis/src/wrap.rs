use serde::{Deserialize, Serialize};

use crate::{Index, Recordable, SerializationError};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum Subtable {
    Main,
    MetadataSingleton,
    Index(u8),
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct WrapPrelude {
    scope: u8,
    subtable: Subtable,
}

impl WrapPrelude {
    pub fn new<R: Recordable>(subtable: Subtable) -> Self {
        WrapPrelude {
            scope: R::SCOPE,
            subtable,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Wrap<R> {
    prelude: WrapPrelude,
    pub key: R,
}

pub fn wrap<R: Recordable>(item_key: &R::Key) -> Result<Vec<u8>, SerializationError> {
    let wrapped = Wrap {
        prelude: WrapPrelude {
            scope: R::SCOPE,
            subtable: Subtable::Main,
        },
        key: item_key.clone(),
    };
    bcs::to_bytes(&wrapped)
}

pub fn wrap_index<R: Recordable, T: Index + Serialize>(
    key: R::Key,
    index_key: T,
) -> Result<Vec<u8>, SerializationError> {
    let wrapped = Wrap {
        prelude: WrapPrelude {
            scope: R::SCOPE,
            subtable: Subtable::Index(T::INDEX),
        },
        key: (index_key, key),
    };
    bcs::to_bytes(&wrapped)
}

pub(crate) fn encode_value<R: Recordable>(record: &R) -> Result<Vec<u8>, SerializationError> {
    bcs::to_bytes(record)
}
pub(crate) fn decode_value<R: Recordable>(data: &[u8]) -> Result<R, SerializationError> {
    bcs::from_bytes(data)
}
