#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use bincode::{
    config::{Config, Configuration},
    error::EncodeError,
    serde::{decode_from_slice, encode_to_vec},
};
use serde::{Deserialize, Serialize};

use crate::{DatabaseEntry, DeserializationError, SerializationError};

/// Internal enum representing different subtables within a database scope.
#[derive(Debug)]
pub(crate) enum Subtable {
    /// Main data storage subtable.
    Main,
    Reserved,
    /// Index subtable with discriminator.
    Index(u8),
}

impl Serialize for Subtable {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value = match self {
            Subtable::Main => 0u8,
            Subtable::Reserved => 1u8,
            Subtable::Index(discriminator) => {
                // Reserve 1, start Index at 2
                discriminator.checked_add(2).ok_or_else(|| {
                    serde::ser::Error::custom("Index discriminator overflow when adding 2")
                })?
            }
        };
        serializer.serialize_u8(value)
    }
}

impl<'de> Deserialize<'de> for Subtable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;
        match value {
            0 => Ok(Subtable::Main),
            1 => Err(serde::de::Error::custom("Reserved subtable value 1")),
            n => Ok(Subtable::Index(n.checked_sub(2).ok_or_else(|| {
                serde::de::Error::custom("Index discriminator underflow when subtracting 2")
            })?)),
        }
    }
}

/// Internal structure for key prefixing to separate different scopes and subtables.
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct WrapPrelude {
    scope: u8,
    subtable: Subtable,
}

impl WrapPrelude {
    /// Creates a new wrap prelude for the given database entry type and subtable.
    pub fn new<R: DatabaseEntry>(subtable: Subtable) -> Self {
        WrapPrelude {
            scope: R::SCOPE,
            subtable,
        }
    }

    /// Converts the wrap prelude to bytes for storage key prefixing.
    pub fn to_bytes(&self, config: Configuration) -> Result<Vec<u8>, EncodeError> {
        encode_to_vec(self, config)
    }
}

/// Internal wrapper structure that combines prelude and key for storage.
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Wrap<R> {
    prelude: WrapPrelude,
    pub key: R,
}

/// Wraps a database entry key with scope and subtable information for storage.
pub(crate) fn wrap<R: DatabaseEntry>(
    item_key: &R::Key,
    config: Configuration,
) -> Result<Wrap<R::Key>, SerializationError> {
    let wrapped = Wrap {
        prelude: WrapPrelude {
            scope: R::SCOPE,
            subtable: Subtable::Main,
        },
        key: item_key.clone(),
    };
    // encode_to_vec(wrapped, config)
    Ok(wrapped)
}

pub(crate) fn empty_wrap<R: DatabaseEntry>(
    config: Configuration,
) -> Result<(Vec<u8>, Vec<u8>), SerializationError> {
    let start = Wrap {
        prelude: WrapPrelude {
            scope: R::SCOPE,
            subtable: Subtable::Main,
        },
        key: (),
    };

    let end = Wrap {
        prelude: WrapPrelude {
            scope: R::SCOPE,
            subtable: Subtable::Reserved,
        },
        key: (),
    };

    Ok((encode_to_vec(start, config)?, encode_to_vec(end, config)?))
}

/// Encodes a database entry record to bytes for storage.
pub(crate) fn encode_value<R: DatabaseEntry>(
    record: &R,
    config: impl Config,
) -> Result<Vec<u8>, SerializationError> {
    encode_to_vec(record, config)
}

/// Decodes bytes back to a database entry record.
pub(crate) fn decode_value<R: DatabaseEntry>(
    data: &[u8],
    config: impl Config,
) -> Result<R, DeserializationError> {
    decode_from_slice(data, config).map(|(record, _)| record)
}
