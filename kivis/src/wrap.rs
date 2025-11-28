#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

use crate::{DatabaseEntry, Unifier};

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
}

/// Internal wrapper structure that combines prelude and key for storage.
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Wrap<R> {
    prelude: WrapPrelude,
    pub key: R,
}

/// Wraps a database entry key with scope and subtable information for storage.
pub(crate) fn wrap<R: DatabaseEntry, U: Unifier>(
    item_key: &R::Key,
    unifier: &U,
) -> Result<U::D, U::SerError> {
    let wrapped = Wrap {
        prelude: WrapPrelude {
            scope: R::SCOPE,
            subtable: Subtable::Main,
        },
        key: item_key.clone(),
    };
    unifier.serialize(wrapped)
}

pub(crate) fn empty_wrap<R: DatabaseEntry, U: Unifier>(
    config: &U,
) -> Result<(U::D, U::D), U::SerError> {
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

    Ok((config.serialize(start)?, config.serialize(end)?))
}
