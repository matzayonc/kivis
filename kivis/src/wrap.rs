use serde::{Deserialize, Serialize};

use crate::{DatabaseEntry, SerializationError};

/// Internal enum representing different subtables within a database scope.
#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum Subtable {
    /// Main data storage subtable.
    Main,
    /// Index subtable with discriminator.
    Index(u8),
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
    pub fn to_bytes(&self) -> Vec<u8> {
        // This should never fail as WrapPrelude is a simple, well-defined structure
        bcs::to_bytes(self).expect("BCS serialization failed for WrapPrelude")
    }
}

/// Internal wrapper structure that combines prelude and key for storage.
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Wrap<R> {
    prelude: WrapPrelude,
    pub key: R,
}

/// Wraps a database entry key with scope and subtable information for storage.
pub(crate) fn wrap<R: DatabaseEntry>(item_key: &R::Key) -> Result<Vec<u8>, SerializationError> {
    let wrapped = Wrap {
        prelude: WrapPrelude {
            scope: R::SCOPE,
            subtable: Subtable::Main,
        },
        key: item_key.clone(),
    };
    bcs::to_bytes(&wrapped)
}

/// Encodes a database entry record to bytes for storage.
pub(crate) fn encode_value<R: DatabaseEntry>(record: &R) -> Result<Vec<u8>, SerializationError> {
    bcs::to_bytes(record)
}

/// Decodes bytes back to a database entry record.
pub(crate) fn decode_value<R: DatabaseEntry>(data: &[u8]) -> Result<R, SerializationError> {
    bcs::from_bytes(data)
}
