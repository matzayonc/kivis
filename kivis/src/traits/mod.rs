mod incrementable_types;
mod schema;
mod serialization;
mod storage;

use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

pub use schema::*;
pub use serialization::*;
pub use storage::*;

/// Error type for serialization operations, re-exported from the BCS crate.
pub type SerializationError = bcs::Error;

/// The main trait of the crate, defines a database entry that can be stored with its indexes.
pub trait DatabaseEntry: Serialize + DeserializeOwned + Debug {
    /// Unique table identifier for this database entry type.
    /// Must be unique across all tables in a database instance.
    const SCOPE: u8;

    /// The primary key type for this database entry.
    type Key: RecordKey;

    /// Returns the index keys for this entry.
    /// Each tuple contains the index discriminator and the key bytes.
    fn index_keys(&self) -> Vec<(u8, &dyn KeyBytes)> {
        vec![]
    }
}
