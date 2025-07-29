mod incrementable_types;
mod schema;
mod serialization;
mod storage;

use std::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};

pub use schema::*;
pub use serialization::*;
pub use storage::*;

pub type SerializationError = bcs::Error;

/// The main trait of the crate, defines a database entry that can be stored with its indexes.
pub trait DatabaseEntry: Serialize + DeserializeOwned + Debug {
    const SCOPE: u8;
    type Key: RecordKey;

    fn index_keys(&self) -> Vec<(u8, &dyn KeyBytes)> {
        vec![]
    }
}
