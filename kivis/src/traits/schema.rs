#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use core::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};

use super::*;
use crate::{Database, DatabaseError};

/// A trait defining that the implementing type is a key of some record.
/// Each type can be a key of only one record type, which is defined by the [`DatabaseEntry`] trait.
pub trait RecordKey: Serialize + DeserializeOwned + Clone + Eq + UnifiableRef {
    /// The record type that this key identifies.
    type Record: DatabaseEntry;
}

/// A trait defining how a key can be extracted from a record.
/// This might be one of the fields, a composite key, a hash, random uuid or any other type of derivation.
/// It shouldn't be implemented for auto-incrementing keys.
pub trait DeriveKey {
    /// The key type that can be derived from this record.
    type Key: RecordKey;
    /// Derives the key from the record.
    fn key(c: &<Self::Key as RecordKey>::Record) -> Self::Key;
}

/// A trait describing how a key can be auto-incremented, defined for numeric types.
pub trait Incrementable: Default + Sized {
    /// The first and last valid values of the type.
    // const BOUNDS: (Self, Self);
    /// Returns the next value of the type, if applicable.
    fn next_id(&self) -> Option<Self>;
}

/// The main trait of the crate, defines a database entry that can be stored with its indexes.
#[allow(unused_variables)] // Defalt implementation may not use all variables.
pub trait DatabaseEntry: Scope + Serialize + DeserializeOwned + Debug {
    /// The primary key type for this database entry.
    type Key: RecordKey;
    const INDEX_COUNT_HINT: usize = 0;

    /// Returns the index keys for this entry.
    /// Each tuple contains the index discriminator and the key bytes.
    /// # Errors
    /// Returns an error if serializing any of the index keys fails.
    fn index_keys<U: Unifier>(&self, indexer: &mut IndexBuilder<U>) -> Result<(), U::SerError> {
        Ok(())
    }
}

pub trait Manifests<T: Scope + DatabaseEntry> {
    fn last(&mut self) -> &mut Option<T::Key>;
}

pub trait Manifest: Default {
    fn members() -> Vec<u8>;
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if loading manifests requires access to the
    /// underlying storage and that operation fails.
    fn load<S: Storage>(&mut self, db: &mut Database<S, Self>) -> Result<(), DatabaseError<S>>
    where
        Self: Sized;
}

pub trait Scope {
    /// Unique table identifier for this database entry type.
    /// Must be unique across all tables in a database instance.
    const SCOPE: u8;
    type Manifest;
}
