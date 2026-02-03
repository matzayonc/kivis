use core::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    BufferOverflowOr, Database, DatabaseError, Storage, Unifiable, UnifiableRef, Unifier,
    UnifierData,
};

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

/// A trait defining an index in the database.
///
/// An index is a way to efficiently look up records in the database by a specific key.
/// It defines a table, primary key type, and an unique prefix for the index.
pub trait Index: Unifiable + Debug {
    /// The key type used by this index.
    type Key: Unifiable + Clone + Eq + Debug;
    /// The record type that this index applies to.
    type Record: DatabaseEntry;
    /// Unique identifier for this index within the record type.
    const INDEX: u8;
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
    const INDEX_COUNT_HINT: u8 = 0;

    /// Serializes a specific index into the provided buffer.
    /// # Errors
    /// Returns an error if serializing the index fails.
    fn index_key<U: Unifier>(
        &self,
        buffer: &mut <U::K as UnifierData>::Buffer,
        discriminator: u8,
        serializer: &U,
    ) -> Result<(), BufferOverflowOr<U::SerError>> {
        Ok(())
    }
}

pub trait Manifests<T: Scope + DatabaseEntry> {
    fn last(&mut self) -> &mut Option<T::Key>;
}

pub trait Manifest: Default {
    fn members() -> &'static [u8];
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
