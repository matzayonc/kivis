use std::{
    fmt::{Debug, Display},
    ops::Range,
};

use serde::{Serialize, de::DeserializeOwned};

pub type SerializationError = bcs::Error;

pub trait DefineRecord: Serialize + DeserializeOwned + Ord + Clone + Eq {
    type Record: Recordable;
}

pub trait HasKey {
    type Key: DefineRecord;
    fn key(c: &<Self::Key as DefineRecord>::Record) -> Self::Key;
}

pub trait Recordable: Serialize + DeserializeOwned + Debug {
    const SCOPE: u8;
    type Key: DefineRecord;

    fn index_keys(&self) -> Vec<(u8, &dyn KeyBytes)> {
        vec![]
    }
}

/// Needed to for dyn compatibility as well as custom serializations.
pub trait KeyBytes {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self, SerializationError>
    where
        Self: Sized;
}
impl<T: Serialize + DeserializeOwned> KeyBytes for T {
    fn to_bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).expect("Failed to serialize key")
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SerializationError>
    where
        Self: Sized,
    {
        bcs::from_bytes(bytes)
    }
}

/// A trait describing how a key can be auto-incremented, defined for numeric types.
pub trait Incrementable: Sized {
    /// Returns the bounds of the type, if applicable.
    fn bounds() -> (Self, Self);
    /// Returns the next value of the type, if applicable.
    fn next_id(&self) -> Option<Self>;
}

/// A trait defining an index in the database.
///
/// An index is a way to efficiently look up records in the database by a specific key.
/// It defines a table, primary key type, and an unique prefix for the index.
pub trait Index: KeyBytes + Debug {
    type Key: KeyBytes + DeserializeOwned + Ord + Clone + Eq + Debug;
    type Record: Recordable;
    const INDEX: u8;
}

/// A trait defining a storage backend for the database.
///
/// The storage backend is responsible for storing and retrieving records and their associated indexes.
/// It defines methods for inserting, getting, removing, and iterating over keys in the storage.
/// All storage operations are defined over `Vec<u8>` keys and values.
pub trait Storage: Sized {
    type StoreError: Debug + Display + Eq + PartialEq;

    /// Should insert the given key-value pair into the storage.
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError>;
    /// Should retrieve the value associated with the given key from the storage.
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError>;
    /// Should remove the value associated with the given key from the storage.
    fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError>;
    /// Should iterate over the keys in the storage that are in range.
    fn iter_keys(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError>;
}

impl Incrementable for u128 {
    fn bounds() -> (Self, Self) {
        (0, u128::MAX)
    }

    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u64 {
    fn bounds() -> (Self, Self) {
        (0, u64::MAX)
    }

    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u32 {
    fn bounds() -> (Self, Self) {
        (0, u32::MAX)
    }

    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u16 {
    fn bounds() -> (Self, Self) {
        (0, u16::MAX)
    }

    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u8 {
    fn bounds() -> (Self, Self) {
        (0, u8::MAX)
    }

    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}
