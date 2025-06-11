use core::fmt;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

pub use kivis_derive::Record;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub trait Recordable: Serialize + DeserializeOwned + Debug {
    const SCOPE: u8;
    type Key: Serialize + DeserializeOwned + Ord + Clone + Eq + Debug;

    fn key(&self) -> Self::Key;
    fn index_keys(&self) -> Result<Vec<Vec<u8>>, bcs::Error> {
        Ok(vec![])
    }
}

pub trait Indexed: Serialize + DeserializeOwned + Debug {
    type Key;
}

#[derive(Debug, Clone)]
pub enum DatabaseError<S: Debug + Display + Eq + PartialEq> {
    Serialization(bcs::Error),
    Deserialization(bcs::Error),
    Io(S),
}

impl<S: Debug + Display + Eq + PartialEq> fmt::Display for DatabaseError<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Serialization(ref e) => write!(f, "Serialization error: {}", e),
            Self::Deserialization(ref e) => write!(f, "Deserialization error: {}", e),
            Self::Io(ref s) => write!(f, "IO error: {}", s),
        }
    }
}

pub struct Database<S: RawStore> {
    store: S,
}

#[derive(Serialize, Deserialize, Debug)]
enum Subtable {
    Main,
    MetadataSingleton,
    Index(u8),
}

#[derive(Serialize, Deserialize, Debug)]
struct Wrap<R> {
    scope: u8,
    subtable: Subtable,
    key: R,
}

impl<S: RawStore> Database<S> {
    pub fn new(store: S) -> Self {
        Database { store }
    }

    pub fn dissolve(self) -> S {
        self.store
    }

    pub fn insert<R: Recordable>(
        &mut self,
        record: R,
    ) -> Result<(), DatabaseError<<S as RawStore>::StoreError>> {
        let wrapped = Wrap {
            scope: R::SCOPE,
            subtable: Subtable::Main,
            key: record.key(),
        };
        let key = bcs::to_bytes(&wrapped).map_err(DatabaseError::Serialization)?;

        for (index, index_key) in record.index_keys().iter().enumerate() {
            // Index keys will be double serialized, but this will safe a serialization at read.
            let wrapped_index = Wrap {
                scope: R::SCOPE,
                subtable: Subtable::Index(index as u8),
                key: (index_key, record.key()),
            };
            let index_value =
                bcs::to_bytes(&wrapped_index).map_err(DatabaseError::Serialization)?;
            self.store
                .insert(index_value, Vec::new())
                .map_err(DatabaseError::Io)?
        }

        let value = bcs::to_bytes(&record).map_err(DatabaseError::Serialization)?;
        self.store.insert(key, value).map_err(DatabaseError::Io)
    }

    pub fn get<R: Recordable>(
        &self,
        key: &R::Key,
    ) -> Result<Option<R>, DatabaseError<S::StoreError>> {
        let wrapped = Wrap {
            scope: R::SCOPE,
            subtable: Subtable::Main,
            key,
        };
        let key = bcs::to_bytes(&wrapped).map_err(DatabaseError::Serialization)?;
        let Some(value) = self.store.get(&key).map_err(DatabaseError::Io)? else {
            return Ok(None);
        };
        Ok(Some(
            bcs::from_bytes(&value).map_err(DatabaseError::Deserialization)?,
        ))
    }

    pub fn remove<R: Recordable>(
        &mut self,
        key: &R::Key,
    ) -> Result<Option<R>, DatabaseError<S::StoreError>> {
        let wrapped = Wrap {
            scope: R::SCOPE,
            subtable: Subtable::Main,
            key,
        };
        let key = bcs::to_bytes(&wrapped).map_err(DatabaseError::Serialization)?;
        let Some(value) = self.store.remove(&key).map_err(DatabaseError::Io)? else {
            return Ok(None);
        };
        Ok(Some(
            bcs::from_bytes(&value).map_err(DatabaseError::Deserialization)?,
        ))
    }
}

pub trait RawStore {
    type StoreError: Debug + Display + Eq + PartialEq;

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError>;
    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError>;
    fn remove(&mut self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError>;
}

pub trait Store<R: Recordable> {
    type SerializationError;

    fn insert(&mut self, record: R) -> Result<(), Self::SerializationError>;
    fn get(&self, key: &R::Key) -> Result<Option<R>, Self::SerializationError>;
    fn remove(&mut self, key: &R::Key) -> Result<Option<R>, Self::SerializationError>;
}

impl<R: Recordable + Clone> Store<R> for BTreeMap<R::Key, R> {
    type SerializationError = ();

    fn insert(&mut self, record: R) -> Result<(), Self::SerializationError> {
        self.insert(record.key(), record);
        Ok(())
    }

    fn get(&self, key: &R::Key) -> Result<Option<R>, Self::SerializationError> {
        Ok(self.get(key).cloned())
    }

    fn remove(&mut self, key: &R::Key) -> Result<Option<R>, Self::SerializationError> {
        Ok(self.remove(key))
    }
}

impl<R: Recordable> Store<R> for BTreeMap<Vec<u8>, Vec<u8>> {
    type SerializationError = bcs::Error;

    fn insert(&mut self, record: R) -> Result<(), Self::SerializationError> {
        let key = bcs::to_bytes(&record.key())?;
        let value = bcs::to_bytes(&record)?;
        self.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &R::Key) -> Result<Option<R>, Self::SerializationError> {
        let serialized_key = bcs::to_bytes(key)?;
        let Some(value) = self.get(&serialized_key) else {
            return Ok(None);
        };
        bcs::from_bytes(&value).map(Some)
    }

    fn remove(&mut self, key: &R::Key) -> Result<Option<R>, Self::SerializationError> {
        let key = bcs::to_bytes(key)?;
        let Some(value) = self.remove(&key) else {
            return Ok(None);
        };
        bcs::from_bytes(&value).map(Some)
    }
}
