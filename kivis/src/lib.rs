mod wrap;

use core::fmt;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    ops::{Range, RangeBounds},
};

pub use kivis_derive::Record;
use serde::{Serialize, de::DeserializeOwned};
pub use wrap::{wrap, wrap_index};

use crate::wrap::{Wrap, decode_value, encode_value, wrap_just_index};

pub type SerializationError = bcs::Error;

pub trait Recordable: Serialize + DeserializeOwned + Debug {
    const SCOPE: u8;
    type Key: Serialize + DeserializeOwned + Ord + Clone + Eq + Debug;

    fn key(&self) -> Self::Key;
    fn index_keys(&self) -> Result<Vec<Vec<u8>>, SerializationError> {
        Ok(vec![])
    }
}

type DatabaseIteratorItem<R, S> =
    Result<<R as Recordable>::Key, DatabaseError<<S as RawStore>::StoreError>>;

pub trait Indexed: Serialize + DeserializeOwned + Debug {
    type Key: Serialize + DeserializeOwned + Ord + Clone + Eq + Debug;
    type Record: Recordable;
    const INDEX: u8;
}

#[derive(Debug)]
pub enum DatabaseError<S: Debug + Display + Eq + PartialEq> {
    Serialization(SerializationError),
    Deserialization(SerializationError),
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
        let key = wrap::<R>(&record.key()).map_err(DatabaseError::Serialization)?;

        for index_value in record.index_keys().map_err(DatabaseError::Serialization)? {
            self.store
                .insert(index_value, Vec::new())
                .map_err(DatabaseError::Io)?
        }

        let value = encode_value(&record).map_err(DatabaseError::Serialization)?;
        self.store.insert(key, value).map_err(DatabaseError::Io)
    }

    pub fn get<R: Recordable>(
        &self,
        key: &R::Key,
    ) -> Result<Option<R>, DatabaseError<S::StoreError>> {
        let key = wrap::<R>(key).map_err(DatabaseError::Serialization)?;
        let Some(value) = self.store.get(&key).map_err(DatabaseError::Io)? else {
            return Ok(None);
        };
        Ok(Some(
            decode_value(&value).map_err(DatabaseError::Deserialization)?,
        ))
    }

    pub fn remove<R: Recordable>(
        &mut self,
        key: &R::Key,
    ) -> Result<Option<R>, DatabaseError<S::StoreError>> {
        let key = wrap::<R>(key).map_err(DatabaseError::Serialization)?;
        let Some(value) = self.store.remove(&key).map_err(DatabaseError::Io)? else {
            return Ok(None);
        };
        Ok(Some(
            decode_value(&value).map_err(DatabaseError::Deserialization)?,
        ))
    }

    pub fn iter_keys<R: Recordable>(
        &mut self,
        range: Range<&R::Key>,
    ) -> Result<impl Iterator<Item = DatabaseIteratorItem<R, S>>, DatabaseError<S::StoreError>>
    {
        let start = wrap::<R>(range.start).map_err(DatabaseError::Serialization)?;
        let end = wrap::<R>(range.end).map_err(DatabaseError::Serialization)?;
        let raw_iter = self
            .store
            .iter_keys(start..end)
            .map_err(DatabaseError::Io)?;

        Ok(
            raw_iter.map(|elem: Result<Vec<u8>, <S as RawStore>::StoreError>| {
                let value = match elem {
                    Ok(value) => value,
                    Err(e) => return Err(DatabaseError::Io(e)),
                };

                let deserialized: Wrap<R::Key> = match bcs::from_bytes(&value) {
                    Ok(deserialized) => deserialized,
                    Err(e) => return Err(DatabaseError::Deserialization(e)),
                };

                Ok(deserialized.key)
            }),
        )
    }

    pub fn iter_by_index<I: Indexed>(
        &mut self,
        range: Range<I>,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<I::Record, S>>,
        DatabaseError<S::StoreError>,
    > {
        let start =
            wrap_just_index::<I::Record, I>(range.start).map_err(DatabaseError::Serialization)?;
        let end =
            wrap_just_index::<I::Record, I>(range.end).map_err(DatabaseError::Serialization)?;

        let raw_iter = self
            .store
            .iter_keys(start..end)
            .map_err(DatabaseError::Io)?;

        Ok(
            raw_iter.map(|elem: Result<Vec<u8>, <S as RawStore>::StoreError>| {
                let value = match elem {
                    Ok(value) => value,
                    Err(e) => return Err(DatabaseError::Io(e)),
                };

                let deserialized: Wrap<(Vec<u8>, <I::Record as Recordable>::Key)> =
                    match bcs::from_bytes(&value) {
                        Ok(deserialized) => deserialized,
                        Err(e) => return Err(DatabaseError::Deserialization(e)),
                    };

                Ok(deserialized.key.1)
            }),
        )
    }
}

pub trait RawStore: Sized {
    type StoreError: Debug + Display + Eq + PartialEq;

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError>;
    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError>;
    fn remove(&mut self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError>;
    fn iter_keys(
        &mut self,
        range: impl RangeBounds<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError>;
}

pub trait Store<R: Recordable> {
    type SerializationError;

    fn insert(&mut self, record: R) -> Result<(), Self::SerializationError>;
    fn get(&self, key: &R::Key) -> Result<Option<R>, Self::SerializationError>;
    fn remove(&mut self, key: &R::Key) -> Result<Option<R>, Self::SerializationError>;
    fn iter_keys(&mut self, range: Range<&R::Key>) -> Result<Option<R>, Self::SerializationError>;
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

    fn iter_keys(&mut self, range: Range<&R::Key>) -> Result<Option<R>, Self::SerializationError> {
        let mut iter = self.range(range);
        if let Some((_, record)) = iter.next() {
            Ok(Some(record.clone()))
        } else {
            Ok(None)
        }
    }
}
