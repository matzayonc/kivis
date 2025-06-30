use std::{
    fmt::{Debug, Display},
    ops::Range,
};

use serde::{Serialize, de::DeserializeOwned};

pub type SerializationError = bcs::Error;

pub trait RecordKey: Serialize + DeserializeOwned + Ord + Clone + Eq {
    type Record: Recordable;
}

pub trait Recordable: Serialize + DeserializeOwned + Debug {
    const SCOPE: u8;
    type Key: RecordKey + Debug;

    fn key(&self) -> Option<Self::Key> {
        None
    }
    fn index_keys(&self, _key: Self::Key) -> Result<Vec<Vec<u8>>, SerializationError> {
        Ok(vec![])
    }
}

pub trait Incrementable: Sized {
    fn bounds() -> Option<(Self, Self)>;
    fn next_id(&self) -> Option<Self>;
}

pub trait Index: Serialize + DeserializeOwned + Debug {
    type Key: Serialize + DeserializeOwned + Ord + Clone + Eq + Debug;
    type Record: Recordable;
    const INDEX: u8;
}

pub trait Storage: Sized {
    type StoreError: Debug + Display + Eq + PartialEq;

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError>;
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError>;
    fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError>;
    fn iter_keys(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError>;
}
