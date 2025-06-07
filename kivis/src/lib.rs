use std::{collections::BTreeMap, fmt::Debug};

pub use kivis_derive::Record;
use serde::{Serialize, de::DeserializeOwned};

pub trait Recordable: Serialize + DeserializeOwned + Debug {
    const SCOPE: u8;
    type Key: Serialize + DeserializeOwned + Ord + Clone + Eq + Debug;

    fn key(&self) -> Self::Key;
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
