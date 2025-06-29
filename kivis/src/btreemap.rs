use std::{cmp::Reverse, collections::BTreeMap, fmt::Display, ops::Range};

use crate::traits::Storage;

pub type MemoryStorage = BTreeMap<Reverse<Vec<u8>>, Vec<u8>>;

#[derive(Debug, PartialEq, Eq)]
pub struct MemoryStorageError;
impl Display for MemoryStorageError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl Storage for MemoryStorage {
    type StoreError = MemoryStorageError;

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError> {
        self.insert(Reverse(key), value);
        Ok(())
    }

    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.get(&Reverse(key)).cloned())
    }

    fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.remove(&Reverse(key)))
    }

    fn iter_keys(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError> {
        let reverse_range = Reverse(range.end)..Reverse(range.start);

        let iter = self.range(reverse_range);
        Ok(iter.map(|(k, _v)| Ok(k.0.clone())))
    }
}
