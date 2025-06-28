use std::{collections::BTreeMap, fmt::Display};

use crate::traits::Storage;

#[derive(Debug, PartialEq, Eq)]
pub struct NoError;
impl Display for NoError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl Storage for BTreeMap<Vec<u8>, Vec<u8>> {
    type StoreError = NoError;

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError> {
        self.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.get(key).cloned())
    }

    fn remove(&mut self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.remove(key))
    }

    fn iter_keys(
        &mut self,
        range: impl std::ops::RangeBounds<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError> {
        let iter = self.range(range);
        Ok(iter.map(|(k, _v)| Ok(k.clone())))
    }
}
