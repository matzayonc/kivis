use std::{cmp::Reverse, collections::BTreeMap, fmt::Display, ops::Range};

use crate::traits::BinaryStorage;

/// A memory-based storage implementation using a [`BTreeMap`].
///
/// This storage backend keeps all data in memory and uses reverse-ordered keys
/// for efficient range queries. Implements the [`Storage`] trait to be used as a storage backend.
pub type MemoryStorage = BTreeMap<Reverse<Vec<u8>>, Vec<u8>>;

/// Error type for [`MemoryStorage`] operations.
///
/// [`MemoryStorage`] operations don't actually fail, so this is an empty error type.
#[derive(Debug, PartialEq, Eq)]
pub struct MemoryStorageError;
impl Display for MemoryStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Memory storage operations do not fail")
    }
}

impl BinaryStorage for MemoryStorage {
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
