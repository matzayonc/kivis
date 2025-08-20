use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{DatabaseEntry, DatabaseError, Storage};

pub struct ManifestManager<S: Storage> {
    manifest: Manifest,
    phantom: std::marker::PhantomData<S>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Manifest {
    pub tables: HashMap<String, UniquePrefix>,
}

impl<S: Storage> ManifestManager<S> {
    pub fn new(store: &S) -> Result<Self, DatabaseError<S::StoreError>> {
        let manifest = Self::load(store)?.unwrap_or_default();
        Ok(ManifestManager {
            manifest,
            phantom: std::marker::PhantomData,
        })
    }

    pub fn get_prefix<R: DatabaseEntry>(
        &mut self,
        store: &mut S,
    ) -> Result<UniquePrefix, DatabaseError<S::StoreError>> {
        // If prefix for this table is already present in the manifest, return it.
        if self.manifest.tables.contains_key(&R::SCOPE.to_string()) {
            return Ok(self
                .manifest
                .tables
                .get(&R::SCOPE.to_string())
                .unwrap()
                .clone());
        }

        // Otherwise, load the manifest from storage and check again.
        // Another instance might have added the prefix, then we can return it.
        // Should be newest before modifying in any case.
        self.manifest = Self::load(store)?.unwrap_or_default();

        // Check again after loading the manifest
        if self.manifest.tables.contains_key(&R::SCOPE.to_string()) {
            return Ok(self
                .manifest
                .tables
                .get(&R::SCOPE.to_string())
                .unwrap()
                .clone());
        }

        let next_prefix = UniquePrefix::new(self.manifest.tables.len());

        self.manifest
            .tables
            .insert(R::SCOPE.to_string(), next_prefix);

        // Update the manifest to include the new table prefix.
        self.save(store)?;
        Ok(self
            .manifest
            .tables
            .get(&R::SCOPE.to_string())
            .unwrap()
            .clone())
    }

    fn load(store: &S) -> Result<Option<Manifest>, DatabaseError<S::StoreError>> {
        // Load the manifest from the storage
        let Some(data) = store
            .get(Vec::with_capacity(0))
            .map_err(DatabaseError::Io)?
        else {
            return Ok(None);
        };
        bcs::from_bytes(&data).map_err(DatabaseError::Serialization)
    }

    fn save(&self, store: &mut S) -> Result<(), DatabaseError<S::StoreError>> {
        let data = bcs::to_bytes(&self.manifest).map_err(DatabaseError::Serialization)?;
        store
            .insert(Vec::with_capacity(0), data)
            .map_err(DatabaseError::Io)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UniquePrefix(Vec<u8>);

impl UniquePrefix {
    const SINGLE_BYTE_DISCRIMIANTOR_LIMIT: u8 = 1 << 7; // 127

    fn new(value: usize) -> UniquePrefix {
        // If there are less than 127 tables, we can describe them with just a single byte.
        if value < Self::SINGLE_BYTE_DISCRIMIANTOR_LIMIT as usize {
            // This should be the most common case by far
            return UniquePrefix(vec![value as u8]);
        }

        // Otherwise we need more bytes to encode them
        let mut value = value.to_be_bytes().to_vec();
        while value.first() == Some(&0) {
            value.remove(0);
        }

        // Prepending value with it's length doesn't change the ordering.
        // It does make sure that no value is prefix of another
        value.insert(0, value.len() as u8 & Self::SINGLE_BYTE_DISCRIMIANTOR_LIMIT);
        UniquePrefix(value)
    }

    fn to_number(iter: &mut impl Iterator<Item = u8>) -> Option<usize> {
        let short_or_len = iter.next()?;
        if short_or_len > Self::SINGLE_BYTE_DISCRIMIANTOR_LIMIT {
            let len = short_or_len & !Self::SINGLE_BYTE_DISCRIMIANTOR_LIMIT;
            iter.take(len as usize).fold(Some(0), |acc, byte| {
                acc.and_then(|v| v.checked_shl(8).and_then(|v| v.checked_add(byte as usize)))
            })
        } else {
            Some(short_or_len as usize)
        }
    }
}
