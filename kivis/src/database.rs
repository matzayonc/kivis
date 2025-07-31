use serde::de::DeserializeOwned;

use crate::errors::DatabaseError;
use crate::traits::{DatabaseEntry, Index, Storage};
use crate::wrap::{decode_value, encode_value, wrap, Subtable, Wrap, WrapPrelude};
use crate::{DeriveKey, Incrementable, KeyBytes, RecordKey};
use std::ops::Range;

type DatabaseIteratorItem<R, S> =
    Result<<R as DatabaseEntry>::Key, DatabaseError<<S as Storage>::StoreError>>;

/// The `kivis` database type. All interactions with the database are done through this type.
pub struct Database<S: Storage> {
    store: S,
    fallback: Option<Box<dyn Storage<StoreError = S::StoreError>>>,
}

impl<S: Storage> Database<S> {
    /// Creates a new [`Database`] instance over any storage backend.
    /// One of the key features of `kivis` is that it can work with any storage backend that implements the [`Storage`] trait.
    pub fn new(store: S) -> Self {
        Database {
            store,
            fallback: None,
        }
    }

    /// Sets a fallback storage that will be used if the main storage does not contain the requested record.
    /// The current storage then becomes the cache for the fallback storage.
    pub fn set_fallback(&mut self, fallback: Box<dyn Storage<StoreError = S::StoreError>>) {
        self.fallback = Some(fallback);
    }

    /// Add a record with autoincremented key into the database, together with all related index entries.
    ///
    /// The record must implement the [`DatabaseEntry`] trait, with the key type implementing the [`RecordKey`] trait pointing back to it.
    /// The record's key must implement the [`Incrementable`] trait.
    /// For records that do not have an autoincremented key, use [`Self::insert`] instead.
    pub fn put<R: DatabaseEntry>(
        &mut self,
        record: R,
    ) -> Result<R::Key, DatabaseError<<S as Storage>::StoreError>>
    where
        R::Key: RecordKey<Record = R> + Incrementable,
    {
        let original_key = self
            .last_id::<R::Key>(R::Key::BOUNDS)?
            .next_id()
            .ok_or(DatabaseError::FailedToIncrement)?;

        let key = wrap::<R>(&original_key).map_err(DatabaseError::Serialization)?;

        self.add_index_entries(&record, &original_key)?;

        let value = encode_value(&record).map_err(DatabaseError::Serialization)?;
        self.store.insert(key, value).map_err(DatabaseError::Io)?;
        Ok(original_key)
    }

    /// Inserts a record with a derived key into the database, together with all related index entries.
    ///
    /// The record must implement the [`DatabaseEntry`] trait, with the key type implementing the [`RecordKey`] trait pointing back to it.
    /// The record's key must implement the [`DeriveKey`] trait, returning the key type.
    /// For records that don't store keys internally, use [`Self::put`] instead.
    pub fn insert<K: RecordKey<Record = R>, R>(
        &mut self,
        record: R,
    ) -> Result<K, DatabaseError<<S as Storage>::StoreError>>
    where
        R: DeriveKey<Key = K> + DatabaseEntry<Key = K>,
    {
        let original_key = R::key(&record);
        let key = wrap::<R>(&original_key).map_err(DatabaseError::Serialization)?;

        self.add_index_entries(&record, &original_key)?;

        let value = encode_value(&record).map_err(DatabaseError::Serialization)?;
        if let Some(fallback) = &mut self.fallback {
            fallback
                .insert(key.clone(), value.clone())
                .map_err(DatabaseError::Io)?;
        }
        self.store.insert(key, value).map_err(DatabaseError::Io)?;
        Ok(original_key)
    }

    fn add_index_entries<R: DatabaseEntry>(
        &mut self,
        record: &R,
        key: &R::Key,
    ) -> Result<(), DatabaseError<<S as Storage>::StoreError>>
    where
        R::Key: RecordKey<Record = R>,
    {
        for (discriminator, index_key) in record.index_keys() {
            let mut entry = WrapPrelude::new::<R>(Subtable::Index(discriminator)).to_bytes();
            entry.extend_from_slice(&index_key.to_bytes());

            // Indexes might be repeated, so we need to ensure that the key is unique.
            // TODO: Add a way to declare as unique and deduplicate by provided hash.
            let key_bytes = key.to_bytes();
            entry.extend_from_slice(&key_bytes);

            if let Some(fallback) = &mut self.fallback {
                fallback
                    .insert(entry.clone(), key_bytes.clone())
                    .map_err(DatabaseError::Io)?;
            }
            self.store
                .insert(entry, key_bytes)
                .map_err(DatabaseError::Io)?;
        }
        Ok(())
    }

    /// Retrieves a record from the database by its key.
    ///
    /// The record must implement the [`DatabaseEntry`] trait, with the key type implementing the [`RecordKey`] trait pointing back to it.
    /// If the record is not found, `None` is returned.
    pub fn get<K: RecordKey>(
        &self,
        key: &K,
    ) -> Result<Option<K::Record>, DatabaseError<S::StoreError>>
    where
        K::Record: DatabaseEntry<Key = K>,
    {
        let serialized_key = wrap::<K::Record>(key).map_err(DatabaseError::Serialization)?;
        let value =
            if let Some(value) = self.store.get(serialized_key).map_err(DatabaseError::Io)? {
                value
            } else {
                let Some(fallback) = &self.fallback else {
                    return Ok(None);
                };
                let key = wrap::<K::Record>(key).map_err(DatabaseError::Serialization)?;
                let Some(value) = fallback.get(key).map_err(DatabaseError::Io)? else {
                    return Ok(None);
                };
                value
            };
        Ok(Some(
            decode_value(&value).map_err(DatabaseError::Deserialization)?,
        ))
    }

    /// Removes a record from the database by its key and returns it.
    ///
    /// The record must implement the [`DatabaseEntry`] trait, with the key type implementing the [`RecordKey`] trait pointing back to it.
    /// If the record is not found, `None` is returned.
    /// The record's index entries are also removed.
    // TODO: Remove the index entries.
    pub fn remove<K: RecordKey>(
        &mut self,
        key: &K,
    ) -> Result<Option<K::Record>, DatabaseError<S::StoreError>>
    where
        K::Record: DatabaseEntry<Key = K>,
    {
        let key = wrap::<K::Record>(key).map_err(DatabaseError::Serialization)?;

        let value = if let Some(fallback) = &mut self.fallback {
            let fallback_value = fallback.remove(key.clone()).map_err(DatabaseError::Io)?;
            self.store.remove(key).map_err(DatabaseError::Io)?;
            fallback_value
        } else {
            self.store.remove(key).map_err(DatabaseError::Io)?
        };

        Ok(if let Some(ref value) = value {
            Some(decode_value(value).map_err(DatabaseError::Deserialization)?)
        } else {
            None
        })
    }

    /// Iterates over all keys in the database within the specified range.
    ///
    /// The range is inclusive of the start and exclusive of the end.
    /// The keys must implement the [`RecordKey`] trait, and the related [`DatabaseEntry`] must point back to it.
    pub fn iter_keys<K: RecordKey>(
        &self,
        range: Range<K>,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<K::Record, S>> + use<'_, K, S>,
        DatabaseError<S::StoreError>,
    >
    where
        K::Record: DatabaseEntry<Key = K>,
    {
        let start = wrap::<K::Record>(&range.start).map_err(DatabaseError::Serialization)?;
        let end = wrap::<K::Record>(&range.end).map_err(DatabaseError::Serialization)?;
        let raw_iter = self
            .store
            .iter_keys(start..end)
            .map_err(DatabaseError::Io)?;

        Ok(
            raw_iter.map(|elem: Result<Vec<u8>, <S as Storage>::StoreError>| {
                let value = match elem {
                    Ok(value) => value,
                    Err(e) => return Err(DatabaseError::Io(e)),
                };

                let deserialized: Wrap<K> = match bcs::from_bytes(&value) {
                    Ok(deserialized) => deserialized,
                    Err(e) => return Err(DatabaseError::Deserialization(e)),
                };

                Ok(deserialized.key)
            }),
        )
    }

    /// Iterates over all index entries in the database within the specified range and returns their primary keys.
    ///
    /// The range is inclusive of the start and exclusive of the end.
    /// The index must implement the [`Index`] trait.
    /// The returned iterator yields items of type `Result<Index::Record, DatabaseError<S::StoreError>>`.
    pub fn iter_by_index<I: Index>(
        &mut self,
        range: Range<I>,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<I::Record, S>> + use<'_, I, S>,
        DatabaseError<S::StoreError>,
    > {
        let index_prelude = WrapPrelude::new::<I::Record>(Subtable::Index(I::INDEX));
        let mut start = index_prelude.to_bytes();
        let mut end = start.clone();
        start.extend(range.start.to_bytes());
        end.extend(range.end.to_bytes());

        let raw_iter = self
            .store
            .iter_keys(start..end)
            .map_err(DatabaseError::Io)?;

        Ok(raw_iter.map(|elem| self.process_iter_result(elem)))
    }

    /// Consumes the database and returns the underlying storage.
    pub fn dissolve(self) -> S {
        self.store
    }

    /// Helper function to get the last ID in a given range, used for autoincrementing keys.
    fn last_id<K: RecordKey>(&self, bounds: (K, K)) -> Result<K, DatabaseError<S::StoreError>>
    where
        K::Record: DatabaseEntry<Key = K>,
    {
        let (start, end) = bounds;
        let range = if start < end {
            start.clone()..end
        } else {
            end..start.clone()
        };
        let mut first = self.iter_keys::<K>(range)?;
        Ok(first.next().transpose()?.unwrap_or(start))
    }

    /// Helper function to process iterator results and get deserialized values
    fn process_iter_result<T: DeserializeOwned>(
        &self,
        result: Result<Vec<u8>, S::StoreError>,
    ) -> Result<T, DatabaseError<S::StoreError>> {
        let key = result.map_err(DatabaseError::Io)?;
        let value: Vec<u8> = match self.store.get(key) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return Err(DatabaseError::Internal(
                    crate::InternalDatabaseError::MissingIndexEntry,
                ));
            }
            Err(e) => return Err(DatabaseError::Io(e)),
        };

        bcs::from_bytes(&value).map_err(DatabaseError::Deserialization)
    }
}
