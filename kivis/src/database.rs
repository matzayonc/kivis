use bincode::config::Configuration;
use bincode::serde::decode_from_slice;
use serde::de::DeserializeOwned;

use crate::errors::DatabaseError;
use crate::traits::{DatabaseEntry, Index, Storage};
use crate::transaction::DatabaseTransaction;
use crate::wrap::{decode_value, empty_wrap, wrap, Subtable, Wrap, WrapPrelude};
use crate::{DeriveKey, Incrementable, Manifest, Manifests, RecordKey};
use std::ops::Range;

type DatabaseIteratorItem<R, S> =
    Result<<R as DatabaseEntry>::Key, DatabaseError<<S as Storage>::StoreError>>;

/// The `kivis` database type. All interactions with the database are done through this type.
pub struct Database<S: Storage, M: Manifest> {
    pub(crate) store: S,
    fallback: Option<Box<dyn Storage<StoreError = S::StoreError>>>,
    pub(crate) manifest: M,
    serialization_config: Configuration,
}

impl<S: Default + Storage, M: Manifest> Default for Database<S, M> {
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<S: Storage, M: Manifest> Database<S, M> {
    /// Creates a new [`Database`] instance over any storage backend.
    /// One of the key features of `kivis` is that it can work with any storage backend that implements the [`Storage`] trait.
    pub fn new(store: S) -> Self {
        let mut db = Database {
            store,
            fallback: None,
            manifest: M::default(),
            serialization_config: Configuration::default(),
        };
        let mut manifest = M::default();
        manifest.load(&mut db).unwrap();
        db.manifest = manifest;
        db
    }

    pub fn with_serialization_config(&mut self, config: Configuration) {
        self.serialization_config = config;
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
        R::Key: RecordKey<Record = R> + Incrementable + Ord,
        M: Manifests<R>,
    {
        let mut transaction = DatabaseTransaction::new(self);
        let inserted_key = transaction.put(record, self)?;
        self.commit(transaction)?;
        Ok(inserted_key)
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
        M: Manifests<R>,
    {
        let mut transaction = DatabaseTransaction::new(self);
        let inserted_key = transaction.insert::<K, R>(record)?;
        self.commit(transaction)?;
        Ok(inserted_key)
    }

    pub fn create_transaction(&self) -> DatabaseTransaction<M> {
        DatabaseTransaction::new(self)
    }

    pub fn commit(
        &mut self,
        transaction: DatabaseTransaction<M>,
    ) -> Result<(), DatabaseError<S::StoreError>> {
        let (writes, deletes) = transaction.consume();
        for (key, value) in writes {
            if let Some(fallback) = &mut self.fallback {
                fallback
                    .insert(key.clone(), value.clone())
                    .map_err(DatabaseError::Io)?;
            }
            self.store.insert(key, value).map_err(DatabaseError::Io)?;
        }

        for key in deletes {
            if let Some(fallback) = &mut self.fallback {
                fallback.remove(key.clone()).map_err(DatabaseError::Io)?;
            }
            self.store.remove(key).map_err(DatabaseError::Io)?;
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
        M: Manifests<K::Record>,
    {
        let serialized_key = wrap::<K::Record>(key, self.serialization_config)
            .map_err(DatabaseError::Serialization)?;
        let value =
            if let Some(value) = self.store.get(serialized_key).map_err(DatabaseError::Io)? {
                value
            } else {
                let Some(fallback) = &self.fallback else {
                    return Ok(None);
                };
                let key = wrap::<K::Record>(key, self.serialization_config)
                    .map_err(DatabaseError::Serialization)?;
                let Some(value) = fallback.get(key).map_err(DatabaseError::Io)? else {
                    return Ok(None);
                };
                value
            };
        Ok(Some(
            decode_value(&value, self.serialization_config)
                .map_err(DatabaseError::Deserialization)?,
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
        M: Manifests<K::Record>,
    {
        let key = wrap::<K::Record>(key, self.serialization_config)
            .map_err(DatabaseError::Serialization)?;

        let value = if let Some(fallback) = &mut self.fallback {
            let fallback_value = fallback.remove(key.clone()).map_err(DatabaseError::Io)?;
            self.store.remove(key).map_err(DatabaseError::Io)?;
            fallback_value
        } else {
            self.store.remove(key).map_err(DatabaseError::Io)?
        };

        Ok(if let Some(ref value) = value {
            Some(
                decode_value(value, self.serialization_config)
                    .map_err(DatabaseError::Deserialization)?,
            )
        } else {
            None
        })
    }

    /// Iterates over all keys in the database within the specified range.
    ///
    /// The range is inclusive of the start and exclusive of the end.
    /// The keys must implement the [`RecordKey`] trait, and the related [`DatabaseEntry`] must point back to it.
    pub fn iter_keys<K: RecordKey + Ord>(
        &self,
        range: Range<K>,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<K::Record, S>> + use<'_, K, S, M>,
        DatabaseError<S::StoreError>,
    >
    where
        K::Record: DatabaseEntry<Key = K>,
        M: Manifests<K::Record>,
    {
        let start = wrap::<K::Record>(&range.start, self.serialization_config)
            .map_err(DatabaseError::Serialization)?;
        let end = wrap::<K::Record>(&range.end, self.serialization_config)
            .map_err(DatabaseError::Serialization)?;
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

                let deserialized: Wrap<K> =
                    match decode_from_slice(&value, self.serialization_config) {
                        Ok((deserialized, _)) => deserialized,
                        Err(e) => return Err(DatabaseError::Deserialization(e)),
                    };

                Ok(deserialized.key)
            }),
        )
    }

    pub fn iter_all_keys<K: RecordKey + Ord>(
        &self,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<K::Record, S>> + use<'_, K, S, M>,
        DatabaseError<S::StoreError>,
    >
    where
        K::Record: DatabaseEntry<Key = K>,
        M: Manifests<K::Record>,
    {
        let (start, end) = empty_wrap::<K::Record>(self.serialization_config)
            .map_err(DatabaseError::Serialization)?;
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

                let deserialized: Wrap<K> =
                    match decode_from_slice(&value, self.serialization_config) {
                        Ok((deserialized, _)) => deserialized,
                        Err(e) => return Err(DatabaseError::Deserialization(e)),
                    };

                Ok(deserialized.key)
            }),
        )
    }

    pub fn last_id<K: RecordKey + Ord + Default>(&self) -> Result<K, DatabaseError<S::StoreError>>
    where
        K::Record: DatabaseEntry<Key = K>,
        M: Manifests<K::Record>,
    {
        let mut first = self.iter_all_keys::<K>()?;

        Ok(first.next().transpose()?.unwrap_or_default())
    }

    /// Iterates over all index entries in the database within the specified range and returns their primary keys.
    ///
    /// The range is inclusive of the start and exclusive of the end.
    /// The index must implement the [`Index`] trait.
    /// The returned iterator yields items of type `Result<Index::Record, DatabaseError<S::StoreError>>`.
    pub fn iter_by_index<I: Index + Ord>(
        &mut self,
        range: Range<I>,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<I::Record, S>> + use<'_, I, S, M>,
        DatabaseError<S::StoreError>,
    > {
        let index_prelude = WrapPrelude::new::<I::Record>(Subtable::Index(I::INDEX));
        let mut start = index_prelude.to_bytes(self.serialization_config);
        let mut end = start.clone();
        start.extend(range.start.to_bytes(self.serialization_config));
        end.extend(range.end.to_bytes(self.serialization_config));
        let raw_iter = self
            .store
            .iter_keys(start..end)
            .map_err(DatabaseError::Io)?;

        Ok(raw_iter.map(|elem| self.process_iter_result(elem)))
    }

    /// Iterates over all index entries in the database that exactly match the given index key and returns their primary keys.
    ///
    /// This function outputs multiple results since multiple records can share the same index key.
    /// The index must implement the [`Index`] trait.
    /// The returned iterator yields items of type `Result<Index::Record, DatabaseError<S::StoreError>>`.
    pub fn iter_by_index_exact<I: Index + Ord>(
        &mut self,
        index_key: &I,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<I::Record, S>> + use<'_, I, S, M>,
        DatabaseError<S::StoreError>,
    > {
        let index_prelude = WrapPrelude::new::<I::Record>(Subtable::Index(I::INDEX));
        let mut start = index_prelude.to_bytes(self.serialization_config);
        let mut end = index_prelude.to_bytes(self.serialization_config);

        let start_bytes = index_key.to_bytes(self.serialization_config);
        let end_bytes = {
            let mut end_bytes = start_bytes.clone();
            bytes_next(&mut end_bytes);
            end_bytes
        };
        start.extend(start_bytes);
        end.extend(end_bytes);

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

    /// Returns the current [`Configuration`] used by the database.
    pub fn serialization_config(&self) -> Configuration {
        self.serialization_config
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

        decode_from_slice(&value, self.serialization_config)
            .map_err(DatabaseError::Deserialization)
            .map(|(v, _)| v)
    }
}

fn bytes_next(bytes: &mut Vec<u8>) {
    for i in (0..bytes.len()).rev() {
        // Add one if possible
        if bytes[i] < 255 {
            bytes[i] += 1;
            return;
        } else {
            // Otherwise, set to zero and carry over
            bytes[i] = 0;
        }
    }

    // If all bytes were 255, we need to add a new byte
    bytes.push(0);
}
