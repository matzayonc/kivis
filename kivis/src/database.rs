use serde::de::DeserializeOwned;

use crate::errors::DatabaseError;
use crate::traits::{DatabaseEntry, Index, Storage};
use crate::transaction::DatabaseTransaction;
use crate::wrap::{empty_wrap, wrap, Subtable, Wrap, WrapPrelude};
use crate::{
    DeriveKey, Incrementable, Indexer, Manifest, Manifests, RecordKey, SimpleIndexer, Unifier,
    UnifierData,
};
use core::ops::Range;

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

type DatabaseIteratorItem<R, S> = Result<<R as DatabaseEntry>::Key, DatabaseError<S>>;

/// The `kivis` database type. All interactions with the database are done through this type.
pub struct Database<S: Storage, M: Manifest> {
    pub(crate) store: S,
    fallback: Option<Box<dyn Storage<StoreError = S::StoreError, Serializer = S::Serializer>>>,
    pub(crate) manifest: M,
    pub(crate) serialization_config: <S as Storage>::Serializer,
}

impl<S: Storage, M: Manifest> Database<S, M>
where
    S::Serializer: Unifier + Copy,
    SimpleIndexer<S::Serializer>: Indexer<Error = <S::Serializer as Unifier>::SerError>,
{
    /// Creates a new [`Database`] instance over any storage backend.
    /// One of the key features of `kivis` is that it can work with any storage backend that implements the [`Storage`] trait.
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if the manifest fails to load during initialization.
    pub fn new(store: S) -> Result<Self, DatabaseError<S>> {
        let mut db = Database {
            store,
            fallback: None,
            manifest: M::default(),
            serialization_config: S::Serializer::default(),
        };
        let mut manifest = M::default();
        manifest.load(&mut db)?;
        db.manifest = manifest;
        Ok(db)
    }

    pub fn with_serialization_config(&mut self, config: <S as Storage>::Serializer) {
        self.serialization_config = config;
    }

    /// Sets a fallback storage that will be used if the main storage does not contain the requested record.
    /// The current storage then becomes the cache for the fallback storage.
    pub fn set_fallback(
        &mut self,
        fallback: Box<dyn Storage<Serializer = S::Serializer, StoreError = S::StoreError>>,
    ) {
        self.fallback = Some(fallback);
    }

    /// Add a record with autoincremented key into the database, together with all related index entries.
    ///
    /// The record must implement the [`DatabaseEntry`] trait, with the key type implementing the [`RecordKey`] trait pointing back to it.
    /// The record's key must implement the [`Incrementable`] trait.
    /// For records that do not have an autoincremented key, use [`Self::insert`] instead.
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if serializing or writing the record fails.
    pub fn put<R: DatabaseEntry>(&mut self, record: &R) -> Result<R::Key, DatabaseError<S>>
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
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if serializing or writing the record fails.
    pub fn insert<K: RecordKey<Record = R>, R>(&mut self, record: &R) -> Result<K, DatabaseError<S>>
    where
        R: DeriveKey<Key = K> + DatabaseEntry<Key = K>,
        M: Manifests<R>,
    {
        let mut transaction = DatabaseTransaction::new(self);
        let inserted_key = transaction
            .insert::<K, R>(record)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        self.commit(transaction)?;
        Ok(inserted_key)
    }

    pub fn create_transaction(&self) -> DatabaseTransaction<M, S::Serializer> {
        DatabaseTransaction::new(self)
    }

    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if writing to the underlying storage fails.
    pub fn commit(
        &mut self,
        transaction: DatabaseTransaction<M, S::Serializer>,
    ) -> Result<(), DatabaseError<S>> {
        let (writes, deletes) = transaction.consume();
        for (key, value) in writes {
            if let Some(fallback) = &mut self.fallback {
                fallback
                    .insert(key.clone(), value.clone())
                    .map_err(DatabaseError::Storage)?;
            }
            self.store
                .insert(key, value)
                .map_err(DatabaseError::Storage)?;
        }

        for key in deletes {
            if let Some(fallback) = &mut self.fallback {
                fallback
                    .remove(key.clone())
                    .map_err(DatabaseError::Storage)?;
            }
            self.store.remove(key).map_err(DatabaseError::Storage)?;
        }

        Ok(())
    }

    /// Retrieves a record from the database by its key.
    ///
    /// The record must implement the [`DatabaseEntry`] trait, with the key type implementing the [`RecordKey`] trait pointing back to it.
    /// If the record is not found, `None` is returned.
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if the key cannot be serialized, if IO fails,
    /// or if deserializing the result fails.
    pub fn get<K: RecordKey>(&self, key: &K) -> Result<Option<K::Record>, DatabaseError<S>>
    where
        K::Record: DatabaseEntry<Key = K>,
        M: Manifests<K::Record>,
    {
        let serialized_key = wrap::<K::Record, S::Serializer>(key, &self.serialization_config)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let value = if let Some(value) = self
            .store
            .get(serialized_key)
            .map_err(DatabaseError::Storage)?
        {
            value
        } else {
            let Some(fallback) = &self.fallback else {
                return Ok(None);
            };
            let key = wrap::<K::Record, S::Serializer>(key, &self.serialization_config)
                .map_err(|e| DatabaseError::Storage(e.into()))?;
            let Some(value) = fallback.get(key).map_err(DatabaseError::Storage)? else {
                return Ok(None);
            };
            value
        };
        Ok(Some(
            self.serialization_config
                .deserialize_value(&value)
                .map_err(|e| DatabaseError::Storage(e.into()))?,
        ))
    }

    /// Removes a record from the database by its key and returns it.
    ///
    /// The record must implement the [`DatabaseEntry`] trait, with the key type implementing the [`RecordKey`] trait pointing back to it.
    /// If the record is not found, `None` is returned.
    /// The record's index entries are also removed.
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if the key cannot be serialized or if the underlying
    /// storage reports an error while removing or retrieving records.
    pub fn remove<K: RecordKey<Record = R>, R>(&mut self, key: &K) -> Result<(), DatabaseError<S>>
    where
        R: DatabaseEntry<Key = K>,
        R::Key: RecordKey<Record = R>,
        M: Manifests<R> + Manifests<K::Record>,
    {
        let Some(record) = self.get(key)? else {
            return Ok(());
        };
        let mut transaction = DatabaseTransaction::new(self);
        transaction
            .remove(key, &record)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        self.commit(transaction)?;
        Ok(())
    }

    /// Iterates over all keys in the database within the specified range.
    ///
    /// The range is inclusive of the start and exclusive of the end.
    /// The keys must implement the [`RecordKey`] trait, and the related [`DatabaseEntry`] must point back to it.
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if serializing the range bounds fails or if the
    /// underlying storage iterator errors.
    pub fn scan_keys<K: RecordKey + Ord>(
        &self,
        range: Range<K>,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<K::Record, S>> + use<'_, K, S, M>,
        DatabaseError<S>,
    >
    where
        K::Record: DatabaseEntry<Key = K>,
        M: Manifests<K::Record>,
    {
        let start = wrap::<K::Record, S::Serializer>(&range.start, &self.serialization_config)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let end = wrap::<K::Record, S::Serializer>(&range.end, &self.serialization_config)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let raw_iter = self
            .store
            .scan_keys(start..end)
            .map_err(DatabaseError::Storage)?;

        Ok(raw_iter.map(|elem| {
            let value = match elem {
                Ok(value) => value,
                Err(e) => return Err(DatabaseError::Storage(e)),
            };

            let deserialized: Wrap<K> = match self.serialization_config.deserialize_key(&value) {
                Ok(deserialized) => deserialized,
                Err(e) => return Err(DatabaseError::Storage(e.into())),
            };

            Ok(deserialized.key)
        }))
    }

    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if serializing the range bounds fails or if the
    /// underlying storage iterator errors.
    pub fn scan_all_keys<K: RecordKey + Ord>(
        &self,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<K::Record, S>> + use<'_, K, S, M>,
        DatabaseError<S>,
    >
    where
        K::Record: DatabaseEntry<Key = K>,
        M: Manifests<K::Record>,
    {
        let (start, end) = empty_wrap::<K::Record, S::Serializer>(&self.serialization_config)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let raw_iter = self
            .store
            .scan_keys(start..end)
            .map_err(DatabaseError::Storage)?;

        Ok(raw_iter.map(|elem| {
            let value = match elem {
                Ok(value) => value,
                Err(e) => return Err(DatabaseError::Storage(e)),
            };

            let deserialized: Wrap<K> = match self.serialization_config.deserialize_key(&value) {
                Ok(deserialized) => deserialized,
                Err(e) => return Err(DatabaseError::Storage(e.into())),
            };

            Ok(deserialized.key)
        }))
    }

    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if retrieving keys from the underlying storage fails.
    pub fn last_id<K: RecordKey + Ord + Default>(&self) -> Result<K, DatabaseError<S>>
    where
        K::Record: DatabaseEntry<Key = K>,
        M: Manifests<K::Record>,
    {
        let mut first = self.scan_all_keys::<K>()?;

        Ok(first.next().transpose()?.unwrap_or_default())
    }

    /// Iterates over all index entries in the database within the specified range and returns their primary keys.
    ///
    /// The range is inclusive of the start and exclusive of the end.
    /// The index must implement the [`Index`] trait.
    /// The returned iterator yields items of type `Result<Index::Record, DatabaseError<S>>`.
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if the underlying storage iterator encounters an error.
    pub fn scan_by_index<I: Index + Ord>(
        &self,
        range: Range<I>,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<I::Record, S>> + use<'_, I, S, M>,
        DatabaseError<S>,
    > {
        let mut start = self
            .serialization_config
            .serialize_key(WrapPrelude::new::<I::Record>(Subtable::Index(I::INDEX)))
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let mut end = start.clone();
        start.combine(
            self.serialization_config()
                .serialize_key(&range.start)
                .map_err(|e| DatabaseError::Storage(e.into()))?,
        );
        end.combine(
            self.serialization_config()
                .serialize_key(&range.end)
                .map_err(|e| DatabaseError::Storage(e.into()))?,
        );
        let raw_iter = self
            .store
            .scan_keys(start..end)
            .map_err(DatabaseError::Storage)?;

        Ok(raw_iter.map(|elem| self.process_iter_result(elem)))
    }

    /// Iterates over all index entries in the database that exactly match the given index key and returns their primary keys.
    ///
    /// This function outputs multiple results since multiple records can share the same index key.
    /// The index must implement the [`Index`] trait.
    /// The returned iterator yields items of type `Result<Index::Record, DatabaseError<S>>`.
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if the underlying storage iterator encounters an error.
    pub fn scan_by_index_exact<I: Index + Ord>(
        &self,
        index_key: &I,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<I::Record, S>> + use<'_, I, S, M>,
        DatabaseError<S>,
    > {
        let index_prelude = WrapPrelude::new::<I::Record>(Subtable::Index(I::INDEX));
        let mut start = self
            .serialization_config
            .serialize_key(index_prelude)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let mut end = start.clone();

        let start_bytes = self
            .serialization_config
            .serialize_key(index_key)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let end_bytes = {
            let mut end_bytes = start_bytes.clone();
            end_bytes.next();
            end_bytes
        };
        start.combine(start_bytes);
        end.combine(end_bytes);

        let raw_iter = self
            .store
            .scan_keys(start..end)
            .map_err(DatabaseError::Storage)?;

        Ok(raw_iter.map(|elem| self.process_iter_result(elem)))
    }

    /// Consumes the database and returns the underlying storage.
    pub fn dissolve(self) -> S {
        self.store
    }

    /// Returns the current [`Configuration`] used by the database.
    pub fn serialization_config(&self) -> &S::Serializer {
        &self.serialization_config
    }

    /// Helper function to process iterator results and get deserialized values
    fn process_iter_result<T: DeserializeOwned>(
        &self,
        result: Result<<S::Serializer as Unifier>::D, S::StoreError>,
    ) -> Result<T, DatabaseError<S>> {
        let key = result.map_err(DatabaseError::Storage)?;
        let value = match self.store.get(key) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return Err(DatabaseError::Internal(
                    crate::InternalDatabaseError::MissingIndexEntry,
                ));
            }
            Err(e) => return Err(DatabaseError::Storage(e)),
        };

        self.serialization_config
            .deserialize_value(&value)
            .map_err(|e| DatabaseError::Storage(e.into()))
    }
}
