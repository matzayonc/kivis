use crate::errors::DatabaseError;
use crate::traits::{DatabaseEntry, Index, Storage};
use crate::transaction::DatabaseTransaction;
use crate::wrap::{Subtable, Wrap, WrapPrelude, empty_wrap};
use crate::{
    DeriveKey, Incrementable, Manifest, Manifests, RecordKey, Unifiable, Unifier, UnifierData,
};
use core::ops::Range;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

type DatabaseIteratorItem<R, S> = Result<<R as DatabaseEntry>::Key, DatabaseError<S>>;

/// The `kivis` database type. All interactions with the database are done through this type.
pub struct Database<S: Storage, M: Manifest> {
    pub(crate) store: S,
    // fallback: Option<Box<dyn StorageInner<StoreError = S::StoreError>>>,
    pub(crate) manifest: M,
    pub(crate) serializer: <S as Storage>::Serializer,
}

impl<S: Storage, M: Manifest> Database<S, M>
where
    S::Serializer: Unifier + Copy,
{
    /// Creates a new [`Database`] instance over any storage backend.
    /// One of the key features of `kivis` is that it can work with any storage backend that implements the [`Storage`] trait.
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if the manifest fails to load during initialization.
    pub fn new(store: S) -> Result<Self, DatabaseError<S>> {
        let mut db = Database {
            store,
            // fallback: None,
            manifest: M::default(),
            serializer: S::Serializer::default(),
        };
        let mut manifest = M::default();
        manifest.load(&mut db)?;
        db.manifest = manifest;
        Ok(db)
    }

    pub fn with_serializer(&mut self, config: <S as Storage>::Serializer) {
        self.serializer = config;
    }

    /// Sets a fallback storage that will be used if the main storage does not contain the requested record.
    /// The current storage then becomes the cache for the fallback storage.
    pub fn set_fallback(
        &mut self,
        _fallback: Box<dyn Storage<Serializer = S::Serializer, StoreError = S::StoreError>>,
    ) {
        // self.fallback = Some(fallback);
    }

    /// Add a record with autoincremented key into the database, together with all related index entries.
    ///
    /// The record must implement the [`DatabaseEntry`] trait, with the key type implementing the [`RecordKey`] trait pointing back to it.
    /// The record's key must implement the [`Incrementable`] trait.
    /// For records that do not have an autoincremented key, use [`Self::insert`] instead.
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if serializing or writing the record fails.
    pub fn put<R: DatabaseEntry>(&mut self, record: R) -> Result<R::Key, DatabaseError<S>>
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
    pub fn insert<K: RecordKey<Record = R>, R>(&mut self, record: R) -> Result<K, DatabaseError<S>>
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

    /// Commits a transaction to the database.
    ///
    /// All operations are applied using the storage backend's `batch_mixed` method.
    ///
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if writing to the underlying storage fails.
    pub fn commit(
        &mut self,
        transaction: DatabaseTransaction<M, S::Serializer>,
    ) -> Result<(), DatabaseError<S>> {
        transaction.commit(&mut self.store)?;
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
        let mut serialized_key =
            <<<S as Storage>::Serializer as Unifier>::K as UnifierData>::Owned::default();

        wrap::<K::Record, S::Serializer>(key, &self.serializer, &mut serialized_key)
            .map_err(|e| DatabaseError::Storage(e.into()))?;

        let Some(value) = self
            .store
            .get(serialized_key.as_ref())
            .map_err(DatabaseError::Storage)?
        else {
            // let Some(fallback) = &self.fallback else {
            //     return Ok(None);
            // };
            // let key = wrap::<K::Record, S::Serializer>(key, &self.serializer)
            //     .map_err(|e| DatabaseError::Storage(e.into()))?;
            // let Some(value) = fallback.get(key).map_err(DatabaseError::Storage)? else {
            //     return Ok(None);
            // };
            // value
            return Ok(None);
        };
        Ok(Some(
            self.serializer
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
    pub fn iter_keys<K: RecordKey + Ord>(
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
        let mut start =
            <<<S as Storage>::Serializer as Unifier>::K as UnifierData>::Owned::default();
        wrap::<K::Record, S::Serializer>(&range.start, &self.serializer, &mut start)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let mut end = <<<S as Storage>::Serializer as Unifier>::K as UnifierData>::Owned::default();
        wrap::<K::Record, S::Serializer>(&range.end, &self.serializer, &mut end)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let raw_iter = self
            .store
            .iter_keys(start..end)
            .map_err(DatabaseError::Storage)?;

        Ok(raw_iter.map(|elem| {
            let value = match elem {
                Ok(value) => value,
                Err(e) => return Err(DatabaseError::Storage(e)),
            };

            let deserialized: Wrap<K> = match self.serializer.deserialize_key(&value) {
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
    pub fn iter_all_keys<K: RecordKey + Ord>(
        &self,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<K::Record, S>> + use<'_, K, S, M>,
        DatabaseError<S>,
    >
    where
        K::Record: DatabaseEntry<Key = K>,
        M: Manifests<K::Record>,
    {
        let (start, end) = empty_wrap::<K::Record, S::Serializer>(&self.serializer)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let raw_iter = self
            .store
            .iter_keys(start..end)
            .map_err(DatabaseError::Storage)?;

        Ok(raw_iter.map(|elem| {
            let value = match elem {
                Ok(value) => value,
                Err(e) => return Err(DatabaseError::Storage(e)),
            };

            let deserialized: Wrap<K> = match self.serializer.deserialize_key(&value) {
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
        let mut first = self.iter_all_keys::<K>()?;

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
    pub fn iter_by_index<I: Index + Ord>(
        &self,
        range: Range<I>,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<I::Record, S>> + use<'_, I, S, M>,
        DatabaseError<S>,
    > {
        let mut start =
            <<<S as Storage>::Serializer as Unifier>::K as UnifierData>::Owned::default();
        self.serializer
            .serialize_key(
                &mut start,
                WrapPrelude::new::<I::Record>(Subtable::Index(I::INDEX)),
            )
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let mut end = <<S as Storage>::Serializer as Unifier>::K::duplicate(start.as_ref());

        self.serializer()
            .serialize_key(&mut start, range.start)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        self.serializer()
            .serialize_key(&mut end, range.end)
            .map_err(|e| DatabaseError::Storage(e.into()))?;

        let raw_iter = self
            .store
            .iter_keys(start..end)
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
    pub fn iter_by_index_exact<I: Index + Ord>(
        &self,
        index_key: I,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<I::Record, S>> + use<'_, I, S, M>,
        DatabaseError<S>,
    > {
        let index_prelude = WrapPrelude::new::<I::Record>(Subtable::Index(I::INDEX));
        let mut start =
            <<<S as Storage>::Serializer as Unifier>::K as UnifierData>::Owned::default();
        self.serializer
            .serialize_key(&mut start, index_prelude)
            .map_err(|e| DatabaseError::Storage(e.into()))?;

        self.serializer
            .serialize_key(&mut start, index_key)
            .map_err(|e| DatabaseError::Storage(e.into()))?;
        let mut end = <S::Serializer as Unifier>::K::duplicate(start.as_ref());
        <S::Serializer as Unifier>::K::next(&mut end);

        let raw_iter = self
            .store
            .iter_keys(start..end)
            .map_err(DatabaseError::Storage)?;

        Ok(raw_iter.map(|elem| self.process_iter_result(elem)))
    }

    /// Consumes the database and returns the underlying storage.
    pub fn dissolve(self) -> S {
        self.store
    }

    /// Returns the current [`S::Serializer`] used by the database.
    pub fn serializer(&self) -> &S::Serializer {
        &self.serializer
    }

    /// Helper function to process iterator results and get deserialized values
    fn process_iter_result<T: Unifiable>(
        &self,
        result: Result<<<S::Serializer as Unifier>::K as UnifierData>::Owned, S::StoreError>,
    ) -> Result<T, DatabaseError<S>> {
        let key = result.map_err(DatabaseError::Storage)?;
        let value = match self.store.get(key.as_ref()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return Err(DatabaseError::Internal(
                    crate::InternalDatabaseError::MissingIndexEntry,
                ));
            }
            Err(e) => return Err(DatabaseError::Storage(e)),
        };

        self.serializer
            .deserialize_value(&value)
            .map_err(|e| DatabaseError::Storage(e.into()))
    }
}

/// Wraps a database entry key with scope and subtable information for storage.
fn wrap<R: DatabaseEntry, U: Unifier>(
    item_key: &R::Key,
    unifier: &U,
    buffer: &mut <U::K as UnifierData>::Owned,
) -> Result<(), U::SerError> {
    unifier.serialize_key(buffer, WrapPrelude::new::<R>(Subtable::Main))?;
    unifier.serialize_key_ref(buffer, item_key)?;
    Ok(())
}
