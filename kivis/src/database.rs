use crate::RecordKey;
use crate::errors::DatabaseError;
use crate::traits::{Incrementable, Index, Recordable, Storage};
use crate::wrap::{Wrap, decode_value, encode_value, unwrap_index, wrap, wrap_just_index};
use std::ops::Range;

type DatabaseIteratorItem<R, S> =
    Result<<R as Recordable>::Key, DatabaseError<<S as Storage>::StoreError>>;

pub struct Database<S: Storage> {
    store: S,
}

impl<S: Storage> Database<S> {
    pub fn new(store: S) -> Self {
        Database { store }
    }

    pub fn dissolve(self) -> S {
        self.store
    }

    pub fn insert<R: Recordable>(
        &mut self,
        record: R,
    ) -> Result<R::Key, DatabaseError<<S as Storage>::StoreError>>
    where
        R::Key: Incrementable + RecordKey,
        <R::Key as RecordKey>::Record: Recordable<Key = R::Key>,
    {
        let original_key = if let Some(key) = record.maybe_key() {
            key
        } else {
            R::Key::next_id(&self.last_id::<R::Key>()?).ok_or(DatabaseError::FailedToIncrement)?
        };

        let key = wrap::<R>(&original_key).map_err(DatabaseError::Serialization)?;

        for index_value in record
            .index_keys(original_key.clone())
            .map_err(DatabaseError::Serialization)?
        {
            self.store
                .insert(index_value, Vec::new())
                .map_err(DatabaseError::Io)?
        }

        let value = encode_value(&record).map_err(DatabaseError::Serialization)?;
        self.store.insert(key, value).map_err(DatabaseError::Io)?;
        Ok(original_key)
    }

    pub fn get<K: RecordKey>(
        &self,
        key: &K,
    ) -> Result<Option<K::Record>, DatabaseError<S::StoreError>>
    where
        K::Record: Recordable<Key = K>,
    {
        let key = wrap::<K::Record>(key).map_err(DatabaseError::Serialization)?;
        let Some(value) = self.store.get(key).map_err(DatabaseError::Io)? else {
            return Ok(None);
        };
        Ok(Some(
            decode_value(&value).map_err(DatabaseError::Deserialization)?,
        ))
    }

    pub fn remove<K: RecordKey>(
        &mut self,
        key: &K,
    ) -> Result<Option<K::Record>, DatabaseError<S::StoreError>>
    where
        K::Record: Recordable<Key = K>,
    {
        let key = wrap::<K::Record>(key).map_err(DatabaseError::Serialization)?;
        let Some(value) = self.store.remove(key).map_err(DatabaseError::Io)? else {
            return Ok(None);
        };
        Ok(Some(
            decode_value(&value).map_err(DatabaseError::Deserialization)?,
        ))
    }

    pub fn iter_keys<K: RecordKey>(
        &self,
        range: Range<K>,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<K::Record, S>>,
        DatabaseError<S::StoreError>,
    >
    where
        K::Record: Recordable<Key = K>,
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

    pub fn iter_by_index<I: Index>(
        &mut self,
        range: Range<I>,
    ) -> Result<
        impl Iterator<Item = DatabaseIteratorItem<I::Record, S>>,
        DatabaseError<S::StoreError>,
    > {
        let start =
            wrap_just_index::<I::Record, I>(range.start).map_err(DatabaseError::Serialization)?;
        let end =
            wrap_just_index::<I::Record, I>(range.end).map_err(DatabaseError::Serialization)?;
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

                let entry = unwrap_index::<I::Record, I>(&value)?;

                Ok(entry)
            }),
        )
    }

    pub fn last_id<K: RecordKey>(&self) -> Result<K, DatabaseError<S::StoreError>>
    where
        K: Incrementable,
        K::Record: Recordable<Key = K>,
    {
        let (start, end) = K::bounds().ok_or(DatabaseError::ToAutoincrement)?;
        let range = if start < end {
            start.clone()..end
        } else {
            end..start.clone()
        };
        let mut first = self.iter_keys::<K>(range)?;
        Ok(first.next().transpose()?.unwrap_or(start))
    }
}
