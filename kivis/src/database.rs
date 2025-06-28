use crate::errors::DatabaseError;
use crate::traits::{Incrementable, Index, Recordable, Storage};
use crate::wrap::{Wrap, decode_value, encode_value, wrap, wrap_just_index};
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
        R::Key: Incrementable,
    {
        let original_key = if let Some(key) = record.key() {
            key
        } else {
            R::Key::next_id(&self.last_id::<R>()?).ok_or(DatabaseError::FailedToIncrement)?
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

    pub fn get<R: Recordable>(
        &self,
        key: &R::Key,
    ) -> Result<Option<R>, DatabaseError<S::StoreError>> {
        let key = wrap::<R>(key).map_err(DatabaseError::Serialization)?;
        let Some(value) = self.store.get(key).map_err(DatabaseError::Io)? else {
            return Ok(None);
        };
        Ok(Some(
            decode_value(&value).map_err(DatabaseError::Deserialization)?,
        ))
    }

    pub fn remove<R: Recordable>(
        &mut self,
        key: &R::Key,
    ) -> Result<Option<R>, DatabaseError<S::StoreError>> {
        let key = wrap::<R>(key).map_err(DatabaseError::Serialization)?;
        let Some(value) = self.store.remove(key).map_err(DatabaseError::Io)? else {
            return Ok(None);
        };
        Ok(Some(
            decode_value(&value).map_err(DatabaseError::Deserialization)?,
        ))
    }

    pub fn iter_keys<R: Recordable>(
        &self,
        range: Range<R::Key>,
    ) -> Result<impl Iterator<Item = DatabaseIteratorItem<R, S>>, DatabaseError<S::StoreError>>
    {
        let start = wrap::<R>(&range.start).map_err(DatabaseError::Serialization)?;
        let end = wrap::<R>(&range.end).map_err(DatabaseError::Serialization)?;
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

                let deserialized: Wrap<R::Key> = match bcs::from_bytes(&value) {
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

                let deserialized: Wrap<(Vec<u8>, <I::Record as Recordable>::Key)> =
                    match bcs::from_bytes(&value) {
                        Ok(deserialized) => deserialized,
                        Err(e) => return Err(DatabaseError::Deserialization(e)),
                    };

                Ok(deserialized.key.1)
            }),
        )
    }

    pub fn last_id<R: Recordable>(&self) -> Result<R::Key, DatabaseError<S::StoreError>>
    where
        R::Key: Incrementable,
    {
        let (start, end) = R::Key::bounds().ok_or(DatabaseError::ToAutoincrement)?;
        let range = if start < end {
            start.clone()..end
        } else {
            end..start.clone()
        };
        let mut first = self.iter_keys::<R>(range)?;
        Ok(first.next().transpose()?.unwrap_or(start))
    }
}
