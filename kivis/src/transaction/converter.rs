use crate::{
    BatchOp, BufferOverflowOr, DatabaseEntry, RecordKey, Repository, TryApplyError, Unifier,
    UnifierData, UnifierPair,
    transaction::buffer::PreBufferOps,
    wrap::{Subtable, WrapPrelude},
};

use super::errors::{ApplyError, TransactionError};

type WriteChain<'r, R, U> = core::iter::Chain<
    IndexWriteEntries<'r, R, U>,
    core::iter::Once<Result<BatchOp<U>, TransactionError<U>>>,
>;
type DeleteChain<'r, R, U> = core::iter::Chain<
    IndexDeleteKeys<'r, R, U>,
    core::iter::Once<Result<BatchOp<U>, TransactionError<U>>>,
>;

/// Concrete iterator returned by [`build_record_ops`].
pub enum RecordOps<'r, R: DatabaseEntry, U: UnifierPair> {
    Write(WriteChain<'r, R, U>),
    Delete(DeleteChain<'r, R, U>),
}

impl<R, U> Iterator for RecordOps<'_, R, U>
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    type Item = Result<BatchOp<U>, TransactionError<U>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Write(it) => it.next(),
            Self::Delete(it) => it.next(),
        }
    }
}

/// Returns a concrete iterator of [`BatchOp`]s for the given record operation.
///
/// Used by [`Manifest::record_ops`](crate::Manifest::record_ops) implementations inside the
/// `manifest!` macro and manual `Manifest` implementations.
#[doc(hidden)]
pub fn build_record_ops<'r, R, U>(
    op: PreBufferOps,
    record: &'r R,
    key: &'r R::Key,
    unifiers: U,
) -> RecordOps<'r, R, U>
where
    R: DatabaseEntry + Clone,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    match op {
        PreBufferOps::Insert | PreBufferOps::Put => RecordOps::Write(
            index_write_entries::<R, U>(record, key, unifiers)
                .chain(main_write_entry::<R, U>(record, key, unifiers)),
        ),
        PreBufferOps::Delete => RecordOps::Delete(
            index_delete_keys::<R, U>(record, key, unifiers)
                .chain(main_delete_key::<R, U>(key, unifiers)),
        ),
    }
}

/// Converts a record operation and applies the resulting writes or deletes directly to `repo`.
///
/// Used by `process_record` implementations inside the `manifest!` macro and manual
/// `Manifest` implementations.
#[doc(hidden)]
pub fn apply_record_ops<'r, R, U, Repo>(
    op: PreBufferOps,
    record: &'r R,
    key: &'r R::Key,
    unifiers: U,
    repo: &mut Repo,
) -> Result<(), ApplyError<U, Repo::Error>>
where
    R: DatabaseEntry + Clone,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair + 'r,
    Repo: Repository<K = <U::KeyUnifier as Unifier>::D, V = <U::ValueUnifier as Unifier>::D>,
{
    let result = match op {
        PreBufferOps::Insert | PreBufferOps::Put => repo.try_apply(
            index_write_entries::<R, U>(record, key, unifiers)
                .chain(main_write_entry::<R, U>(record, key, unifiers)),
        ),
        PreBufferOps::Delete => repo.try_apply(
            index_delete_keys::<R, U>(record, key, unifiers)
                .chain(main_delete_key::<R, U>(key, unifiers)),
        ),
    };
    result.map_err(|e| match e {
        TryApplyError::Iterator(e) => ApplyError::Transaction(e),
        TryApplyError::Storage(e) => ApplyError::Storage(e),
    })
}

/// Returns an iterator of `BatchOp::Insert` write buffers for index entries.
fn index_write_entries<'r, R, U>(
    record: &'r R,
    key: &'r R::Key,
    unifiers: U,
) -> IndexWriteEntries<'r, R, U>
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    IndexWriteEntries {
        record,
        key,
        key_serializer: unifiers.key_unifier(),
        value_serializer: unifiers.value_unifier(),
        cached_key_hash: None,
        cached_key_value: None,
        discriminator: 0,
    }
}

pub struct IndexWriteEntries<'r, R: DatabaseEntry, U: UnifierPair> {
    record: &'r R,
    key: &'r R::Key,
    key_serializer: U::KeyUnifier,
    value_serializer: U::ValueUnifier,
    cached_key_hash: Option<<U::KeyUnifier as Unifier>::D>,
    cached_key_value: Option<<U::ValueUnifier as Unifier>::D>,
    discriminator: u8,
}

impl<R, U> Iterator for IndexWriteEntries<'_, R, U>
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    type Item = Result<BatchOp<U>, TransactionError<U>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.discriminator >= R::INDEX_COUNT_HINT {
            return None;
        }
        let discriminator = self.discriminator;
        self.discriminator += 1;
        Some((|| {
            let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
            self.key_serializer.serialize(
                &mut key_buf,
                WrapPrelude::new::<R>(Subtable::Index(discriminator)),
            )?;
            self.record
                .index_key(&mut key_buf, discriminator, &self.key_serializer)?;
            let key_hash = if let Some(kh) = &self.cached_key_hash {
                kh.clone()
            } else {
                let mut kh = <U::KeyUnifier as Unifier>::D::default();
                self.key_serializer.serialize_ref(&mut kh, self.key)?;
                self.cached_key_hash = Some(kh.clone());
                kh
            };
            key_buf
                .extend_from(key_hash.as_view())
                .map_err(BufferOverflowOr::overflow)?;
            let value = if let Some(kv) = &self.cached_key_value {
                kv.clone()
            } else {
                let mut kv = <U::ValueUnifier as Unifier>::D::default();
                self.value_serializer
                    .serialize_ref(&mut kv, self.key)
                    .map_err(TransactionError::from_value)?;
                self.cached_key_value = Some(kv.clone());
                kv
            };
            Ok(BatchOp::Insert {
                key: key_buf,
                value,
            })
        })())
    }
}

/// Returns a single-item iterator with a `BatchOp::Insert` for the main record entry.
fn main_write_entry<'r, R, U>(
    record: &'r R,
    key: &'r R::Key,
    unifiers: U,
) -> core::iter::Once<Result<BatchOp<U>, TransactionError<U>>>
where
    R: DatabaseEntry + Clone,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    let result = (|| {
        let key_serializer = unifiers.key_unifier();
        let value_serializer = unifiers.value_unifier();
        let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
        key_serializer.serialize(&mut key_buf, WrapPrelude::new::<R>(Subtable::Main))?;
        key_serializer.serialize_ref(&mut key_buf, key)?;
        let mut value_buf = <U::ValueUnifier as Unifier>::D::default();
        value_serializer
            .serialize_ref(&mut value_buf, record)
            .map_err(TransactionError::from_value)?;
        Ok(BatchOp::Insert {
            key: key_buf,
            value: value_buf,
        })
    })();
    core::iter::once(result)
}

/// Returns a single-item iterator with a `BatchOp::Delete` for the main record entry.
fn main_delete_key<R, U>(
    key: &R::Key,
    unifiers: U,
) -> core::iter::Once<Result<BatchOp<U>, TransactionError<U>>>
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    let result = (|| {
        let key_serializer = unifiers.key_unifier();
        let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
        key_serializer.serialize(&mut key_buf, WrapPrelude::new::<R>(Subtable::Main))?;
        key_serializer.serialize_ref(&mut key_buf, key)?;
        Ok(BatchOp::Delete { key: key_buf })
    })();
    core::iter::once(result)
}

/// Each item contains a `BatchOp::Delete` with the complete serialized key
/// (prelude + `index_key` + `primary_key`) for one index entry.
fn index_delete_keys<'r, R, U>(
    record: &'r R,
    key: &'r R::Key,
    unifiers: U,
) -> IndexDeleteKeys<'r, R, U>
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    IndexDeleteKeys {
        record,
        key,
        key_serializer: unifiers.key_unifier(),
        cached_key: None,
        discriminator: 0,
    }
}

pub struct IndexDeleteKeys<'r, R: DatabaseEntry, U: UnifierPair> {
    record: &'r R,
    key: &'r R::Key,
    key_serializer: U::KeyUnifier,
    cached_key: Option<<U::KeyUnifier as Unifier>::D>,
    discriminator: u8,
}

impl<R, U> Iterator for IndexDeleteKeys<'_, R, U>
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    type Item = Result<BatchOp<U>, TransactionError<U>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.discriminator >= R::INDEX_COUNT_HINT {
            return None;
        }
        let discriminator = self.discriminator;
        self.discriminator += 1;
        Some((|| {
            let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
            self.key_serializer.serialize(
                &mut key_buf,
                WrapPrelude::new::<R>(Subtable::Index(discriminator)),
            )?;
            self.record
                .index_key(&mut key_buf, discriminator, &self.key_serializer)?;
            let key_bytes = if let Some(kb) = &self.cached_key {
                kb.clone()
            } else {
                let mut kb = <U::KeyUnifier as Unifier>::D::default();
                self.key_serializer.serialize_ref(&mut kb, self.key)?;
                self.cached_key = Some(kb.clone());
                kb
            };
            key_buf
                .extend_from(key_bytes.as_view())
                .map_err(BufferOverflowOr::overflow)?;
            Ok(BatchOp::Delete { key: key_buf })
        })())
    }
}
