use crate::{
    BatchOp, BufferOverflowOr, DatabaseEntry, RecordKey, Repository, TryApplyError, Unifier,
    UnifierData, UnifierPair,
    transaction::buffer::PreBufferOps,
    wrap::{Subtable, WrapPrelude},
};

use super::errors::{ApplyError, TransactionError};

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
) -> impl Iterator<Item = Result<BatchOp<U>, TransactionError<U>>> + 'r
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair + 'r,
{
    let key_serializer = unifiers.key_unifier();
    let value_serializer = unifiers.value_unifier();
    let mut cached_key_hash: Option<<U::KeyUnifier as Unifier>::D> = None;
    let mut cached_key_value: Option<<U::ValueUnifier as Unifier>::D> = None;
    (0..R::INDEX_COUNT_HINT).map(move |discriminator| {
        let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
        key_serializer.serialize(
            &mut key_buf,
            WrapPrelude::new::<R>(Subtable::Index(discriminator)),
        )?;
        record.index_key(&mut key_buf, discriminator, &key_serializer)?;
        let key_hash = if let Some(kh) = &cached_key_hash {
            kh.clone()
        } else {
            let mut kh = <U::KeyUnifier as Unifier>::D::default();
            key_serializer.serialize_ref(&mut kh, key)?;
            cached_key_hash = Some(kh.clone());
            kh
        };
        key_buf
            .extend_from(key_hash.as_view())
            .map_err(BufferOverflowOr::overflow)?;
        let value = if let Some(kv) = &cached_key_value {
            kv.clone()
        } else {
            let mut kv = <U::ValueUnifier as Unifier>::D::default();
            value_serializer
                .serialize_ref(&mut kv, key)
                .map_err(TransactionError::from_value)?;
            cached_key_value = Some(kv.clone());
            kv
        };
        Ok(BatchOp::Insert {
            key: key_buf,
            value,
        })
    })
}

/// Returns a single-item iterator with a `BatchOp::Insert` for the main record entry.
fn main_write_entry<'r, R, U>(
    record: &'r R,
    key: &'r R::Key,
    unifiers: U,
) -> impl Iterator<Item = Result<BatchOp<U>, TransactionError<U>>> + 'r
where
    R: DatabaseEntry + Clone,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair + 'r,
{
    core::iter::once_with(move || {
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
    })
}

/// Returns a single-item iterator with a `BatchOp::Delete` for the main record entry.
fn main_delete_key<'r, R, U>(
    key: &'r R::Key,
    unifiers: U,
) -> impl Iterator<Item = Result<BatchOp<U>, TransactionError<U>>> + 'r
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair + 'r,
{
    core::iter::once_with(move || {
        let key_serializer = unifiers.key_unifier();
        let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
        key_serializer.serialize(&mut key_buf, WrapPrelude::new::<R>(Subtable::Main))?;
        key_serializer.serialize_ref(&mut key_buf, key)?;
        Ok(BatchOp::Delete { key: key_buf })
    })
}

/// Each item contains a `BatchOp::Delete` with the complete serialized key
/// (prelude + `index_key` + `primary_key`) for one index entry.
fn index_delete_keys<'r, R, U>(
    record: &'r R,
    key: &'r R::Key,
    unifiers: U,
) -> impl Iterator<Item = Result<BatchOp<U>, TransactionError<U>>> + 'r
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair + 'r,
{
    let key_serializer = unifiers.key_unifier();
    let mut cached_key: Option<<U::KeyUnifier as Unifier>::D> = None;
    (0..R::INDEX_COUNT_HINT).map(move |discriminator| {
        let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
        key_serializer.serialize(
            &mut key_buf,
            WrapPrelude::new::<R>(Subtable::Index(discriminator)),
        )?;
        record.index_key(&mut key_buf, discriminator, &key_serializer)?;

        let key_bytes = if let Some(kb) = &cached_key {
            kb.clone()
        } else {
            let mut kb = <U::KeyUnifier as Unifier>::D::default();
            key_serializer.serialize_ref(&mut kb, key)?;
            cached_key = Some(kb.clone());
            kb
        };

        key_buf
            .extend_from(key_bytes.as_view())
            .map_err(BufferOverflowOr::overflow)?;
        Ok(BatchOp::Delete { key: key_buf })
    })
}
