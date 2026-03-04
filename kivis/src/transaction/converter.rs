use crate::{
    BufferOverflowOr, DatabaseEntry, RecordKey, Repository, Unifier, UnifierData, UnifierPair,
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
    let key_ser = unifiers.key_unifier();
    let val_ser = unifiers.value_unifier();
    match op {
        PreBufferOps::Insert | PreBufferOps::Put => {
            for result in prepare_writes::<R, U>(record, key, key_ser, val_ser) {
                let (key_buf, value_buf) = result.map_err(ApplyError::Transaction)?;
                repo.insert_entry(key_buf.as_view(), value_buf.as_view())
                    .map_err(ApplyError::Storage)?;
            }
        }
        PreBufferOps::Delete => {
            for result in prepare_deletes::<R, U>(record, key, key_ser) {
                let key_buf = result.map_err(ApplyError::Transaction)?;
                repo.remove_entry(key_buf.as_view())
                    .map_err(ApplyError::Storage)?;
            }
        }
    }
    Ok(())
}

/// Returns an iterator of fully-assembled `(key_buf, value_buf)` pairs for all entries
/// (index entries + main record).
#[allow(clippy::type_complexity)]
pub(super) fn prepare_writes<'r, R, U>(
    record: &'r R,
    key: &'r R::Key,
    key_serializer: U::KeyUnifier,
    value_serializer: U::ValueUnifier,
) -> impl Iterator<
    Item = Result<
        (
            <U::KeyUnifier as Unifier>::D,
            <U::ValueUnifier as Unifier>::D,
        ),
        TransactionError<U>,
    >,
> + 'r
where
    R: DatabaseEntry + Clone,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair + 'r,
{
    index_write_entries::<R, U>(record, key, key_serializer, value_serializer).chain(
        main_write_entry::<R, U>(record, key, key_serializer, value_serializer),
    )
}

/// Returns an iterator of fully-assembled key buffers for all delete entries
/// (index entries + main record).
pub(super) fn prepare_deletes<'r, R, U>(
    record: &'r R,
    key: &'r R::Key,
    key_serializer: U::KeyUnifier,
) -> impl Iterator<Item = Result<<U::KeyUnifier as Unifier>::D, TransactionError<U>>> + 'r
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair + 'r,
{
    index_delete_keys::<R, U>(record, key, key_serializer)
        .chain(main_delete_key::<R, U>(key, key_serializer))
}

/// Returns an iterator of fully-assembled (key, value) write buffers for index entries.
#[allow(clippy::type_complexity)]
fn index_write_entries<'r, R, U>(
    record: &'r R,
    key: &'r R::Key,
    key_serializer: U::KeyUnifier,
    value_serializer: U::ValueUnifier,
) -> impl Iterator<
    Item = Result<
        (
            <U::KeyUnifier as Unifier>::D,
            <U::ValueUnifier as Unifier>::D,
        ),
        TransactionError<U>,
    >,
> + 'r
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair + 'r,
{
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
        let key_value = if let Some(kv) = &cached_key_value {
            kv.clone()
        } else {
            let mut kv = <U::ValueUnifier as Unifier>::D::default();
            value_serializer
                .serialize_ref(&mut kv, key)
                .map_err(TransactionError::from_value)?;
            cached_key_value = Some(kv.clone());
            kv
        };
        Ok((key_buf, key_value))
    })
}

/// Returns a single-item iterator with the fully-assembled (key, value) write buffers
/// for the main record entry.
#[allow(clippy::type_complexity)]
fn main_write_entry<'r, R, U>(
    record: &'r R,
    key: &'r R::Key,
    key_serializer: U::KeyUnifier,
    value_serializer: U::ValueUnifier,
) -> impl Iterator<
    Item = Result<
        (
            <U::KeyUnifier as Unifier>::D,
            <U::ValueUnifier as Unifier>::D,
        ),
        TransactionError<U>,
    >,
> + 'r
where
    R: DatabaseEntry + Clone,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair + 'r,
{
    core::iter::once_with(move || {
        let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
        key_serializer.serialize(&mut key_buf, WrapPrelude::new::<R>(Subtable::Main))?;
        key_serializer.serialize_ref(&mut key_buf, key)?;
        let mut value_buf = <U::ValueUnifier as Unifier>::D::default();
        value_serializer
            .serialize_ref(&mut value_buf, record)
            .map_err(TransactionError::from_value)?;
        Ok((key_buf, value_buf))
    })
}

fn main_delete_key<'r, R, U>(
    key: &'r R::Key,
    key_serializer: U::KeyUnifier,
) -> impl Iterator<Item = Result<<U::KeyUnifier as Unifier>::D, TransactionError<U>>> + 'r
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair + 'r,
{
    core::iter::once_with(move || {
        let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
        key_serializer.serialize(&mut key_buf, WrapPrelude::new::<R>(Subtable::Main))?;
        key_serializer.serialize_ref(&mut key_buf, key)?;
        Ok(key_buf)
    })
}

/// Each item contains the complete serialized key (prelude + `index_key` + `primary_key`)
/// for one index entry.
fn index_delete_keys<'r, R, U>(
    record: &'r R,
    key: &'r R::Key,
    key_serializer: U::KeyUnifier,
) -> impl Iterator<Item = Result<<U::KeyUnifier as Unifier>::D, TransactionError<U>>> + 'r
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair + 'r,
{
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
        Ok(key_buf)
    })
}
