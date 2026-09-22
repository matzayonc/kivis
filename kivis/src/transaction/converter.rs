use core::cmp::Ordering;

use crate::{
    BatchOp, BufferOverflowOr, DatabaseEntry, RecordKey, Unified, Unifier, UnifierPair,
    transaction::buffer::PreBufferOps,
    wrap::{Subtable, WrapPrelude},
};

use super::errors::TransactionError;

/// The record a [`RecordOps`] iterator serialises: either borrowed from the transaction arena,
/// or owned when it is a previous version loaded from storage to clean up stale index entries.
enum Held<'r, R: DatabaseEntry> {
    Borrowed { record: &'r R, key: &'r R::Key },
    Owned { record: R, key: R::Key },
}

impl<R: DatabaseEntry> Held<'_, R> {
    fn record(&self) -> &R {
        match self {
            Held::Borrowed { record, .. } => record,
            Held::Owned { record, .. } => record,
        }
    }

    fn key(&self) -> &R::Key {
        match self {
            Held::Borrowed { key, .. } => key,
            Held::Owned { key, .. } => key,
        }
    }
}

/// Concrete iterator of [`BatchOp`]s for a single record write or delete.
///
/// Yields index entries first (one per `INDEX_COUNT_HINT`), then the main record entry.
/// For [`PreBufferOps::DeleteIndexes`] only the index entries are yielded.
pub struct RecordOps<'r, R: DatabaseEntry, U: UnifierPair> {
    held: Held<'r, R>,
    key_unifier: U::KeyUnifier,
    value_unifier: U::ValueUnifier,
    op: PreBufferOps,
    /// Counts through `0..INDEX_COUNT_HINT` (index phase) then `INDEX_COUNT_HINT` (main), then done.
    discriminator: u8,
    /// Serialized primary key, computed once and reused across index entries.
    cached_key: Option<<U::KeyUnifier as Unifier>::D>,
    /// Serialized primary key as a value (write path only), computed once and reused.
    cached_key_value: Option<<U::ValueUnifier as Unifier>::D>,
}

impl<R, U> Iterator for RecordOps<'_, R, U>
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    type Item = Result<BatchOp<U>, TransactionError<U>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.discriminator.cmp(&R::INDEX_COUNT_HINT) {
            Ordering::Less => {
                let d = self.discriminator;
                self.discriminator += 1;
                Some(self.index_op(d))
            }
            Ordering::Equal => {
                self.discriminator += 1;
                match self.op {
                    PreBufferOps::DeleteIndexes => None,
                    _ => Some(self.main_op()),
                }
            }
            Ordering::Greater => None,
        }
    }
}

impl<R, U> RecordOps<'_, R, U>
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    fn cached_key(&mut self) -> Result<<U::KeyUnifier as Unifier>::D, TransactionError<U>> {
        if let Some(k) = &self.cached_key {
            return Ok(k.clone());
        }
        let mut kb = <U::KeyUnifier as Unifier>::D::default();
        self.key_unifier.serialize(&mut kb, self.held.key())?;
        Ok(self.cached_key.insert(kb).clone())
    }

    fn cached_key_value(&mut self) -> Result<<U::ValueUnifier as Unifier>::D, TransactionError<U>> {
        if let Some(v) = &self.cached_key_value {
            return Ok(v.clone());
        }
        let mut kv = <U::ValueUnifier as Unifier>::D::default();
        self.value_unifier
            .serialize(&mut kv, self.held.key())
            .map_err(TransactionError::from_value)?;
        Ok(self.cached_key_value.insert(kv).clone())
    }

    fn index_op(&mut self, discriminator: u8) -> Result<BatchOp<U>, TransactionError<U>> {
        let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
        self.key_unifier.serialize(
            &mut key_buf,
            &WrapPrelude::new::<R>(Subtable::Index(discriminator)),
        )?;
        self.held
            .record()
            .index_key(&mut key_buf, discriminator, &self.key_unifier)?;
        let key_bytes = self.cached_key()?;
        key_buf
            .extend_from(key_bytes.as_view())
            .map_err(BufferOverflowOr::overflow)?;
        match self.op {
            PreBufferOps::Insert | PreBufferOps::Put => {
                let value = self.cached_key_value()?;
                Ok(BatchOp::Insert {
                    key: key_buf,
                    value,
                })
            }
            PreBufferOps::Delete | PreBufferOps::DeleteIndexes => {
                Ok(BatchOp::Delete { key: key_buf })
            }
        }
    }

    fn main_op(&mut self) -> Result<BatchOp<U>, TransactionError<U>> {
        let key_buf = build_main_key::<R, U>(self.held.key(), self.key_unifier)?;
        match self.op {
            PreBufferOps::Insert | PreBufferOps::Put => {
                let mut value_buf = <U::ValueUnifier as Unifier>::D::default();
                self.value_unifier
                    .serialize(&mut value_buf, self.held.record())
                    .map_err(TransactionError::from_value)?;
                Ok(BatchOp::Insert {
                    key: key_buf,
                    value: value_buf,
                })
            }
            PreBufferOps::Delete | PreBufferOps::DeleteIndexes => {
                Ok(BatchOp::Delete { key: key_buf })
            }
        }
    }
}

/// Returns a concrete iterator of [`BatchOp`]s for the given record operation.
///
/// Used by [`Manifest::iter_ops`](crate::Manifest::iter_ops) implementations inside the
/// `manifest!` macro and manual `Manifest` implementations.
#[doc(hidden)]
pub fn build_record_ops<'r, R, U>(
    op: PreBufferOps,
    record: &'r R,
    key: &'r R::Key,
    unifiers: U,
) -> RecordOps<'r, R, U>
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    RecordOps {
        held: Held::Borrowed { record, key },
        key_unifier: unifiers.key_unifier(),
        value_unifier: unifiers.value_unifier(),
        op,
        discriminator: 0,
        cached_key: None,
        cached_key_value: None,
    }
}

/// Serialises the storage key of the main entry for `key` (scope, main subtable, then the key).
///
/// Used by [`Manifest::main_key`](crate::Manifest::main_key) implementations inside the
/// `manifest!` macro and manual `Manifest` implementations.
///
/// # Errors
///
/// Returns a [`TransactionError`] if serialising the key fails.
#[doc(hidden)]
pub fn build_main_key<R, U>(
    key: &R::Key,
    key_unifier: U::KeyUnifier,
) -> Result<<U::KeyUnifier as Unifier>::D, TransactionError<U>>
where
    R: DatabaseEntry,
    U: UnifierPair,
{
    let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
    key_unifier.serialize(&mut key_buf, &WrapPrelude::new::<R>(Subtable::Main))?;
    key_unifier.serialize(&mut key_buf, key)?;
    Ok(key_buf)
}

/// Deserialises the previous version of the record stored under `key` from `previous` and
/// returns an iterator that deletes that version's index entries (and nothing else).
///
/// Used by [`Manifest::stale_index_ops`](crate::Manifest::stale_index_ops) implementations
/// inside the `manifest!` macro and manual `Manifest` implementations.
///
/// # Errors
///
/// Returns the value unifier's error if `previous` cannot be deserialised as `R`.
#[doc(hidden)]
pub fn build_stale_index_ops<'r, R, U>(
    key: &R::Key,
    previous: &<U::ValueUnifier as Unifier>::D,
    unifiers: U,
) -> Result<RecordOps<'r, R, U>, <U::ValueUnifier as Unifier>::DeError>
where
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    let record: R = unifiers.value_unifier().deserialize(previous)?;
    Ok(RecordOps {
        held: Held::Owned {
            record,
            key: key.clone(),
        },
        key_unifier: unifiers.key_unifier(),
        value_unifier: unifiers.value_unifier(),
        op: PreBufferOps::DeleteIndexes,
        discriminator: 0,
        cached_key: None,
        cached_key_value: None,
    })
}
