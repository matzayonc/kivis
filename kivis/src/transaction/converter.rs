use std::cmp::Ordering;

use crate::{
    BatchOp, BufferOverflowOr, DatabaseEntry, RecordKey, Unifier, UnifierData, UnifierPair,
    transaction::buffer::PreBufferOps,
    wrap::{Subtable, WrapPrelude},
};

use super::errors::TransactionError;

/// Concrete iterator of [`BatchOp`]s for a single record write or delete.
///
/// Yields index entries first (one per `INDEX_COUNT_HINT`), then the main record entry.
pub struct RecordOps<'r, R: DatabaseEntry, U: UnifierPair> {
    record: &'r R,
    key: &'r R::Key,
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
                Some(self.main_op())
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
        self.key_unifier.serialize(&mut kb, self.key)?;
        Ok(self.cached_key.insert(kb).clone())
    }

    fn cached_key_value(&mut self) -> Result<<U::ValueUnifier as Unifier>::D, TransactionError<U>> {
        if let Some(v) = &self.cached_key_value {
            return Ok(v.clone());
        }
        let mut kv = <U::ValueUnifier as Unifier>::D::default();
        self.value_unifier
            .serialize(&mut kv, self.key)
            .map_err(TransactionError::from_value)?;
        Ok(self.cached_key_value.insert(kv).clone())
    }

    fn index_op(&mut self, discriminator: u8) -> Result<BatchOp<U>, TransactionError<U>> {
        let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
        self.key_unifier.serialize(
            &mut key_buf,
            &WrapPrelude::new::<R>(Subtable::Index(discriminator)),
        )?;
        self.record
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
            PreBufferOps::Delete => Ok(BatchOp::Delete { key: key_buf }),
        }
    }

    fn main_op(&mut self) -> Result<BatchOp<U>, TransactionError<U>> {
        let mut key_buf = <U::KeyUnifier as Unifier>::D::default();
        self.key_unifier
            .serialize(&mut key_buf, &WrapPrelude::new::<R>(Subtable::Main))?;
        self.key_unifier.serialize(&mut key_buf, self.key)?;
        match self.op {
            PreBufferOps::Insert | PreBufferOps::Put => {
                let mut value_buf = <U::ValueUnifier as Unifier>::D::default();
                self.value_unifier
                    .serialize(&mut value_buf, self.record)
                    .map_err(TransactionError::from_value)?;
                Ok(BatchOp::Insert {
                    key: key_buf,
                    value: value_buf,
                })
            }
            PreBufferOps::Delete => Ok(BatchOp::Delete { key: key_buf }),
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
    R: DatabaseEntry,
    R::Key: RecordKey<Record = R>,
    U: UnifierPair,
{
    RecordOps {
        record,
        key,
        key_unifier: unifiers.key_unifier(),
        value_unifier: unifiers.value_unifier(),
        op,
        discriminator: 0,
        cached_key: None,
        cached_key_value: None,
    }
}
