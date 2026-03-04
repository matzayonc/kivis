use crate::{
    BufferOverflowOr, DatabaseEntry, OpsIter, RecordKey, Unifier, UnifierData, UnifierPair,
    transaction::buffer::PreBufferOps,
};

use super::{
    converter::{prepare_deletes, prepare_writes},
    errors::TransactionError,
};

#[derive(Clone, Copy)]
pub enum BufferOp {
    Write { key_end: usize, value_end: usize },
    Delete { key_end: usize },
}

/// Trait for containers that can hold transaction buffer operations.
///
/// This trait combines the necessary bounds for a container to be used
/// as the `OpsContainer` in transactions.
pub trait BufferOpsContainer: Default + Extend<BufferOp> + AsRef<[BufferOp]> {}

// Blanket implementation for any type that satisfies the bounds
impl<T> BufferOpsContainer for T where T: Default + Extend<BufferOp> + AsRef<[BufferOp]> {}

#[doc(hidden)]
pub struct DatabaseTransactionBuffer<U: UnifierPair, OpsContainer>
where
    OpsContainer: BufferOpsContainer,
{
    /// Pending operations: writes and deletes
    pub(super) pending_ops: OpsContainer,
    /// Key data buffer
    pub(super) key_data: <U::KeyUnifier as Unifier>::D,
    /// Value data buffer
    pub(super) value_data: <U::ValueUnifier as Unifier>::D,
    /// Key serialization configuration
    key_serializer: U::KeyUnifier,
    /// Value serialization configuration
    value_serializer: U::ValueUnifier,
}

impl<U, OpsContainer> DatabaseTransactionBuffer<U, OpsContainer>
where
    U: UnifierPair,
    OpsContainer: BufferOpsContainer,
{
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.pending_ops.as_ref().is_empty()
    }

    pub(crate) fn key_serializer(&self) -> U::KeyUnifier {
        self.key_serializer
    }

    pub(crate) fn value_serializer(&self) -> U::ValueUnifier {
        self.value_serializer
    }

    pub(crate) fn iter(&self) -> OpsIter<'_, U, OpsContainer> {
        OpsIter::new(self)
    }
}

impl<'a, U: UnifierPair, OpsContainer: BufferOpsContainer> IntoIterator
    for &'a DatabaseTransactionBuffer<U, OpsContainer>
{
    type Item = <OpsIter<'a, U, OpsContainer> as Iterator>::Item;
    type IntoIter = OpsIter<'a, U, OpsContainer>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<U, OpsContainer> DatabaseTransactionBuffer<U, OpsContainer>
where
    U: UnifierPair,
    OpsContainer: BufferOpsContainer,
{
    /// Serializes `record` and writes it (or deletes it) into the buffer based on `op`.
    #[doc(hidden)]
    pub fn prepare_record<R>(
        &mut self,
        op: PreBufferOps,
        record: &R,
        key: &R::Key,
    ) -> Result<(), TransactionError<U>>
    where
        R: DatabaseEntry + Clone,
        R::Key: RecordKey<Record = R>,
    {
        match op {
            PreBufferOps::Insert | PreBufferOps::Put => self.apply_writes(prepare_writes::<R, U>(
                record,
                key,
                self.key_serializer(),
                self.value_serializer(),
            )),
            PreBufferOps::Delete => {
                self.apply_deletes(prepare_deletes::<R, U>(record, key, self.key_serializer()))
            }
        }
    }

    fn apply_writes<WI>(&mut self, iter: WI) -> Result<(), TransactionError<U>>
    where
        WI: Iterator<
            Item = Result<
                (
                    <U::KeyUnifier as Unifier>::D,
                    <U::ValueUnifier as Unifier>::D,
                ),
                TransactionError<U>,
            >,
        >,
    {
        type KD<U> = <<U as UnifierPair>::KeyUnifier as Unifier>::D;
        type VD<U> = <<U as UnifierPair>::ValueUnifier as Unifier>::D;
        for result in iter {
            let (key_buf, value_buf) = result?;
            self.key_data
                .extend_from(key_buf.as_view())
                .map_err(BufferOverflowOr::overflow)?;
            let key_end = KD::<U>::len(&self.key_data);
            self.value_data
                .extend_from(value_buf.as_view())
                .map_err(BufferOverflowOr::overflow)?;
            let value_end = VD::<U>::len(&self.value_data);
            self.pending_ops
                .extend(core::iter::once(BufferOp::Write { key_end, value_end }));
        }
        Ok(())
    }

    fn apply_deletes<DI>(&mut self, iter: DI) -> Result<(), TransactionError<U>>
    where
        DI: Iterator<Item = Result<<U::KeyUnifier as Unifier>::D, TransactionError<U>>>,
    {
        type KD<U> = <<U as UnifierPair>::KeyUnifier as Unifier>::D;
        for key_buf in iter {
            let key_buf = key_buf?;
            self.key_data
                .extend_from(key_buf.as_view())
                .map_err(BufferOverflowOr::overflow)?;
            let key_end = KD::<U>::len(&self.key_data);
            self.pending_ops
                .extend(core::iter::once(BufferOp::Delete { key_end }));
        }
        Ok(())
    }
}

/// Re-export of [`PreBufferOps`] under a public-facing name.
pub type RecordOpKind = PreBufferOps;
