use crate::{
    BufferOverflowOr, DatabaseEntry, Manifest, NoCache, OpsIter, RecordKey, Unifier, UnifierData,
    UnifierPair,
    transaction::pre_buffer::{BufferProcessor, PreBufferOps},
    wrap::{Subtable, WrapPrelude},
};

use super::errors::TransactionError;

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

impl<M: Manifest, U: UnifierPair, OpsContainer: BufferOpsContainer> BufferProcessor<M>
    for DatabaseTransactionBuffer<U, OpsContainer>
{
    type Error = TransactionError<U>;
    fn process<'outer, 'inner>(
        &mut self,
        op: PreBufferOps,
        record: &'outer M::Record<'inner>,
    ) -> Result<(), Self::Error>
    where
        'inner: 'outer,
    {
        M::process_record::<U, NoCache, OpsContainer>(self, op, record)
    }
}

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
    pub(crate) fn new(unifiers: U) -> Self {
        Self {
            pending_ops: OpsContainer::default(),
            key_data: <U::KeyUnifier as Unifier>::D::default(),
            value_data: <U::ValueUnifier as Unifier>::D::default(),
            key_serializer: unifiers.key_unifier(),
            value_serializer: unifiers.value_unifier(),
        }
    }

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
    #[doc(hidden)]
    pub fn prepare_writes<R: DatabaseEntry>(
        &mut self,
        record: R,
        key: &R::Key,
    ) -> Result<(), TransactionError<U>>
    where
        R::Key: RecordKey<Record = R>,
    {
        type KD<U> = <<U as UnifierPair>::KeyUnifier as Unifier>::D;
        type VD<U> = <<U as UnifierPair>::ValueUnifier as Unifier>::D;
        // Track serialized key hash and value positions, lazily initialized on first iteration
        let mut key_range: Option<(usize, usize)> = None;
        let mut key_value_range: Option<(usize, usize)> = None;

        let key_serializer = self.key_serializer();
        let value_serializer = self.value_serializer();
        for discriminator in 0..R::INDEX_COUNT_HINT {
            // Write index entry directly to buffers
            let mut prelude_buffer = KD::<U>::default();
            key_serializer.serialize(
                &mut prelude_buffer,
                WrapPrelude::new::<R>(Subtable::Index(discriminator)),
            )?;

            self.key_data
                .extend_from(prelude_buffer.as_view())
                .map_err(BufferOverflowOr::overflow)?;

            // Serialize the index key directly into the buffer
            record.index_key(&mut self.key_data, discriminator, &key_serializer)?;
            // Serialize key hash on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_range {
                // Reuse previously serialized key hash
                KD::<U>::duplicate_within(&mut self.key_data, start, end)
                    .map_err(BufferOverflowOr::overflow)?;
            } else {
                // First iteration: serialize key hash and save indices
                let start = KD::<U>::len(&self.key_data);
                key_serializer.serialize_ref(&mut self.key_data, key)?;
                let end = KD::<U>::len(&self.key_data);
                key_range = Some((start, end));
            }

            let key_end = KD::<U>::len(&self.key_data);

            // Serialize key value on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_value_range {
                // Reuse previously serialized key value
                VD::<U>::duplicate_within(&mut self.value_data, start, end)
                    .map_err(BufferOverflowOr::overflow)?;
            } else {
                // First iteration: serialize key value and save indices
                let start = VD::<U>::len(&self.value_data);
                value_serializer
                    .serialize_ref(&mut self.value_data, key)
                    .map_err(TransactionError::from_value)?;
                let end = VD::<U>::len(&self.value_data);
                key_value_range = Some((start, end));
            }

            let value_end = VD::<U>::len(&self.value_data);

            self.pending_ops
                .extend(core::iter::once(BufferOp::Write { key_end, value_end }));
        }

        // Write main record directly to buffers
        self.key_serializer()
            .serialize(&mut self.key_data, WrapPrelude::new::<R>(Subtable::Main))?;
        if let Some((start, end)) = key_range {
            // Reuse previously serialized key hash
            KD::<U>::duplicate_within(&mut self.key_data, start, end)
                .map_err(BufferOverflowOr::overflow)?;
        } else {
            key_serializer.serialize_ref(&mut self.key_data, key)?;
        }
        let key_end = KD::<U>::len(&self.key_data);

        self.value_serializer()
            .serialize(&mut self.value_data, record)
            .map_err(TransactionError::from_value)?;
        let value_end = VD::<U>::len(&self.value_data);

        self.pending_ops
            .extend(core::iter::once(BufferOp::Write { key_end, value_end }));

        Ok(())
    }

    /// Like [`prepare_writes`] but borrows the record, requiring `R: Clone`.
    #[doc(hidden)]
    pub fn prepare_writes_ref<R: DatabaseEntry + Clone>(
        &mut self,
        record: &R,
        key: &R::Key,
    ) -> Result<(), TransactionError<U>>
    where
        R::Key: RecordKey<Record = R>,
    {
        self.prepare_writes::<R>(record.clone(), key)
    }

    /// Dispatches to [`prepare_writes_ref`] or [`prepare_deletes`] based on `op`.
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
            PreBufferOps::Delete => self.prepare_deletes::<R>(record, key),
            PreBufferOps::Insert | PreBufferOps::Put => self.prepare_writes_ref::<R>(record, key),
        }
    }

    #[doc(hidden)]
    pub fn prepare_deletes<R: DatabaseEntry>(
        &mut self,
        record: &R,
        key: &R::Key,
    ) -> Result<(), TransactionError<U>>
    where
        R::Key: RecordKey<Record = R>,
    {
        type KD<UP> = <<UP as UnifierPair>::KeyUnifier as Unifier>::D;
        // Track serialized key position, lazily initialized on first iteration
        let mut key_bytes_range: Option<(usize, usize)> = None;

        let key_serializer = self.key_serializer();
        for discriminator in 0..R::INDEX_COUNT_HINT {
            // Write index delete key directly to buffer
            let mut prelude_buffer = KD::<U>::default();
            key_serializer.serialize(
                &mut prelude_buffer,
                WrapPrelude::new::<R>(Subtable::Index(discriminator)),
            )?;

            self.key_data
                .extend_from(prelude_buffer.as_view())
                .map_err(BufferOverflowOr::overflow)?;

            // Serialize the index key directly into the buffer
            record.index_key(&mut self.key_data, discriminator, &key_serializer)?;
            // Serialize key on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_bytes_range {
                // Reuse previously serialized key
                KD::<U>::duplicate_within(&mut self.key_data, start, end)
                    .map_err(BufferOverflowOr::overflow)?;
            } else {
                // First iteration: serialize key and save indices
                let start = KD::<U>::len(&self.key_data);
                key_serializer.serialize_ref(&mut self.key_data, key)?;
                let end = KD::<U>::len(&self.key_data);
                key_bytes_range = Some((start, end));
            }

            let key_end = KD::<U>::len(&self.key_data);
            self.pending_ops
                .extend(core::iter::once(BufferOp::Delete { key_end }));
        }

        // Delete main record - write directly to buffer
        self.key_serializer()
            .serialize(&mut self.key_data, WrapPrelude::new::<R>(Subtable::Main))?;
        if let Some((start, end)) = key_bytes_range {
            // Reuse previously serialized key
            KD::<U>::duplicate_within(&mut self.key_data, start, end)
                .map_err(BufferOverflowOr::overflow)?;
        } else {
            key_serializer.serialize_ref(&mut self.key_data, key)?;
        }
        let key_end = KD::<U>::len(&self.key_data);
        self.pending_ops
            .extend(core::iter::once(BufferOp::Delete { key_end }));

        Ok(())
    }
}
