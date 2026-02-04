use crate::{
    BufferOverflowOr, DatabaseEntry, OpsIter, RecordKey, Unifier, UnifierData,
    wrap::{Subtable, WrapPrelude},
};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::errors::TransactionError;

pub enum Op {
    Write { key_end: usize, value_end: usize },
    Delete { key_end: usize },
}

pub(crate) struct DatabaseTransactionBuffer<KU: Unifier, VU: Unifier> {
    /// Pending operations: writes and deletes
    pub(super) pending_ops: Vec<Op>,
    /// Key data buffer
    pub(super) key_data: <KU::D as UnifierData>::Buffer,
    /// Value data buffer
    pub(super) value_data: <VU::D as UnifierData>::Buffer,
    /// Key serialization configuration
    key_serializer: KU,
    /// Value serialization configuration
    value_serializer: VU,
}

impl<KU: Unifier + Copy, VU: Unifier + Copy> DatabaseTransactionBuffer<KU, VU> {
    pub(crate) fn new(key_serializer: KU, value_serializer: VU) -> Self {
        Self {
            pending_ops: Vec::new(),
            key_data: <KU::D as UnifierData>::Buffer::default(),
            value_data: <VU::D as UnifierData>::Buffer::default(),
            key_serializer,
            value_serializer,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending_ops.is_empty()
    }

    pub(crate) fn key_serializer(&self) -> KU {
        self.key_serializer
    }

    pub(crate) fn value_serializer(&self) -> VU {
        self.value_serializer
    }

    pub(crate) fn iter(&self) -> OpsIter<'_, KU, VU> {
        OpsIter::new(self)
    }

    pub(crate) fn prepare_writes<R: DatabaseEntry>(
        &mut self,
        record: R,
        key: &R::Key,
    ) -> Result<(), TransactionError<KU::SerError, VU::SerError>>
    where
        R::Key: RecordKey<Record = R>,
    {
        // Track serialized key hash and value positions, lazily initialized on first iteration
        let mut key_range: Option<(usize, usize)> = None;
        let mut key_value_range: Option<(usize, usize)> = None;

        let key_serializer = self.key_serializer();
        let value_serializer = self.value_serializer();
        for discriminator in 0..R::INDEX_COUNT_HINT {
            // Write index entry directly to buffers
            let mut prelude_buffer = <KU::D as UnifierData>::Buffer::default();
            key_serializer.serialize(
                &mut prelude_buffer,
                WrapPrelude::new::<R>(Subtable::Index(discriminator)),
            )?;

            KU::D::extend(&mut self.key_data, prelude_buffer.as_ref())
                .map_err(BufferOverflowOr::overflow)?;

            // Serialize the index key directly into the buffer
            record.index_key(&mut self.key_data, discriminator, &key_serializer)?;
            // Serialize key hash on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_range {
                // Reuse previously serialized key hash
                KU::D::duplicate_within(&mut self.key_data, start, end)
                    .map_err(BufferOverflowOr::overflow)?;
            } else {
                // First iteration: serialize key hash and save indices
                let start = KU::D::len(&self.key_data);
                key_serializer.serialize_ref(&mut self.key_data, key)?;
                let end = KU::D::len(&self.key_data);
                key_range = Some((start, end));
            }

            let key_end = KU::D::len(&self.key_data);

            // Serialize key value on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_value_range {
                // Reuse previously serialized key value
                VU::D::duplicate_within(&mut self.value_data, start, end)
                    .map_err(BufferOverflowOr::overflow)?;
            } else {
                // First iteration: serialize key value and save indices
                let start = VU::D::len(&self.value_data);
                value_serializer
                    .serialize_ref(&mut self.value_data, key)
                    .map_err(TransactionError::from_value)?;
                let end = VU::D::len(&self.value_data);
                key_value_range = Some((start, end));
            }

            let value_end = VU::D::len(&self.value_data);

            self.pending_ops.push(Op::Write { key_end, value_end });
        }

        // Write main record directly to buffers
        self.key_serializer()
            .serialize(&mut self.key_data, WrapPrelude::new::<R>(Subtable::Main))?;
        if let Some((start, end)) = key_range {
            // Reuse previously serialized key hash
            KU::D::duplicate_within(&mut self.key_data, start, end)
                .map_err(BufferOverflowOr::overflow)?;
        } else {
            key_serializer.serialize_ref(&mut self.key_data, key)?;
        }
        let key_end = KU::D::len(&self.key_data);

        self.value_serializer()
            .serialize(&mut self.value_data, record)
            .map_err(TransactionError::from_value)?;
        let value_end = VU::D::len(&self.value_data);

        self.pending_ops.push(Op::Write { key_end, value_end });

        Ok(())
    }

    pub(crate) fn prepare_deletes<R: DatabaseEntry>(
        &mut self,
        record: &R,
        key: &R::Key,
    ) -> Result<(), TransactionError<KU::SerError, VU::SerError>>
    where
        R::Key: RecordKey<Record = R>,
    {
        // Track serialized key position, lazily initialized on first iteration
        let mut key_bytes_range: Option<(usize, usize)> = None;

        let key_serializer = self.key_serializer();
        for discriminator in 0..R::INDEX_COUNT_HINT {
            // Write index delete key directly to buffer
            let mut prelude_buffer = <KU::D as UnifierData>::Buffer::default();
            key_serializer.serialize(
                &mut prelude_buffer,
                WrapPrelude::new::<R>(Subtable::Index(discriminator)),
            )?;

            KU::D::extend(&mut self.key_data, prelude_buffer.as_ref())
                .map_err(BufferOverflowOr::overflow)?;

            // Serialize the index key directly into the buffer
            record.index_key(&mut self.key_data, discriminator, &key_serializer)?;
            // Serialize key on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_bytes_range {
                // Reuse previously serialized key
                KU::D::duplicate_within(&mut self.key_data, start, end)
                    .map_err(BufferOverflowOr::overflow)?;
            } else {
                // First iteration: serialize key and save indices
                let start = KU::D::len(&self.key_data);
                key_serializer.serialize_ref(&mut self.key_data, key)?;
                let end = KU::D::len(&self.key_data);
                key_bytes_range = Some((start, end));
            }

            let key_end = KU::D::len(&self.key_data);
            self.pending_ops.push(Op::Delete { key_end });
        }

        // Delete main record - write directly to buffer
        self.key_serializer()
            .serialize(&mut self.key_data, WrapPrelude::new::<R>(Subtable::Main))?;
        if let Some((start, end)) = key_bytes_range {
            // Reuse previously serialized key
            KU::D::duplicate_within(&mut self.key_data, start, end)
                .map_err(BufferOverflowOr::overflow)?;
        } else {
            key_serializer.serialize_ref(&mut self.key_data, key)?;
        }
        let key_end = KU::D::len(&self.key_data);
        self.pending_ops.push(Op::Delete { key_end });

        Ok(())
    }
}
