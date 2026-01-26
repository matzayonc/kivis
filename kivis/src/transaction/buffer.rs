use crate::{
    DatabaseEntry, IndexBuilder, Indexer, RecordKey, Unifier, UnifierData,
    wrap::{Subtable, WrapPrelude, wrap},
};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::iter::OpsIter;

pub enum Op {
    Write { key_end: usize, value_end: usize },
    Delete { key_end: usize },
}

pub(crate) struct DatabaseTransactionBuffer<U: Unifier> {
    /// Pending operations: writes and deletes
    pub(super) pending_ops: Vec<Op>,
    /// Key data buffer
    pub(super) key_data: <U::K as UnifierData>::Owned,
    /// Value data buffer
    pub(super) value_data: <U::V as UnifierData>::Owned,
    /// Serialization configuration
    serializer: U,
}

impl<U: Unifier + Copy> DatabaseTransactionBuffer<U> {
    pub(crate) fn new(serializer: U) -> Self {
        Self {
            pending_ops: Vec::new(),
            key_data: <U::K as UnifierData>::Owned::default(),
            value_data: <U::V as UnifierData>::Owned::default(),
            serializer,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending_ops.is_empty()
    }

    pub(crate) fn serializer(&self) -> U {
        self.serializer
    }

    pub(crate) fn iter(&self) -> OpsIter<'_, U> {
        OpsIter::new(self)
    }

    pub(crate) fn prepare_writes<R: DatabaseEntry>(
        &mut self,
        record: R,
        key: &R::Key,
    ) -> Result<(), U::SerError>
    where
        R::Key: RecordKey<Record = R>,
        IndexBuilder<U>: Indexer<Error = U::SerError>,
    {
        let mut indexer = IndexBuilder::new(self.serializer());
        record.index_keys(&mut indexer)?;

        // Track serialized key hash and value positions, lazily initialized on first iteration
        let mut key_hash_range: Option<(usize, usize)> = None;
        let mut key_value_range: Option<(usize, usize)> = None;

        for (discriminator, index_key) in indexer.into_index_keys() {
            // Write index entry directly to buffers
            let mut prelude_buffer = <U::K as UnifierData>::Owned::default();
            self.serializer().serialize_key(
                &mut prelude_buffer,
                WrapPrelude::new::<R>(Subtable::Index(discriminator)),
            )?;

            U::K::extend(&mut self.key_data, prelude_buffer.as_ref());
            U::K::extend(&mut self.key_data, index_key.as_ref());

            // Serialize key hash on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_hash_range {
                // Reuse previously serialized key hash
                let key_hash = U::K::extract_range(&self.key_data, start, end);
                let key_hash_owned = U::K::to_owned(key_hash);
                U::K::extend(&mut self.key_data, key_hash_owned.as_ref());
            } else {
                // First iteration: serialize key hash and save indices
                let start = U::K::len(&self.key_data);
                self.serializer()
                    .serialize_key_ref(&mut self.key_data, key)?;
                let end = U::K::len(&self.key_data);
                key_hash_range = Some((start, end));
            }

            let key_end = U::K::len(&self.key_data);

            // Serialize key value on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_value_range {
                // Reuse previously serialized key value
                let key_value = U::V::extract_range(&self.value_data, start, end);
                let key_value_owned = U::V::to_owned(key_value);
                U::V::extend(&mut self.value_data, key_value_owned.as_ref());
            } else {
                // First iteration: serialize key value and save indices
                let start = U::V::len(&self.value_data);
                self.serializer()
                    .serialize_value_ref(&mut self.value_data, key)?;
                let end = U::V::len(&self.value_data);
                key_value_range = Some((start, end));
            }

            let value_end = U::V::len(&self.value_data);

            self.pending_ops.push(Op::Write { key_end, value_end });
        }

        // Write main record directly to buffers
        wrap::<R, U>(key, &self.serializer(), &mut self.key_data)?;
        let key_end = U::K::len(&self.key_data);

        self.serializer()
            .serialize_value(&mut self.value_data, record)?;
        let value_end = U::V::len(&self.value_data);

        self.pending_ops.push(Op::Write { key_end, value_end });

        Ok(())
    }

    pub(crate) fn prepare_deletes<R: DatabaseEntry>(
        &mut self,
        record: &R,
        key: &R::Key,
    ) -> Result<(), U::SerError>
    where
        R::Key: RecordKey<Record = R>,
        IndexBuilder<U>: Indexer<Error = U::SerError>,
    {
        let mut indexer = IndexBuilder::new(self.serializer());
        record.index_keys(&mut indexer)?;

        let index_keys = indexer.into_index_keys();

        // Track serialized key position, lazily initialized on first iteration
        let mut key_bytes_range: Option<(usize, usize)> = None;

        for (discriminator, index_key) in index_keys {
            // Write index delete key directly to buffer
            let mut prelude_buffer = <U::K as UnifierData>::Owned::default();
            self.serializer().serialize_key(
                &mut prelude_buffer,
                WrapPrelude::new::<R>(Subtable::Index(discriminator)),
            )?;

            U::K::extend(&mut self.key_data, prelude_buffer.as_ref());
            U::K::extend(&mut self.key_data, index_key.as_ref());

            // Serialize key on first iteration or reuse from previous iterations
            if let Some((start, end)) = key_bytes_range {
                // Reuse previously serialized key
                let key_bytes = U::K::extract_range(&self.key_data, start, end);
                let key_bytes_owned = U::K::to_owned(key_bytes);
                U::K::extend(&mut self.key_data, key_bytes_owned.as_ref());
            } else {
                // First iteration: serialize key and save indices
                let start = U::K::len(&self.key_data);
                self.serializer()
                    .serialize_key_ref(&mut self.key_data, key)?;
                let end = U::K::len(&self.key_data);
                key_bytes_range = Some((start, end));
            }

            let key_end = U::K::len(&self.key_data);
            self.pending_ops.push(Op::Delete { key_end });
        }

        // Delete main record - write directly to buffer
        // TODO: Use directly
        wrap::<R, _>(key, &self.serializer(), &mut self.key_data)?;
        let key_end = U::K::len(&self.key_data);
        self.pending_ops.push(Op::Delete { key_end });

        Ok(())
    }
}
