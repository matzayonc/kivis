use bumpalo::{Bump, collections::Vec as BumpVec};
use core::marker::PhantomData;
use ouroboros::self_referencing;

use super::errors::TransactionError;
use crate::{
    BatchOp, CacheExpiry, DatabaseError, Manifest, Repository, Storage, Unified, Unifier,
    UnifierPair,
};

#[derive(Debug, Clone, Copy)]
pub enum PreBufferOps {
    Insert,
    Put,
    Delete,
    /// Deletes only the index entries of a record, leaving the main entry untouched.
    /// Emitted internally for the previous version of an overwritten record.
    DeleteIndexes,
}

impl PreBufferOps {
    fn writes(self) -> bool {
        matches!(self, Self::Insert | Self::Put)
    }
}

/// A pre-transaction buffer that uses a bump allocator for fast, arena-based allocation.
///
/// `Records<'this, M, U>` borrows from the internal `Bump` arena via the ouroboros `'this` lifetime.
#[self_referencing]
pub(crate) struct TransactionBuffer<M: Manifest<U>, U: UnifierPair + 'static> {
    bump: Bump,
    /// Anchors `M` and `U` in the generated struct; `M` only appears via GAT in `records`
    /// so without this field ouroboros can't see the type parameters.
    phantom: PhantomData<(M, U)>,
    #[borrows(bump)]
    #[not_covariant]
    records: Records<'this, M, U>,
}

type StaleOps<'a, M, U> =
    bumpalo::collections::vec::IntoIter<'a, Option<<M as Manifest<U>>::Iter<'a>>>;

/// A lifetime-parameterized collection of `(PreBufferOps, M::Record<'a>)` pairs,
/// where `'a` is tied to the bump arena that owns the allocated records.
enum Records<'a, M: Manifest<U>, U: UnifierPair + 'static> {
    /// Still accepting pushed records.
    Collecting(BumpVec<'a, (PreBufferOps, M::Record<'a>)>),
    /// Draining the vec, optionally mid-way through a record's op iterator.
    Iterating {
        inner_iter: bumpalo::collections::vec::IntoIter<'a, (PreBufferOps, M::Record<'a>)>,
        /// One entry per record in `inner_iter`: the ops that delete the stale index entries of
        /// the version this record overwrites, if any. Empty when the buffer was not resolved
        /// against storage.
        stale: StaleOps<'a, M, U>,
        /// The currently active op iterator.
        iter: Option<M::Iter<'a>>,
        /// The record's own ops, queued while its stale-index deletes are being drained.
        pending: Option<M::Iter<'a>>,
    },
    /// All records have been consumed.
    Done,
}

impl<'a, M: Manifest<U>, U: UnifierPair + 'static> Records<'a, M, U> {
    fn new(bump: &'a Bump) -> Self {
        Self::Collecting(BumpVec::new_in(bump))
    }

    /// Transitions `Collecting` -> `Iterating`, attaching per-record stale-index ops.
    fn start_iterating(&mut self, bump: &'a Bump, stale: Option<BumpVec<'a, Option<M::Iter<'a>>>>) {
        if let Records::Collecting(_) = self {
            let old = core::mem::replace(self, Records::Done);
            if let Records::Collecting(vec) = old {
                *self = Records::Iterating {
                    inner_iter: vec.into_iter(),
                    stale: stale.unwrap_or_else(|| BumpVec::new_in(bump)).into_iter(),
                    iter: None,
                    pending: None,
                };
            }
        }
    }
}

impl<M: Manifest<U>, U: UnifierPair + 'static> TransactionBuffer<M, U> {
    pub(crate) fn empty() -> Self {
        TransactionBufferBuilder {
            bump: Bump::new(),
            phantom: PhantomData,
            records_builder: |bump| Records::new(bump),
        }
        .build()
    }

    /// Bump-allocates `record` into the arena and appends it to the collecting vec.
    pub(crate) fn push<'a, T: 'a>(&mut self, op: PreBufferOps, record: T)
    where
        for<'f> &'f T: Into<M::Record<'f>>,
        'a: 'static,
    {
        self.with_mut(|d| {
            let t: &T = d.bump.alloc(record);
            if let Records::Collecting(vec) = d.records {
                vec.push((op, t.into()));
            }
        });
    }

    pub(crate) fn is_empty(&self) -> bool {
        let mut empty = false;
        self.with_records(|r| empty = matches!(r, Records::Collecting(v) if v.is_empty()));
        empty
    }

    /// Looks up, for every buffered write, the version of the record it overwrites and queues
    /// deletes for that version's index entries ahead of the write.
    ///
    /// The previous version is the last write to the same key earlier in this buffer if there is
    /// one (or nothing, if the key was deleted earlier in this buffer), otherwise the value
    /// currently held by `repo`.
    ///
    /// Must be called at most once, before [`into_iter`](Self::into_iter).
    ///
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if a key cannot be serialised, if reading from `repo` fails, or
    /// if a stored previous version cannot be deserialised.
    pub(crate) fn resolve_previous<S>(
        &mut self,
        repo: &S::Repo,
        unifiers: U,
    ) -> Result<(), DatabaseError<S>>
    where
        S: Storage<Unifiers = U>,
    {
        let mut result = Ok(());
        self.with_mut(|d| {
            let Records::Collecting(records) = &*d.records else {
                return;
            };

            let mut keys: BumpVec<'_, <U::KeyUnifier as Unifier>::D> =
                BumpVec::with_capacity_in(records.len(), d.bump);
            for (_, record) in records {
                match M::main_key(*record, unifiers) {
                    Ok(key) => keys.push(key),
                    Err(e) => {
                        result = Err(DatabaseError::from_transaction_error(e));
                        return;
                    }
                }
            }

            let mut stale: BumpVec<'_, Option<M::Iter<'_>>> =
                BumpVec::with_capacity_in(records.len(), d.bump);
            for (i, (op, record)) in records.iter().enumerate() {
                if !op.writes() {
                    stale.push(None);
                    continue;
                }

                // An earlier write to the same key in this buffer is the version being overwritten;
                // an earlier delete means there is nothing left to clean up.
                let earlier = records[..i]
                    .iter()
                    .zip(keys[..i].iter())
                    .rposition(|(_, k)| *k == keys[i]);
                if let Some(j) = earlier {
                    let (prev_op, prev_record) = records[j];
                    stale.push(
                        prev_op.writes().then(|| {
                            M::iter_ops(PreBufferOps::DeleteIndexes, prev_record, unifiers)
                        }),
                    );
                    continue;
                }

                let previous = match repo.get_entry(keys[i].as_view()) {
                    Ok(previous) => previous,
                    Err(e) => {
                        result = Err(DatabaseError::Storage(e));
                        return;
                    }
                };
                let Some(previous) = previous else {
                    stale.push(None);
                    continue;
                };
                match M::stale_index_ops(*record, &previous, unifiers) {
                    Ok(ops) => stale.push(Some(ops)),
                    Err(e) => {
                        result = Err(DatabaseError::ValueDeserialization(e));
                        return;
                    }
                }
            }

            d.records.start_iterating(d.bump, Some(stale));
        });
        result
    }

    /// Expires the cached entry of every buffered record, whatever its operation: a write and a
    /// delete both invalidate the cached value.
    ///
    /// Must be called before [`into_iter`](Self::into_iter) or
    /// [`resolve_previous`](Self::resolve_previous) drain the buffer; afterwards it does nothing.
    pub(crate) fn expire_cached<C>(&self, cache: &mut C)
    where
        C: CacheExpiry<M, U>,
    {
        self.with_records(|records| {
            if let Records::Collecting(vec) = records {
                for (_, record) in vec {
                    cache.expire_record(*record);
                }
            }
        });
    }

    /// Consumes the buffer and returns a flat iterator of serialised [`BatchOp`]s.
    pub(crate) fn into_iter(
        self,
        unifiers: U,
    ) -> impl Iterator<Item = Result<BatchOp<U>, TransactionError<U>>> {
        TransactionBufferIterator {
            buffer: self,
            unifiers,
        }
    }
}

impl<M: Manifest<U> + 'static, U: UnifierPair + 'static> Default for TransactionBuffer<M, U> {
    fn default() -> Self {
        Self::empty()
    }
}

struct TransactionBufferIterator<M: Manifest<U>, U: UnifierPair + 'static> {
    buffer: TransactionBuffer<M, U>,
    unifiers: U,
}

impl<M: Manifest<U>, U: UnifierPair + 'static> Iterator for TransactionBufferIterator<M, U> {
    type Item = Result<BatchOp<U>, TransactionError<U>>;

    fn next(&mut self) -> Option<Self::Item> {
        let unifiers = self.unifiers;
        let mut result = None;
        self.buffer.with_mut(|d| {
            d.records.start_iterating(d.bump, None);

            let Records::Iterating {
                inner_iter,
                stale,
                iter,
                pending,
            } = d.records
            else {
                return;
            };

            loop {
                // Drive the active iterator; fall through when it's exhausted or absent.
                if let Some(item) = iter.as_mut().and_then(Iterator::next) {
                    result = Some(item);
                    return;
                }

                // The stale-index deletes are done; continue with the record's own ops.
                if let Some(ops) = pending.take() {
                    *iter = Some(ops);
                    continue;
                }

                // Advance to the next record, its stale-index deletes first.
                let Some((op, record)) = inner_iter.next() else {
                    *d.records = Records::Done;
                    return;
                };
                let ops = M::iter_ops(op, record, unifiers);
                match stale.next().flatten() {
                    Some(stale_ops) => {
                        *iter = Some(stale_ops);
                        *pending = Some(ops);
                    }
                    None => *iter = Some(ops),
                }
            }
        });
        result
    }
}
