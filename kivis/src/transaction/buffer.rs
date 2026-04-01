use bumpalo::{Bump, collections::Vec as BumpVec};
use core::marker::PhantomData;
use ouroboros::self_referencing;

use super::errors::TransactionError;
use crate::{BatchOp, Manifest, UnifierPair};

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

/// A lifetime-parameterized collection of `(PreBufferOps, M::Record<'a>)` pairs,
/// where `'a` is tied to the bump arena that owns the allocated records.
struct Records<'a, M: Manifest<U>, U: UnifierPair + 'static> {
    inner: BumpVec<'a, (PreBufferOps, M::Record<'a>)>,
    iter: Option<M::Iter<'a>>,
}

impl<'a, M: Manifest<U>, U: UnifierPair + 'static> Records<'a, M, U> {
    fn new(bump: &'a Bump) -> Self {
        Self {
            inner: BumpVec::new_in(bump),
            iter: None,
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

    /// Bump-allocates `record` and stores a reference-based enum variant pointing into it.
    pub(crate) fn push<'a, T: 'a>(&mut self, op: PreBufferOps, record: T)
    where
        for<'f> &'f T: Into<M::Record<'f>>,
        'a: 'static, // 'static bound is required by [`ouroboros`] to ensure the record can be safely stored in the arena
    {
        self.with_mut(|d| {
            let t: &T = d.bump.alloc(record);
            d.records.inner.push((op, t.into()));
        });
    }

    pub(crate) fn is_empty(&self) -> bool {
        let mut empty = false;
        self.with_records(|r| empty = r.inner.is_empty());
        empty
    }

    /// Consumes the buffer and returns a flat iterator of serialised [`BatchOp`]s.
    ///
    /// Each record's ops must be eagerly collected inside `with_records` while the arena
    /// borrow is still valid; the resulting `Vec` is independent of the arena.
    pub(crate) fn into_iter(
        self,
        unifiers: U,
    ) -> impl Iterator<Item = Result<BatchOp<U>, TransactionError<U>>> {
        TransactionBufferIterator {
            buffer: self,
            unifiers,
            index: 0,
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
    index: usize,
}

impl<M: Manifest<U>, U: UnifierPair + 'static> Iterator for TransactionBufferIterator<M, U> {
    type Item = Result<BatchOp<U>, TransactionError<U>>;

    fn next(&mut self) -> Option<Self::Item> {
        let unifiers = self.unifiers;
        let mut result = None;
        self.buffer.with_records_mut(|records| {
            // Drive the active iterator; fall through when it's exhausted or absent.
            if let Some(item) = records.iter.as_mut().and_then(Iterator::next) {
                result = Some(item);
                return;
            }

            if self.index >= records.inner.len() {
                // No more records to process, end the iteration.
                return;
            }

            // Load the iterator for the next record, or stop if there are none left.
            let (op, record) = records.inner[self.index];
            records.iter = Some(M::iter_ops(op, record, unifiers));
            self.index += 1;

            // Return the first item of the new iterator, if it exists.
            // No records should have empty ops.
            result = records.iter.as_mut().and_then(Iterator::next);
        });
        result
    }
}

#[derive(Debug, Clone, Copy)]
pub enum PreBufferOps {
    Insert,
    Put,
    Delete,
}
