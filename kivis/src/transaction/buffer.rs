use bumpalo::{Bump, collections::Vec as BumpVec};
use core::marker::PhantomData;
use ouroboros::self_referencing;

use super::errors::TransactionError;
use crate::{BatchOp, Manifest, UnifierPair};

#[derive(Debug, Clone, Copy)]
pub enum PreBufferOps {
    Insert,
    Put,
    Delete,
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

/// A lifetime-parameterized collection of `(PreBufferOps, M::Record<'a>)` pairs,
/// where `'a` is tied to the bump arena that owns the allocated records.
enum Records<'a, M: Manifest<U>, U: UnifierPair + 'static> {
    /// Still accepting pushed records.
    Collecting(BumpVec<'a, (PreBufferOps, M::Record<'a>)>),
    /// Draining the vec, optionally mid-way through a record's op iterator.
    Iterating {
        inner_iter: bumpalo::collections::vec::IntoIter<'a, (PreBufferOps, M::Record<'a>)>,
        iter: Option<M::Iter<'a>>,
    },
    /// All records have been consumed.
    Done,
}

impl<'a, M: Manifest<U>, U: UnifierPair + 'static> Records<'a, M, U> {
    fn new(bump: &'a Bump) -> Self {
        Self::Collecting(BumpVec::new_in(bump))
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
        self.buffer.with_records_mut(|records| {
            // Drive the active iterator; fall through when it's exhausted or absent.
            if let Records::Iterating {
                iter: Some(iter), ..
            } = records
                && let Some(item) = iter.next()
            {
                result = Some(item);
                return;
            }

            // Transition Collecting -> Iterating on the first call.
            if let Records::Collecting(_) = records {
                let old = core::mem::replace(records, Records::Done);
                if let Records::Collecting(vec) = old {
                    *records = Records::Iterating {
                        inner_iter: vec.into_iter(),
                        iter: None,
                    };
                }
            }

            // Advance to the next record.
            if let Records::Iterating { inner_iter, iter } = records {
                let Some((op, record)) = inner_iter.next() else {
                    *records = Records::Done;
                    return;
                };
                *iter = Some(M::iter_ops(op, record, unifiers));
                result = iter.as_mut().and_then(Iterator::next);
            }
        });
        result
    }
}
