use bumpalo::{Bump, collections::Vec as BumpVec};
use core::marker::PhantomData;
use ouroboros::self_referencing;

use super::errors::TransactionError;
use crate::{BatchOp, Manifest, UnifierPair};

/// A pre-transaction buffer that uses a bump allocator for fast, arena-based allocation.
///
/// Records are bump-allocated (stable address, never moved). The `records` vec holds
/// `M::Record<'this>` — variants containing `&'this T` references into `bump` —
/// where `'this` is the ouroboros lifetime of the arena itself.
#[self_referencing]
pub(crate) struct TransactionBuffer<M: Manifest<U>, U: UnifierPair + 'static> {
    bump: Bump,
    /// Anchors `M` and `U` in the generated struct; `M` only appears via GAT in `records`
    /// so without this field ouroboros can't see the type parameters.
    phantom: PhantomData<(M, U)>,
    #[borrows(bump)]
    #[not_covariant]
    records: BumpVec<'this, (PreBufferOps, M::Record<'this>)>,
}

impl<M: Manifest<U>, U: UnifierPair + 'static> TransactionBuffer<M, U> {
    pub(crate) fn empty() -> Self {
        TransactionBufferBuilder {
            bump: Bump::new(),
            phantom: PhantomData,
            records_builder: |bump| BumpVec::new_in(bump),
        }
        .build()
    }

    /// Bump-allocates `record` and stores a reference-based enum variant pointing into it.
    ///
    pub(crate) fn push<'a, T: 'a>(&mut self, op: PreBufferOps, record: T)
    where
        for<'f> &'f T: Into<M::Record<'f>>,
        'a: 'static, // 'static bounds is required by [`ouroboros`] to ensure the record can be safely stored in the arena
    {
        // There is no way to pass a seconds argument to a function, and to way to modify the generic arguments of the closure.
        // The only requirement for the inner function is that T lives as long as the the 3rd lifetime ( T: 'c ).
        self.with_mut(|d| {
            let t: &T = d.bump.alloc(record);
            d.records.push((op, t.into()));
        });
    }

    pub(crate) fn is_empty(&self) -> bool {
        let mut empty = false;
        self.with_records(|r| empty = r.is_empty());
        empty
    }

    /// Consumes the buffer and returns a flat iterator of serialised [`BatchOp`]s.
    ///
    /// `M::Iter<'b>` borrows from `record`, which lives inside the bump arena. The arena
    /// is owned by `self` and dropped once `with_records` returns, so each record's ops
    /// must be eagerly collected inside the callback while the borrow is still valid.
    /// The resulting `Vec` owns all ops and is independent of the arena.
    pub(crate) fn into_iter(
        self,
        unifiers: U,
    ) -> impl Iterator<Item = Result<BatchOp<U>, TransactionError<U>>> {
        TransactionBufferIterator::new(self, unifiers)
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
    subindex: usize,
}

impl<M: Manifest<U>, U: UnifierPair + 'static> TransactionBufferIterator<M, U> {
    fn new(buffer: TransactionBuffer<M, U>, unifiers: U) -> Self {
        Self {
            buffer,
            unifiers,
            index: 0,
            subindex: 0,
        }
    }
}

impl<M: Manifest<U>, U: UnifierPair + 'static> Iterator for TransactionBufferIterator<M, U> {
    type Item = Result<BatchOp<U>, TransactionError<U>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut all_ops = Vec::new();
        let unifiers = self.unifiers;
        self.buffer.with_records(|records| {
            if let Some(&(op, record)) = records.get(self.index) {
                all_ops.extend(M::iter_ops(op, &record, unifiers));
            }
        });
        if all_ops.is_empty() {
            return None;
        }

        if self.subindex < all_ops.len() {
            let r = all_ops.remove(self.subindex);
            self.subindex += 1;
            Some(r)
        } else {
            self.index += 1;
            self.subindex = 0;
            self.next()
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum PreBufferOps {
    Insert,
    Put,
    Delete,
}
