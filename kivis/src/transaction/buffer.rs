use bumpalo::{Bump, collections::Vec as BumpVec};
use core::marker::PhantomData;
use ouroboros::self_referencing;

use crate::{Manifest, UnifierPair};

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

    // The reverse of iterator.
    // `into_iter` would be more ergonomic, but it would returning data with `this` lifetime, which is not possible with the current design of `ouroboros`.
    pub fn process<E, F>(self, mut f: F) -> Result<(), E>
    where
        F: for<'inner> FnMut(PreBufferOps, M::Record<'inner>) -> Result<(), E>,
    {
        let mut result = Ok(());
        self.with_records(|r| {
            for (op, record) in r {
                if result.is_ok() {
                    result = f(*op, *record);
                }
            }
        });
        result
    }
}

impl<M: Manifest<U> + 'static, U: UnifierPair + 'static> Default for TransactionBuffer<M, U> {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum PreBufferOps {
    Insert,
    Put,
    Delete,
}
