use crate::{BatchOp, Op, Unifier, UnifierData, transaction::buffer::DatabaseTransactionBuffer};

pub struct OpsIter<'a, U: Unifier> {
    pub(super) transaction: &'a DatabaseTransactionBuffer<U>,
    pub(super) current_op: usize,
    pub(super) prev_key_end: usize,
    pub(super) prev_value_end: usize,
}

impl<'a, U: Unifier> OpsIter<'a, U> {
    pub(crate) fn new(transaction: &'a DatabaseTransactionBuffer<U>) -> Self {
        Self {
            transaction,
            current_op: 0,
            prev_key_end: 0,
            prev_value_end: 0,
        }
    }
}

impl<'a, U: Unifier> Iterator for OpsIter<'a, U> {
    type Item = BatchOp<'a, U::K, U::V>;

    fn next(&mut self) -> Option<Self::Item> {
        let op = self.current_op;
        self.current_op += 1;
        self.transaction.pending_ops.get(op).map(|op| match op {
            Op::Write { key_end, value_end } => {
                let key =
                    U::K::extract_range(&self.transaction.key_data, self.prev_key_end, *key_end);
                let value = U::V::extract_range(
                    &self.transaction.value_data,
                    self.prev_value_end,
                    *value_end,
                );
                self.prev_key_end = *key_end;
                self.prev_value_end = *value_end;
                crate::BatchOp::Insert { key, value }
            }
            Op::Delete { key_end } => {
                let key =
                    U::K::extract_range(&self.transaction.key_data, self.prev_key_end, *key_end);
                self.prev_key_end = *key_end;
                crate::BatchOp::Delete { key }
            }
        })
    }
}
