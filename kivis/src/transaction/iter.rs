use crate::{
    BatchOp, BufferOp, Unifier, UnifierData, UnifierPair,
    transaction::buffer::{BufferOpsContainer, DatabaseTransactionBuffer},
};

pub struct OpsIter<'a, U: UnifierPair, C: BufferOpsContainer> {
    pub(super) transaction: &'a DatabaseTransactionBuffer<U, C>,
    pub(super) current_op: usize,
    pub(super) prev_key_end: usize,
    pub(super) prev_value_end: usize,
}

impl<'a, U: UnifierPair, C: BufferOpsContainer> OpsIter<'a, U, C> {
    pub(crate) fn new(transaction: &'a DatabaseTransactionBuffer<U, C>) -> Self {
        Self {
            transaction,
            current_op: 0,
            prev_key_end: 0,
            prev_value_end: 0,
        }
    }
}

impl<'a, U: UnifierPair, C: BufferOpsContainer> Iterator for OpsIter<'a, U, C> {
    type Item = BatchOp<'a, <U::KeyUnifier as Unifier>::D, <U::ValueUnifier as Unifier>::D>;

    fn next(&mut self) -> Option<Self::Item> {
        let op = self.current_op;
        self.current_op += 1;
        self.transaction
            .pending_ops
            .as_ref()
            .get(op)
            .map(|op| match op {
                BufferOp::Write { key_end, value_end } => {
                    let key = <U::KeyUnifier as Unifier>::D::extract_range(
                        &self.transaction.key_data,
                        self.prev_key_end,
                        *key_end,
                    );
                    let value = <U::ValueUnifier as Unifier>::D::extract_range(
                        &self.transaction.value_data,
                        self.prev_value_end,
                        *value_end,
                    );
                    self.prev_key_end = *key_end;
                    self.prev_value_end = *value_end;
                    crate::BatchOp::Insert { key, value }
                }
                BufferOp::Delete { key_end } => {
                    let key = <U::KeyUnifier as Unifier>::D::extract_range(
                        &self.transaction.key_data,
                        self.prev_key_end,
                        *key_end,
                    );
                    self.prev_key_end = *key_end;
                    crate::BatchOp::Delete { key }
                }
            })
    }
}
