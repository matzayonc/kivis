use crate::{
    BatchOp, BufferOp, Unifier, UnifierData,
    transaction::buffer::{BufferOpsContainer, DatabaseTransactionBuffer},
};

pub struct OpsIter<'a, KU: Unifier, VU: Unifier, C: BufferOpsContainer> {
    pub(super) transaction: &'a DatabaseTransactionBuffer<KU, VU, C>,
    pub(super) current_op: usize,
    pub(super) prev_key_end: usize,
    pub(super) prev_value_end: usize,
}

impl<'a, KU: Unifier, VU: Unifier, C: BufferOpsContainer> OpsIter<'a, KU, VU, C> {
    pub(crate) fn new(transaction: &'a DatabaseTransactionBuffer<KU, VU, C>) -> Self {
        Self {
            transaction,
            current_op: 0,
            prev_key_end: 0,
            prev_value_end: 0,
        }
    }
}

impl<'a, KU: Unifier, VU: Unifier, C: BufferOpsContainer> Iterator for OpsIter<'a, KU, VU, C> {
    type Item = BatchOp<'a, KU::D, VU::D>;

    fn next(&mut self) -> Option<Self::Item> {
        let op = self.current_op;
        self.current_op += 1;
        self.transaction
            .pending_ops
            .as_ref()
            .get(op)
            .map(|op| match op {
                BufferOp::Write { key_end, value_end } => {
                    let key = KU::D::extract_range(
                        &self.transaction.key_data,
                        self.prev_key_end,
                        *key_end,
                    );
                    let value = VU::D::extract_range(
                        &self.transaction.value_data,
                        self.prev_value_end,
                        *value_end,
                    );
                    self.prev_key_end = *key_end;
                    self.prev_value_end = *value_end;
                    crate::BatchOp::Insert { key, value }
                }
                BufferOp::Delete { key_end } => {
                    let key = KU::D::extract_range(
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
