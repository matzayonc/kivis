use crate::{BatchOp, Op, Unifier, UnifierData, transaction::buffer::DatabaseTransactionBuffer};

pub struct OpsIter<'a, KU: Unifier, VU: Unifier> {
    pub(super) transaction: &'a DatabaseTransactionBuffer<KU, VU>,
    pub(super) current_op: usize,
    pub(super) prev_key_end: usize,
    pub(super) prev_value_end: usize,
}

impl<'a, KU: Unifier, VU: Unifier> OpsIter<'a, KU, VU> {
    pub(crate) fn new(transaction: &'a DatabaseTransactionBuffer<KU, VU>) -> Self {
        Self {
            transaction,
            current_op: 0,
            prev_key_end: 0,
            prev_value_end: 0,
        }
    }
}

impl<'a, KU: Unifier, VU: Unifier> Iterator for OpsIter<'a, KU, VU> {
    type Item = BatchOp<'a, KU::D, VU::D>;

    fn next(&mut self) -> Option<Self::Item> {
        let op = self.current_op;
        self.current_op += 1;
        self.transaction.pending_ops.get(op).map(|op| match op {
            Op::Write { key_end, value_end } => {
                let key =
                    KU::D::extract_range(&self.transaction.key_data, self.prev_key_end, *key_end);
                let value = VU::D::extract_range(
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
                    KU::D::extract_range(&self.transaction.key_data, self.prev_key_end, *key_end);
                self.prev_key_end = *key_end;
                crate::BatchOp::Delete { key }
            }
        })
    }
}
