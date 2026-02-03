use crate::Incrementable;

impl Incrementable for u128 {
    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u64 {
    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u32 {
    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u16 {
    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u8 {
    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}
