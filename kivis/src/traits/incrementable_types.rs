use super::Incrementable;

impl Incrementable for u128 {
    fn bounds() -> (Self, Self) {
        (0, u128::MAX)
    }

    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u64 {
    fn bounds() -> (Self, Self) {
        (0, u64::MAX)
    }

    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u32 {
    fn bounds() -> (Self, Self) {
        (0, u32::MAX)
    }

    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u16 {
    fn bounds() -> (Self, Self) {
        (0, u16::MAX)
    }

    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Incrementable for u8 {
    fn bounds() -> (Self, Self) {
        (0, u8::MAX)
    }

    fn next_id(&self) -> Option<Self> {
        self.checked_add(1)
    }
}
