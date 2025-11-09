use kivis::Record;
use serde::{Deserialize, Serialize};

// This should fail - cannot use both #[key] field attributes and #[derived_key] attribute
#[derive(Debug, Serialize, Deserialize, Record)]
#[derived_key(u32)]
struct ConflictingStrategies {
    #[key]
    id: u64,
    p: u16,
}

fn main() {}
