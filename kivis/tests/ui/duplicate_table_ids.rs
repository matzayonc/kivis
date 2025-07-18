use kivis::Record;
use serde::{Deserialize, Serialize};

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[table(1)]
struct FirstTable {
    id: u64,
    name: String,
}

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[table(1)] // This should cause a compile error
struct SecondTable {
    id: u64,
    value: String,
}

fn main() {}
