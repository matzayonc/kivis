use kivis::Record;
use serde::{Deserialize, Serialize};

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[external(1)]
struct FirstTable {
    id: u64,
    name: String,
}

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[external(2)]
struct SecondTable {
    id: u64,
    value: String,
}

fn main() {}
