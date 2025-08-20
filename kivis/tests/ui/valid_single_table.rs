use kivis::Record;
use serde::{Deserialize, Serialize};

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[external(1)]
struct ValidTable {
    id: u64,
    name: String,
}

fn main() {}
