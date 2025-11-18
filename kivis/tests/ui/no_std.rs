#![no_std]
extern crate alloc;

use alloc::{string::String, vec::Vec};
use kivis::manifest;
use kivis::Record;
use serde::{Deserialize, Serialize};

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NoStdRecord {
    pub id: u64,
    pub name: String,
}

manifest![MyManifest: NoStdRecord];

fn main() {}
