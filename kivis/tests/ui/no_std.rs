#![no_std]
extern crate alloc;

use alloc::string::String;
use kivis::Record;
use kivis::manifest;
use serde::{Deserialize, Serialize};

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NoStdRecord {
    pub id: u64,
    pub name: String,
}

manifest![MyManifest: NoStdRecord];

fn main() {}
