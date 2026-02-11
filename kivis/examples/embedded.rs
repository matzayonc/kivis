#![no_std]

extern crate alloc;

use alloc::vec::Vec;
use core::ops::Range;
use ekv::flash::{Flash, PageID};
use kivis::{
    BufferOverflowError, BufferOverflowOr, Record, Repository, Storage, Unifier, UnifierData,
    manifest,
};
use serde::Serialize;

/// A simple sensor reading with fixed-size data
#[derive(
    Record, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct SensorReading {
    #[key]
    sensor_id: u8,
    temperature: i16,
    humidity: u8,
}

/// A device configuration record
#[derive(
    Record, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct DeviceConfig {
    #[key]
    device_id: u16,
    enabled: bool,
    sample_rate: u8,
}

manifest![EmbeddedManifest: SensorReading, DeviceConfig];

/// Postcard unifier for no_std environments
#[derive(Debug, Clone, Copy, Default)]
pub struct PostcardUnifier;

impl Unifier for PostcardUnifier {
    type D = [u8];
    type SerError = postcard::Error;
    type DeError = postcard::Error;

    fn serialize(
        &self,
        buffer: &mut Vec<u8>,
        data: impl Serialize,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        let start = buffer.len();
        let serialized = postcard::to_allocvec(&data)?;
        <[u8]>::extend(buffer, &serialized).map_err(BufferOverflowOr::overflow)?;
        Ok((start, buffer.len()))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(
        &self,
        data: &Vec<u8>,
    ) -> Result<T, Self::DeError> {
        postcard::from_bytes(data)
    }
}

/// Mock flash implementation for demonstration
pub struct MockFlash<const SIZE: usize> {
    data: [u8; SIZE],
}

impl<const SIZE: usize> Default for MockFlash<SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const SIZE: usize> MockFlash<SIZE> {
    pub const fn new() -> Self {
        Self { data: [0xFF; SIZE] }
    }
}

impl<const SIZE: usize> Flash for MockFlash<SIZE> {
    type Error = core::convert::Infallible;

    async fn read(
        &mut self,
        page_id: PageID,
        offset: usize,
        data: &mut [u8],
    ) -> Result<(), Self::Error> {
        let page_offset = page_id.index() * 4096 + offset;
        data.copy_from_slice(&self.data[page_offset..page_offset + data.len()]);
        Ok(())
    }

    async fn write(
        &mut self,
        page_id: PageID,
        offset: usize,
        data: &[u8],
    ) -> Result<(), Self::Error> {
        let page_offset = page_id.index() * 4096 + offset;
        self.data[page_offset..page_offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    async fn erase(&mut self, page_id: PageID) -> Result<(), Self::Error> {
        let page_offset = page_id.index() * 4096;
        self.data[page_offset..page_offset + 4096].fill(0xFF);
        Ok(())
    }

    fn page_count(&self) -> usize {
        SIZE / 4096
    }
}

/// Error type for ekv storage operations
#[derive(Debug)]
pub enum EkvError {
    Ekv(ekv::Error<core::convert::Infallible>),
    Write(ekv::WriteError<core::convert::Infallible>),
    Read(ekv::ReadError<core::convert::Infallible>),
    Commit(ekv::CommitError<core::convert::Infallible>),
    Cursor(ekv::CursorError<core::convert::Infallible>),
    BufferOverflow(BufferOverflowError),
}

impl core::fmt::Display for EkvError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            EkvError::Ekv(e) => write!(f, "Ekv error: {:?}", e),
            EkvError::Write(e) => write!(f, "Write error: {:?}", e),
            EkvError::Read(e) => write!(f, "Read error: {:?}", e),
            EkvError::Commit(e) => write!(f, "Commit error: {:?}", e),
            EkvError::Cursor(e) => write!(f, "Cursor error: {:?}", e),
            EkvError::BufferOverflow(e) => write!(f, "Buffer overflow: {:?}", e),
        }
    }
}

impl core::error::Error for EkvError {}

impl From<ekv::Error<core::convert::Infallible>> for EkvError {
    fn from(e: ekv::Error<core::convert::Infallible>) -> Self {
        EkvError::Ekv(e)
    }
}

impl From<ekv::WriteError<core::convert::Infallible>> for EkvError {
    fn from(e: ekv::WriteError<core::convert::Infallible>) -> Self {
        EkvError::Write(e)
    }
}

impl From<ekv::ReadError<core::convert::Infallible>> for EkvError {
    fn from(e: ekv::ReadError<core::convert::Infallible>) -> Self {
        EkvError::Read(e)
    }
}

impl From<ekv::CommitError<core::convert::Infallible>> for EkvError {
    fn from(e: ekv::CommitError<core::convert::Infallible>) -> Self {
        EkvError::Commit(e)
    }
}

impl From<ekv::CursorError<core::convert::Infallible>> for EkvError {
    fn from(e: ekv::CursorError<core::convert::Infallible>) -> Self {
        EkvError::Cursor(e)
    }
}

impl From<BufferOverflowError> for EkvError {
    fn from(e: BufferOverflowError) -> Self {
        EkvError::BufferOverflow(e)
    }
}

/// Storage implementation using ekv with postcard serialization
pub struct EkvStorage<const SIZE: usize> {
    db: ekv::Database<MockFlash<SIZE>, embassy_sync::blocking_mutex::raw::NoopRawMutex>,
}

impl<const SIZE: usize> Default for EkvStorage<SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const SIZE: usize> EkvStorage<SIZE> {
    pub fn new() -> Self {
        let flash = MockFlash::<SIZE>::new();
        let db = ekv::Database::new(flash, ekv::Config::default());

        // Format and mount the database
        futures::executor::block_on(async {
            db.format().await.expect("Failed to format database");
            db.mount().await.expect("Failed to mount database");
        });

        Self { db }
    }
}

impl<const SIZE: usize> Repository for EkvStorage<SIZE> {
    type K = [u8];
    type V = [u8];
    type Error = EkvError;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        futures::executor::block_on(async {
            let mut txn = self.db.write_transaction().await;
            txn.write(key, value).await?;
            txn.commit().await?;
            Ok::<_, EkvError>(())
        })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        futures::executor::block_on(async {
            let mut buffer = [0u8; 1024];
            let txn = self.db.read_transaction().await;
            match txn.read(key, &mut buffer).await {
                Ok(len) => Ok(Some(buffer[..len].to_vec())),
                Err(_) => Ok(None), // Treat any read error as key not found for simplicity
            }
        })
    }

    fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let existing = self.get(key)?;
        if existing.is_some() {
            futures::executor::block_on(async {
                let mut txn = self.db.write_transaction().await;
                txn.delete(key).await?;
                txn.commit().await?;
                Ok::<_, EkvError>(())
            })?;
        }
        Ok(existing)
    }

    fn iter_keys(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::Error>>, Self::Error> {
        // Collect all keys within the range using ekv's read_range cursor
        let keys = futures::executor::block_on(async {
            let txn = self.db.read_transaction().await;
            let mut collected_keys = Vec::new();

            // Get a cursor for the range
            let start_bound = range.start.as_slice();
            let end_bound = range.end.as_slice();
            let mut cursor = txn.read_range(start_bound..end_bound).await.ok();

            if let Some(cursor) = cursor.as_mut() {
                // Iterate through all keys in the range
                loop {
                    let mut key_buf = [0u8; 256];
                    let mut value_buf = [0u8; 1024];
                    match cursor.next(&mut key_buf, &mut value_buf).await {
                        Ok(Some((key_len, _value_len))) => {
                            collected_keys.push(Ok(key_buf[..key_len].to_vec()));
                        }
                        Ok(None) => break, // No more keys
                        Err(e) => {
                            collected_keys.push(Err(EkvError::from(e)));
                            break;
                        }
                    }
                }
            }

            collected_keys
        });

        Ok(keys.into_iter())
    }

    fn batch_mixed<'a>(
        &mut self,
        operations: impl Iterator<Item = kivis::BatchOp<'a, Self::K, Self::V>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let mut deleted = Vec::new();

        // Collect operations into a vector so we can sort them
        let mut ops: Vec<_> = operations
            .map(|op| match op {
                kivis::BatchOp::Insert { key, value } => (key.to_vec(), Some(value.to_vec())),
                kivis::BatchOp::Delete { key } => (key.to_vec(), None),
            })
            .collect();

        // Sort operations by key (required by ekv)
        ops.sort_by(|a, b| a.0.cmp(&b.0));

        futures::executor::block_on(async {
            let mut txn = self.db.write_transaction().await;

            for (key, value_opt) in ops {
                match value_opt {
                    Some(value) => {
                        txn.write(&key, &value).await?;
                        deleted.push(None);
                    }
                    None => {
                        // Get the value before deleting
                        let existing = {
                            let mut buffer = [0u8; 1024];
                            let read_txn = self.db.read_transaction().await;
                            match read_txn.read(&key, &mut buffer).await {
                                Ok(len) => Some(buffer[..len].to_vec()),
                                Err(_) => None,
                            }
                        };

                        txn.delete(&key).await?;
                        deleted.push(existing);
                    }
                }
            }

            txn.commit().await?;
            Ok::<_, EkvError>(())
        })?;

        Ok(deleted)
    }
}

impl<const SIZE: usize> Storage for EkvStorage<SIZE> {
    type KeyUnifier = PostcardUnifier;
    type ValueUnifier = PostcardUnifier;
}

fn main() {
    // Create an embedded database with ekv storage (64KB flash)
    let storage = EkvStorage::<65536>::new();
    let mut db = kivis::Database::<_, EmbeddedManifest>::new(storage).unwrap();

    // 1. Insert 4 values
    let reading1 = SensorReading {
        sensor_id: 1,
        temperature: 2250, // 22.5°C
        humidity: 65,
    };
    let reading2 = SensorReading {
        sensor_id: 2,
        temperature: 2100, // 21.0°C
        humidity: 70,
    };
    let reading3 = SensorReading {
        sensor_id: 3,
        temperature: 2300, // 23.0°C
        humidity: 68,
    };
    let reading4 = SensorReading {
        sensor_id: 4,
        temperature: 2400, // 24.0°C
        humidity: 72,
    };

    let key1 = db.insert(reading1).unwrap();
    let key2 = db.insert(reading2).unwrap();
    let key3 = db.insert(reading3).unwrap();
    let key4 = db.insert(reading4).unwrap();

    assert_eq!(key1, SensorReadingKey(1));
    assert_eq!(key2, SensorReadingKey(2));
    assert_eq!(key3, SensorReadingKey(3));
    assert_eq!(key4, SensorReadingKey(4));

    // 2. Read two of them and assert them to be different
    let read1 = db.get(&key1).unwrap();
    let read2 = db.get(&key2).unwrap();

    assert_eq!(read1, Some(reading1));
    assert_eq!(read2, Some(reading2));
    assert_ne!(read1, read2);

    // 3. Iterate all (should see 4 values)
    let all_keys: Vec<_> = db
        .iter_keys(SensorReadingKey(0)..SensorReadingKey(255))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(all_keys.len(), 4);
    assert_eq!(all_keys[0], key1);
    assert_eq!(all_keys[1], key2);
    assert_eq!(all_keys[2], key3);
    assert_eq!(all_keys[3], key4);

    // 4. Iterate over a range where only the 2 inner values should be shown
    // Keys are 1, 2, 3, 4, so range [2..4) should give us keys 2 and 3
    let range_keys: Vec<_> = db
        .iter_keys(SensorReadingKey(2)..SensorReadingKey(4))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(range_keys.len(), 2);
    assert_eq!(range_keys[0], key2);
    assert_eq!(range_keys[1], key3);

    // 5. Delete 2 values and iterate over all again
    db.remove(&key1).unwrap();
    db.remove(&key4).unwrap();

    let remaining_keys: Vec<_> = db
        .iter_keys(SensorReadingKey(0)..SensorReadingKey(255))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(remaining_keys.len(), 2);
    assert_eq!(remaining_keys[0], key2);
    assert_eq!(remaining_keys[1], key3);

    // All tests passed!
}
