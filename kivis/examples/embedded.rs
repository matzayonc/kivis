#![no_std]

use core::ops::Range;
use ekv::flash::{Flash, PageID};
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use heapless::Vec;
use kivis::{
    BufferOp, BufferOverflowError, BufferOverflowOr, Record, Repository, Storage, Unifier,
    UnifierData, manifest,
};
use ouroboros::self_referencing;
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

/// Postcard unifier for no_std environments with heapless buffers
#[derive(Debug, Clone, Copy, Default)]
pub struct PostcardUnifier<const N: usize>;

impl<const N: usize> Unifier for PostcardUnifier<N> {
    type D = Vec<u8, N>;
    type SerError = postcard::Error;
    type DeError = postcard::Error;

    fn serialize(
        &self,
        buffer: &mut Self::D,
        data: impl Serialize,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        let start = buffer.len();
        // Create a temporary buffer for serialization
        let mut temp_buffer = [0u8; N];
        let serialized = postcard::to_slice(&data, &mut temp_buffer)?;
        buffer
            .extend_from(serialized)
            .map_err(BufferOverflowOr::overflow)?;
        Ok((start, buffer.len()))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(
        &self,
        data: &Self::D,
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
pub struct EkvStorage<const SIZE: usize, const KEY_SIZE: usize, const VALUE_SIZE: usize> {
    db: ekv::Database<MockFlash<SIZE>, embassy_sync::blocking_mutex::raw::NoopRawMutex>,
}

impl<const SIZE: usize, const KEY_SIZE: usize, const VALUE_SIZE: usize> Default
    for EkvStorage<SIZE, KEY_SIZE, VALUE_SIZE>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<const SIZE: usize, const KEY_SIZE: usize, const VALUE_SIZE: usize>
    EkvStorage<SIZE, KEY_SIZE, VALUE_SIZE>
{
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

impl<const SIZE: usize, const KEY_SIZE: usize, const VALUE_SIZE: usize> Repository
    for EkvStorage<SIZE, KEY_SIZE, VALUE_SIZE>
{
    type K = Vec<u8, KEY_SIZE>;
    type V = Vec<u8, VALUE_SIZE>;
    type Error = EkvError;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        futures::executor::block_on(async {
            let mut txn = self.db.write_transaction().await;
            txn.write(key, value).await?;
            txn.commit().await?;
            Ok::<_, EkvError>(())
        })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Self::V>, Self::Error> {
        futures::executor::block_on(async {
            let mut buffer = Vec::<u8, VALUE_SIZE>::new();
            buffer.resize(VALUE_SIZE, 0).ok();

            let txn = self.db.read_transaction().await;
            match txn.read(key, buffer.as_mut_slice()).await {
                Ok(len) => Vec::from_slice(&buffer[..len])
                    .map(Some)
                    .map_err(|_| EkvError::BufferOverflow(BufferOverflowError)),
                Err(_) => Ok(None), // Treat any read error as key not found for simplicity
            }
        })
    }

    fn remove(&mut self, key: &[u8]) -> Result<Option<Self::V>, Self::Error> {
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
        range: Range<Self::K>,
    ) -> Result<impl Iterator<Item = Result<Self::K, Self::Error>>, Self::Error> {
        let iter = CursorIterBuilder {
            db: &self.db,
            range,
            txn_builder: |db| futures::executor::block_on(db.read_transaction()),
            cursor_builder: |txn, range| {
                futures::executor::block_on(
                    txn.read_range(range.start.as_slice()..range.end.as_slice()),
                )
                .map_err(|e| match e {
                    ekv::Error::Corrupted => ekv::CursorError::Corrupted,
                    ekv::Error::Flash(_) => unreachable!(),
                })
            },
        }
        .build();

        Ok(iter)
    }

    fn batch_mixed<'a>(
        &mut self,
        operations: impl Iterator<Item = kivis::BatchOp<'a, Self::K, Self::V>>,
    ) -> Result<(), Self::Error> {
        futures::executor::block_on(async {
            let mut txn = self.db.write_transaction().await;

            for op in operations {
                match op {
                    kivis::BatchOp::Insert { key, value } => {
                        txn.write(key, value).await?;
                    }
                    kivis::BatchOp::Delete { key } => {
                        txn.delete(key).await?;
                    }
                }
            }

            txn.commit().await?;
            Ok::<_, EkvError>(())
        })
    }
}

#[self_referencing]
struct CursorIter<'a, const SIZE: usize, const KEY_SIZE: usize> {
    db: &'a ekv::Database<MockFlash<SIZE>, NoopRawMutex>,
    range: Range<Vec<u8, KEY_SIZE>>,
    #[borrows(db)]
    #[not_covariant]
    txn: ekv::ReadTransaction<'this, MockFlash<SIZE>, NoopRawMutex>,
    #[borrows(txn, range)]
    #[not_covariant]
    cursor: Result<
        ekv::Cursor<'this, MockFlash<SIZE>, NoopRawMutex>,
        ekv::CursorError<core::convert::Infallible>,
    >,
}

impl<const SIZE: usize, const KEY_SIZE: usize> Iterator for CursorIter<'_, SIZE, KEY_SIZE> {
    type Item = Result<Vec<u8, KEY_SIZE>, EkvError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut key_buf = Vec::<u8, KEY_SIZE>::new();
        let mut value_buf = Vec::<u8, 1024>::new();
        key_buf.resize(KEY_SIZE, 0).ok();
        value_buf.resize(1024, 0).ok();

        self.with_cursor_mut(|cursor| {
            match cursor {
                Ok(cursor) => {
                    match futures::executor::block_on(
                        cursor.next(key_buf.as_mut_slice(), value_buf.as_mut_slice()),
                    ) {
                        Ok(Some((key_len, _value_len))) => Vec::from_slice(&key_buf[..key_len])
                            .map(Ok)
                            .ok()
                            .or(Some(Err(EkvError::BufferOverflow(BufferOverflowError)))),
                        Ok(None) => None, // No more keys
                        Err(e) => Some(Err(EkvError::from(e))),
                    }
                }
                Err(_) => None,
            }
        })
    }
}

impl<const SIZE: usize, const KEY_SIZE: usize, const VALUE_SIZE: usize> Storage
    for EkvStorage<SIZE, KEY_SIZE, VALUE_SIZE>
{
    type Repo = Self;
    type KeyUnifier = PostcardUnifier<KEY_SIZE>;
    type ValueUnifier = PostcardUnifier<VALUE_SIZE>;
    type Container = heapless::Vec<BufferOp, 256>;

    fn repository(&self) -> &Self::Repo {
        self
    }

    fn repository_mut(&mut self) -> &mut Self::Repo {
        self
    }
}

fn main() {
    // Create an embedded database with ekv storage (64KB flash, 256-byte keys, 1024-byte values)
    let storage = EkvStorage::<65536, 256, 1024>::new();
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

    // Insert readings 3 and 4 using transaction API
    let mut tx = db.create_transaction();
    let key3 = tx.insert(reading3).unwrap();
    let key4 = tx.insert(reading4).unwrap();
    db.commit(tx).unwrap();

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
    let all_keys: Vec<_, 10> = db
        .iter_keys(SensorReadingKey(0)..SensorReadingKey(255))
        .unwrap()
        .collect::<Result<Vec<_, 10>, _>>()
        .unwrap();

    assert_eq!(all_keys.len(), 4);
    assert_eq!(all_keys[0], key1);
    assert_eq!(all_keys[1], key2);
    assert_eq!(all_keys[2], key3);
    assert_eq!(all_keys[3], key4);

    // 4. Iterate over a range where only the 2 inner values should be shown
    // Keys are 1, 2, 3, 4, so range [2..4) should give us keys 2 and 3
    let range_keys: Vec<_, 10> = db
        .iter_keys(SensorReadingKey(2)..SensorReadingKey(4))
        .unwrap()
        .collect::<Result<Vec<_, 10>, _>>()
        .unwrap();

    assert_eq!(range_keys.len(), 2);
    assert_eq!(range_keys[0], key2);
    assert_eq!(range_keys[1], key3);

    // 5. Delete 2 values and iterate over all again
    db.remove(&key1).unwrap();
    db.remove(&key4).unwrap();

    let remaining_keys: Vec<_, 10> = db
        .iter_keys(SensorReadingKey(0)..SensorReadingKey(255))
        .unwrap()
        .collect::<Result<Vec<_, 10>, _>>()
        .unwrap();

    assert_eq!(remaining_keys.len(), 2);
    assert_eq!(remaining_keys[0], key2);
    assert_eq!(remaining_keys[1], key3);

    // All tests passed!
}
