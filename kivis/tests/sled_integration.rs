#[cfg(feature = "sled-storage")]
use kivis::{Database, Record, SledStorage, manifest};

#[cfg(feature = "sled-storage")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "sled-storage")]
#[derive(Record, Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct TestRecord {
    data: Vec<u8>,
}

#[cfg(feature = "sled-storage")]
manifest![TestManifest: TestRecord];

#[test]
#[cfg(feature = "sled-storage")]
fn test_sled_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Database::<_, TestManifest>::new(SledStorage::new()?)?;

    let record = TestRecord {
        data: vec![1, 2, 3, 4],
    };

    let key = store.put(record.clone())?;
    let got = store.get(&key)?;
    assert_eq!(got, Some(record.clone()));

    store.remove(&key)?;
    let got2 = store.get(&key)?;
    assert_eq!(got2, None);

    Ok(())
}

#[test]
#[cfg(feature = "sled-storage")]
fn test_sled_persistence() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let path = temp_dir.path().join("test.db");

    // Create and populate database
    {
        let storage = SledStorage::open(&path)?;
        storage.db().insert(b"key1", b"value1")?;
        storage.db().insert(b"key2", b"value2")?;
        storage.db().flush()?;
    }

    // Reopen and verify
    {
        let storage = SledStorage::open(&path)?;
        assert_eq!(storage.db().get(b"key1")?.unwrap(), b"value1");
        assert_eq!(storage.db().get(b"key2")?.unwrap(), b"value2");
    }

    Ok(())
}

#[test]
#[cfg(feature = "sled-storage")]
fn test_sled_iteration() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Database::<_, TestManifest>::new(SledStorage::new()?)?;

    let record1 = TestRecord { data: vec![1, 2] };
    let record2 = TestRecord { data: vec![3, 4] };

    let key1 = store.put(record1)?;
    let key2 = store.put(record2)?;

    let keys: Vec<_> = store
        .iter_keys(TestRecordKey(0)..TestRecordKey(100))?
        .collect::<Result<Vec<_>, _>>()?;

    // Keys should be in reverse order
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&key1));
    assert!(keys.contains(&key2));

    Ok(())
}

#[test]
#[cfg(feature = "sled-storage")]
fn test_sled_batch_operations() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Database::<_, TestManifest>::new(SledStorage::new()?)?;

    let record1 = TestRecord { data: vec![1, 2] };
    let record2 = TestRecord { data: vec![3, 4] };

    // Insert some records
    let key1 = store.put(record1.clone())?;
    let key2 = store.put(record2.clone())?;

    // Verify both records exist
    assert_eq!(store.get(&key1)?, Some(record1));
    assert_eq!(store.get(&key2)?, Some(record2.clone()));

    // Remove one record
    store.remove(&key1)?;

    // Verify the removal
    assert_eq!(store.get(&key1)?, None);
    assert_eq!(store.get(&key2)?, Some(record2));

    Ok(())
}
