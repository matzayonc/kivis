#[cfg(feature = "sled")]
mod tests {

    use kivis::{Database, Record, manifest};

    use serde::{Deserialize, Serialize};

    #[derive(Record, Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    struct TestRecord {
        data: Vec<u8>,
    }

    manifest![TestManifest: TestRecord];

    #[test]
    fn test_sled_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let path = temp_dir.path().join("test.db");
        let mut store = Database::<_, TestManifest>::new(sled::open(&path)?)?;

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
    fn test_sled_persistence() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let path = temp_dir.path().join("test.db");

        // Create and populate database
        {
            let storage = sled::open(&path)?;
            storage.insert(b"key1", b"value1")?;
            storage.insert(b"key2", b"value2")?;
            storage.flush()?;
        }

        // Reopen and verify
        {
            let storage = sled::open(&path)?;
            assert_eq!(storage.get(b"key1")?.unwrap(), b"value1");
            assert_eq!(storage.get(b"key2")?.unwrap(), b"value2");
        }

        Ok(())
    }

    #[test]
    fn test_sled_iteration() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let path = temp_dir.path().join("test.db");
        let mut store = Database::<_, TestManifest>::new(sled::open(&path)?)?;

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
    fn test_sled_batch_operations() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let path = temp_dir.path().join("test.db");
        let mut store = Database::<_, TestManifest>::new(sled::open(&path)?)?;

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

    #[test]
    fn test_sled_autoincrement_survives_reopen() -> Result<(), Box<dyn std::error::Error>> {
        // Regression: postcard varint keys made `last_id` return 255 after reopen, so the next
        // `put` overwrote record 256. Keys are now encoded with `OrderedKeyConfig`.
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let path = temp_dir.path().join("test.db");

        let mut store = Database::<_, TestManifest>::new(sled::open(&path)?)?;
        let mut keys = Vec::new();
        for i in 0..300u16 {
            keys.push(store.put(TestRecord {
                data: i.to_le_bytes().to_vec(),
            })?);
        }
        assert_eq!(keys.last(), Some(&TestRecordKey(300)));
        drop(store.dissolve());

        let mut store = Database::<_, TestManifest>::new(sled::open(&path)?)?;
        assert_eq!(store.last_id::<TestRecordKey>()?, TestRecordKey(300));
        let fresh = store.put(TestRecord { data: vec![255] })?;
        assert_eq!(fresh, TestRecordKey(301));
        assert!(!keys.contains(&fresh));

        let all: Vec<u64> = store
            .iter_all_keys::<TestRecordKey>()?
            .map(|k| k.map(|k| k.0))
            .collect::<Result<_, _>>()?;
        assert_eq!(all, (1..=301).collect::<Vec<u64>>());
        Ok(())
    }
}
