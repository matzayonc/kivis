use kivis::{Database, MemoryStorage, Record, manifest};
use serde::{Deserialize, Serialize};

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UserRecord {
    #[key]
    id: u64,
    data: Vec<u8>,
}

manifest![Manifest: UserRecord];

#[test]
fn test_lifecycle() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let user = UserRecord {
        id: 1,
        data: vec![1, 2, 3, 4],
    };
    let key = store.insert(&user)?;
    assert_eq!(store.get(&key)?, Some(user.clone()));
    assert_eq!(key, UserRecordKey(1));
    store.remove(&key)?;
    assert_eq!(store.get(&key)?, None);
    Ok(())
}

#[test]
fn test_autoincrement_iter() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let user = UserRecord {
        id: 1,
        data: vec![1, 2, 3, 4],
    };
    let another = UserRecord {
        id: 2,
        data: vec![5, 6, 7, 8],
    };

    store.insert(&user)?;
    store.insert(&another)?;

    let iter = store
        .iter_keys(UserRecordKey(0)..UserRecordKey(3))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(iter, vec![UserRecordKey(2), UserRecordKey(1)]);
    Ok(())
}
