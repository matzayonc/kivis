use kivis::{Database, MemoryStorage, Record, manifest};
use serde::{Deserialize, Serialize};

#[derive(Record, Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct UserRecord {
    data: Vec<u8>,
}

manifest![Manifest: UserRecord];

#[test]
fn test_lifecycle() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let user = UserRecord {
        data: vec![1, 2, 3, 4],
    };

    let user_key = store.put(&user)?;
    let got = store.get(&user_key)?;
    assert_eq!(got, Some(user.clone()));
    store.remove(&user_key)?;
    let got2 = store.get(&user_key)?;
    assert_eq!(got2, None);
    Ok(())
}

#[test]
fn test_iter() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let user = UserRecord {
        data: vec![1, 2, 3, 4],
    };
    let another = UserRecord {
        data: vec![5, 6, 7, 8],
    };

    let user_key = store.put(&user)?;
    let another_key = store.put(&another)?;

    assert_ne!(user_key, another_key);

    let iter = store
        .iter_keys(UserRecordKey(0)..UserRecordKey(3))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(iter, vec![another_key, user_key]);
    Ok(())
}
