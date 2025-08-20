use kivis::{manifest, Database, MemoryStorage, Record};
use serde::{Deserialize, Serialize};

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct UserRecord {
    #[key]
    id: u64,
    data: Vec<u8>,
}

manifest![Manifest: UserRecord];

#[test]
fn test_lifecycle() {
    let mut store = Database::<MemoryStorage, Manifest>::default();

    let user = UserRecord {
        id: 1,
        data: vec![1, 2, 3, 4],
    };
    let key = store.insert(user.clone()).unwrap();
    assert_eq!(store.get(&key).unwrap(), Some(user.clone()));
    assert_eq!(key, UserRecordKey(1));
    store.remove(&key).unwrap();
    assert_eq!(store.get(&key).unwrap(), None);
}

#[test]
fn test_iter() {
    let mut store = Database::<MemoryStorage, Manifest>::default();

    let user = UserRecord {
        id: 1,
        data: vec![1, 2, 3, 4],
    };
    let another = UserRecord {
        id: 2,
        data: vec![5, 6, 7, 8],
    };

    store.insert(user.clone()).unwrap();
    store.insert(another.clone()).unwrap();

    // let iter = store
    //     .iter_keys::<UserRecord>(&UserRecordKey(0)..&UserRecordKey(3))
    //     .unwrap()
    //     .collect::<Result<Vec<_>, _>>()
    //     .unwrap();
    // assert_eq!(iter, vec![user.key(), another.key()]);
}
