use std::collections::BTreeMap;

use kivis::{Database, Record, Recordable};
use serde::{Deserialize, Serialize};

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[table(1)]
struct UserRecord {
    id: u64,
    data: Vec<u8>,
}

#[test]
fn test_lifecycle() {
    let mut store = Database::new(BTreeMap::<Vec<u8>, Vec<u8>>::new());

    let user = UserRecord {
        id: 1,
        data: vec![1, 2, 3, 4],
    };
    let key = user.key();

    store.insert(user.clone()).unwrap();
    assert_eq!(store.get(&key).unwrap(), Some(user.clone()));
    store.remove::<UserRecord>(&key).unwrap();
    assert_eq!(store.get::<UserRecord>(&key).unwrap(), None);
}

#[test]
fn test_iter() {
    let mut store = Database::new(BTreeMap::<Vec<u8>, Vec<u8>>::new());

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
