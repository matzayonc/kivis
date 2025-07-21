use kivis::{Database, MemoryStorage, Record};
use serde::{Deserialize, Serialize};

#[derive(Record, Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[table(1)]
struct UserRecord {
    data: Vec<u8>,
}

#[test]
fn test_lifecycle() {
    let mut store = Database::new(MemoryStorage::new());

    let user = UserRecord {
        data: vec![1, 2, 3, 4],
    };

    let user_key = store.put(user.clone()).unwrap();
    assert_eq!(store.get(&user_key).unwrap(), Some(user.clone()));
    store.remove(&user_key).unwrap();
    assert_eq!(store.get(&user_key).unwrap(), None);
}

#[test]
fn test_iter() {
    let mut store = Database::new(MemoryStorage::new());

    let user = UserRecord {
        data: vec![1, 2, 3, 4],
    };
    let another = UserRecord {
        data: vec![5, 6, 7, 8],
    };

    let user_key = store.put(user.clone()).unwrap();
    let another_key = store.put(another.clone()).unwrap();

    let iter = store
        .iter_keys(UserRecordKey(0)..UserRecordKey(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(iter, vec![another_key, user_key]);
}
