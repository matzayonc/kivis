use std::{collections::BTreeMap, u64};

use kivis::{Database, Record, Recordable, wrap_index};

#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[table(1)]
pub struct User {
    #[index]
    name: String,
    email: String,
}

// Define a record type for a Pet.
#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[table(2)]
struct Pet {
    name: String,
    owner: UserKey,
}

#[test]
fn test_user_record() {
    let mut store = Database::new(BTreeMap::<Vec<u8>, Vec<u8>>::new());

    let user = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let user_key = store.insert(user.clone()).unwrap();

    let retrieved: User = store.get(&user_key).unwrap().unwrap();
    assert_eq!(retrieved, user);
}

#[test]
fn test_pet_record() {
    let mut store = Database::new(BTreeMap::<Vec<u8>, Vec<u8>>::new());

    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    let pet_key = store.insert(pet.clone()).unwrap();

    let retrieved: Pet = store.get(&pet_key).unwrap().unwrap();
    assert_eq!(retrieved, pet);
}

#[test]
fn test_get_owner_of_pet() {
    let mut store = Database::new(BTreeMap::<Vec<u8>, Vec<u8>>::new());

    let user = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let user_key = store.insert(user.clone()).unwrap();
    let pet = Pet {
        name: "Fido".to_string(),
        owner: user_key.clone(),
    };
    let pet_key = store.insert(pet.clone()).unwrap();

    let userr: User = store.get(&user_key).unwrap().unwrap();
    assert_eq!(user, userr);

    let retrieved: Pet = store.get(&pet_key).unwrap().unwrap();
    assert_eq!(retrieved, pet);

    let owner: User = store.get(&pet.owner).unwrap().unwrap();
    assert_eq!(owner, user);
}

#[test]
fn test_index() {
    let mut store = Database::new(BTreeMap::<Vec<u8>, Vec<u8>>::new());

    let user = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    let user_key = store.insert(user.clone()).unwrap();

    let index_keys = user.index_keys(user_key.clone()).unwrap();
    assert_eq!(index_keys.len(), 1);
    assert_eq!(
        index_keys[0],
        wrap_index::<User, UserNameIndex>(user_key.clone(), UserNameIndex(user.name.clone()))
            .unwrap()
    );

    let retrieved: User = store.get(&user_key).unwrap().unwrap();
    assert_eq!(retrieved, user);

    assert_eq!(store.dissolve().len(), 2)
}

#[test]
fn test_iter() {
    let mut store = Database::new(BTreeMap::<Vec<u8>, Vec<u8>>::new());

    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    store.insert(pet.clone()).unwrap();

    let retrieved = store
        .iter_keys::<Pet>(PetKey(1)..PetKey(u64::MAXq))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

    assert_eq!(retrieved, PetKey(18446744073709551614));
}

#[test]
fn test_iter_index() {
    let mut store = Database::new(BTreeMap::<Vec<u8>, Vec<u8>>::new());

    let user = User {
        name: "Al".to_string(),
        email: "alice@example.com".to_string(),
    };

    store.insert(user.clone()).unwrap();

    let retrieved: UserKey = store
        .iter_by_index(UserNameIndex("A".to_string())..UserNameIndex("Bob".to_string()))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(retrieved, UserKey(18446744073709551614));
}
