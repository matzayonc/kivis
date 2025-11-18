#![allow(clippy::unwrap_used)]
use kivis::{manifest, Database, DatabaseEntry, Index, KeyBytes, MemoryStorage, Record};

#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct User {
    #[index]
    name: String,
    email: String,
}

// Define a record type for a Pet.
#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
struct Pet {
    name: String,
    owner: UserKey,
}

manifest![Manifest: User, Pet];

#[test]
fn test_user_record() {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default()).unwrap();

    let user = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let user_key = store.put(&user).unwrap();

    let retrieved: User = store.get(&user_key).unwrap().unwrap();
    assert_eq!(retrieved, user);
    assert_eq!(user_key, UserKey(1));
}

#[test]
fn test_pet_record() {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default()).unwrap();

    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    let pet_key = store.put(&pet).unwrap();

    let retrieved: Pet = store.get(&pet_key).unwrap().unwrap();
    assert_eq!(retrieved, pet);
}

#[test]
fn test_get_owner_of_pet() {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default()).unwrap();

    let user = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let user_key = store.put(&user).unwrap();
    let pet = Pet {
        name: "Fido".to_string(),
        owner: user_key.clone(),
    };
    let pet_key = store.put(&pet).unwrap();

    let userr: User = store.get(&user_key).unwrap().unwrap();
    assert_eq!(user, userr);

    let retrieved: Pet = store.get(&pet_key).unwrap().unwrap();
    assert_eq!(retrieved, pet);

    let owner: User = store.get(&pet.owner).unwrap().unwrap();
    assert_eq!(owner, user);
}

#[test]
fn test_index() {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default()).unwrap();

    let user = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    let user_key = store.put(&user).unwrap();

    let index_keys = user.index_keys();
    assert_eq!(index_keys.len(), 1);
    assert_eq!(index_keys[0].0, UserNameIndex::INDEX);
    assert_eq!(
        index_keys[0]
            .1
            .to_bytes(bincode::config::standard())
            .unwrap(),
        user.name.to_bytes(bincode::config::standard()).unwrap()
    );

    let retrieved: User = store.get(&user_key).unwrap().unwrap();
    assert_eq!(retrieved, user);

    assert_eq!(store.dissolve().len(), 2)
}

#[test]
fn test_iter() {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default()).unwrap();

    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    store.put(&pet).unwrap();

    let retrieved = store
        .iter_keys(PetKey(0)..PetKey(u64::MAX))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

    assert_eq!(retrieved, PetKey(1));
}

#[test]
fn test_iter_index() {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default()).unwrap();

    let user = User {
        name: "Al".to_string(),
        email: "alice@example.com".to_string(),
    };

    store.put(&user).unwrap();

    let retrieved = store
        .iter_by_index(UserNameIndex("A".to_string())..UserNameIndex("Bob".to_string()))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(retrieved, UserKey(1));
}

#[test]
fn test_iter_index_exact() {
    let mut store = Database::<MemoryStorage, Manifest>::new(MemoryStorage::default()).unwrap();

    let names = [
        "Al", "Al", // The target name
        "Ak", "Am", // Previous and next names
        "Ala", "A",     // Shorter and longer names
        "Alice", // Alice for tradition
    ];
    for name in names {
        store
            .put(&User {
                name: name.to_string(),
                email: format!("{}@example.com", name.to_lowercase()),
            })
            .expect("Put should succeed.");
    }

    let als = store
        .iter_by_index_exact(&UserNameIndex("Al".into()))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(als.len(), 2);
    let al_1 = store.get(&als[0]).unwrap().unwrap();
    let al_2 = store.get(&als[1]).unwrap().unwrap();

    assert_eq!(al_1.name, "Al");
    assert_eq!(al_2.name, "Al");
}
