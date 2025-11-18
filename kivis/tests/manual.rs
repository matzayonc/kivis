#![allow(clippy::unwrap_used)]
use std::{collections::BTreeMap, fmt::Display, ops::Range};

use kivis::{
    Database, DatabaseEntry, DeriveKey, Incrementable, Index, KeyBytes, RecordKey, Scope, Storage,
};

// Define a record type for an User.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct UserKey(pub u64);

impl RecordKey for UserKey {
    type Record = User;
}
impl DeriveKey for User {
    type Key = UserKey;
    fn key(c: &<Self::Key as RecordKey>::Record) -> Self::Key {
        UserKey(c.id)
    }
}

impl Scope for User {
    const SCOPE: u8 = 1;
    type Manifest = ();
}
impl kivis::DatabaseEntry for User {
    type Key = UserKey;

    fn index_keys(&self) -> Vec<(u8, &dyn KeyBytes)> {
        vec![(UserNameIndex::INDEX, &self.name)]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct UserNameIndex(pub String);
impl Index for UserNameIndex {
    type Key = UserKey;
    type Record = User;
    const INDEX: u8 = 1;
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct User {
    id: u64,
    name: String,
    email: String,
}

// Define a record type for a Pet.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct PetKey(pub u64);
impl Default for PetKey {
    fn default() -> Self {
        PetKey(u64::MAX)
    }
}
impl RecordKey for PetKey {
    type Record = Pet;
}
impl Scope for Pet {
    const SCOPE: u8 = 2;
    type Manifest = ();
}
impl kivis::DatabaseEntry for Pet {
    type Key = PetKey;
}
impl Incrementable for PetKey {
    // Order is reversed here, as we want to be able to get the latest entries first for the auto-increment.
    // const BOUNDS: (Self, Self) = (PetKey(u64::MAX), PetKey(0));

    fn next_id(&self) -> Option<Self> {
        self.0.checked_sub(1).map(PetKey)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct Pet {
    name: String,
    owner: UserKey,
}

#[derive(Default)]
struct Manifest {
    last_user: Option<UserKey>,
    last_pet: Option<PetKey>,
}
impl kivis::Manifest for Manifest {
    fn members() -> Vec<u8> {
        vec![User::SCOPE, Pet::SCOPE]
    }

    fn load<S: Storage>(
        &mut self,
        db: &mut Database<S, Self>,
    ) -> Result<(), kivis::DatabaseError<S::StoreError>> {
        *self = Self {
            last_user: None,
            last_pet: Some(db.last_id::<PetKey>()?),
        };
        Ok(())
    }
}
impl kivis::Manifests<User> for Manifest {
    fn last(&mut self) -> &mut Option<<User as kivis::DatabaseEntry>::Key> {
        &mut self.last_user
    }
}
impl kivis::Manifests<Pet> for Manifest {
    fn last(&mut self) -> &mut Option<<Pet as kivis::DatabaseEntry>::Key> {
        &mut self.last_pet
    }
}

// Define storage for the database.
#[derive(Default)]
struct ManualStorage {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}
#[derive(Debug, PartialEq, Eq)]
struct NoError;
impl Display for NoError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
impl kivis::Storage for ManualStorage {
    type StoreError = NoError;

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError> {
        self.data.insert(key, value);
        Ok(())
    }

    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.data.get(&key).cloned())
    }

    fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.data.remove(&key))
    }

    fn iter_keys(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError> {
        let iter = self.data.range(range);
        Ok(iter.map(|(k, _v)| Ok(k.clone())))
    }
}

#[test]
fn test_user_record() {
    let db = ManualStorage::default();
    let mut database: Database<_, Manifest> = Database::new(db).unwrap();

    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    database.insert(&user).unwrap();

    let retrieved: User = database.get(&UserKey(user.id)).unwrap().unwrap();
    assert_eq!(retrieved, user);
}

#[test]
fn test_pet_record() {
    let db = ManualStorage::default();
    let mut database: Database<_, Manifest> = Database::new(db).unwrap();

    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    let pet_key = database.put(&pet).unwrap();

    let retrieved: Pet = database.get(&pet_key).unwrap().unwrap();
    assert_eq!(retrieved, pet);
}

#[test]
fn test_get_owner_of_pet() {
    let db = ManualStorage::default();
    let mut database: Database<_, Manifest> = Database::new(db).unwrap();

    let mut user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(user.id),
    };

    user.id = database.insert(&user).unwrap().0;
    let pet_key = database.put(&pet).unwrap();

    let retrieved: Pet = database.get(&pet_key).unwrap().unwrap();
    assert_eq!(retrieved, pet);

    let owner: User = database.get(&pet.owner).unwrap().unwrap();
    assert_eq!(owner, user);
}

#[test]
fn test_index() {
    let db = ManualStorage::default();
    let mut database: Database<_, Manifest> = Database::new(db).unwrap();

    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    let _user_key = database.insert(&user).unwrap();

    let index_keys = user.index_keys();
    assert_eq!(index_keys.len(), 1);
    assert_eq!(index_keys[0].0, 1);
    assert_eq!(
        index_keys[0]
            .1
            .to_bytes(bincode::config::standard())
            .unwrap(),
        user.name.to_bytes(bincode::config::standard()).unwrap()
    );

    let retrieved: User = database.get(&UserKey(user.id)).unwrap().unwrap();
    assert_eq!(retrieved, user);

    assert_eq!(database.dissolve().data.len(), 2)
}

#[test]
fn test_iter() {
    let db = ManualStorage::default();
    let mut database: Database<_, Manifest> = Database::new(db).unwrap();

    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    let pet_key = database.put(&pet).unwrap();

    let retrieved = database
        .iter_keys(PetKey(1)..PetKey(u64::MAX))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

    assert_eq!(retrieved, pet_key);
}

#[test]
fn test_iter_index() {
    let mut database: Database<_, Manifest> = Database::new(ManualStorage::default()).unwrap();

    let user = User {
        id: 42,
        name: "Al".to_string(),
        email: "alice@example.com".to_string(),
    };

    // Before inserting the user.
    let retrieved = database
        .iter_by_index(UserNameIndex("A".to_string())..UserNameIndex("Bob".to_string()))
        .unwrap()
        .collect::<Vec<_>>();
    assert!(retrieved.is_empty());

    // After inserting the user.
    database.insert(&user).unwrap();
    let retrieved = database
        .iter_by_index(UserNameIndex("A".to_string())..UserNameIndex("Bob".to_string()))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(retrieved, vec![UserKey(42)]);

    // After inserting the same user again.
    database.insert(&user).unwrap();
    let retrieved = database
        .iter_by_index(UserNameIndex("A".to_string())..UserNameIndex("Bob".to_string()))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(retrieved, vec![UserKey(42)]);
}
