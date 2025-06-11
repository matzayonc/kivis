use std::{collections::BTreeMap, fmt::Display};

use kivis::{Indexed, Recordable};

// Define a record type for an User.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct UserKey(pub u64);
impl kivis::Recordable for User {
    const SCOPE: u8 = 1;
    type Key = UserKey;

    fn key(&self) -> Self::Key {
        UserKey(self.id)
    }

    fn index_keys(&self) -> Result<Vec<Vec<u8>>, bcs::Error> {
        Ok([bcs::to_bytes(&self.name)?].to_vec())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct UserNameIndex(pub String);
impl Indexed for UserNameIndex {
    type Key = UserKey;
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct User {
    id: u64,
    name: String,
    email: String,
}

// Define a record type for a Pet.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct PetKey(pub u64);
impl kivis::Recordable for Pet {
    const SCOPE: u8 = 2;
    type Key = PetKey;

    fn key(&self) -> Self::Key {
        PetKey(self.id)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct Pet {
    id: u64,
    name: String,
    owner: UserKey,
}

// Define storage for the database.
#[derive(Default)]
struct Storage {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}
#[derive(Debug, PartialEq, Eq)]
struct NoError;
impl Display for NoError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
impl kivis::RawStore for Storage {
    type StoreError = NoError;

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError> {
        self.data.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.data.get(key).cloned())
    }

    fn remove(&mut self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.data.remove(key))
    }

    fn iter_keys(
        &mut self,
        range: impl std::ops::RangeBounds<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError> {
        let iter = self.data.range(range);
        Ok(iter.map(|(k, _v)| Ok(k.clone())))
    }
}

#[test]
fn test_user_record() {
    let db = Storage::default();
    let mut database = kivis::Database::new(db);

    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    database.insert(user.clone()).unwrap();

    let retrieved: User = database.get(&user.key()).unwrap().unwrap();
    assert_eq!(retrieved, user);
}

#[test]
fn test_pet_record() {
    let db = Storage::default();
    let mut database = kivis::Database::new(db);

    let pet = Pet {
        id: 1,
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    database.insert(pet.clone()).unwrap();

    let retrieved: Pet = database.get(&pet.key()).unwrap().unwrap();
    assert_eq!(retrieved, pet);
}

#[test]
fn test_get_owner_of_pet() {
    let db = Storage::default();
    let mut database = kivis::Database::new(db);

    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let pet = Pet {
        id: 1,
        name: "Fido".to_string(),
        owner: user.key(),
    };

    database.insert(user.clone()).unwrap();
    database.insert(pet.clone()).unwrap();

    let retrieved: Pet = database.get(&pet.key()).unwrap().unwrap();
    assert_eq!(retrieved, pet);

    let owner: User = database.get(&pet.owner).unwrap().unwrap();
    assert_eq!(owner, user);
}

#[test]
fn test_index() {
    let db = Storage::default();
    let mut database = kivis::Database::new(db);

    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    database.insert(user.clone()).unwrap();

    let index_keys = user.index_keys().unwrap();
    assert_eq!(index_keys.len(), 1);
    assert_eq!(index_keys[0], bcs::to_bytes(&user.name.clone()).unwrap());

    let retrieved: User = database.get(&user.key()).unwrap().unwrap();
    assert_eq!(retrieved, user);

    assert_eq!(database.dissolve().data.len(), 2)
}

#[test]
fn test_iter() {
    let db = Storage::default();
    let mut database = kivis::Database::new(db);

    let pet = Pet {
        id: 42,
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    database.insert(pet.clone()).unwrap();

    let retrieved = database
        .iter_keys::<Pet>(&PetKey(1)..&PetKey(222))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

    assert_eq!(retrieved, PetKey(42));
}
