use anyhow::Context;
use bincode::{
    config::Configuration,
    error::{DecodeError, EncodeError},
    serde::encode_to_vec,
};
use std::{collections::BTreeMap, fmt::Display, ops::Range};

use kivis::{
    Database, DatabaseEntry, DeriveKey, Incrementable, Index, IndexBuilder, RecordKey, Scope,
    Storage,
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
    const INDEX_COUNT_HINT: usize = 1;
    fn index_keys<I: kivis::Indexer>(&self, indexer: &mut I) -> Result<(), I::Error> {
        indexer.add(1, &self.name)?;
        Ok(())
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
    ) -> Result<(), kivis::DatabaseError<S>> {
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
#[derive(Debug, Default)]
struct ManualStorage {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}
#[derive(Debug)]
enum NoError {
    Serialization(EncodeError),
    Deserialization(DecodeError),
}

impl Display for NoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization(e) => write!(f, "Serialization error: {e:?}"),
            Self::Deserialization(e) => write!(f, "Deserialization error: {e:?}"),
        }
    }
}

impl PartialEq for NoError {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Serialization(_), Self::Serialization(_))
                | (Self::Deserialization(_), Self::Deserialization(_))
        )
    }
}

impl Eq for NoError {}

impl From<EncodeError> for NoError {
    fn from(e: EncodeError) -> Self {
        Self::Serialization(e)
    }
}

impl From<DecodeError> for NoError {
    fn from(e: DecodeError) -> Self {
        Self::Deserialization(e)
    }
}

impl Storage for ManualStorage {
    type Serializer = Configuration;
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
fn test_user_record() -> anyhow::Result<()> {
    let db = ManualStorage::default();
    let mut database: Database<_, Manifest> = Database::new(db)?;

    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    database.insert(&user)?;

    let retrieved = database.get(&UserKey(user.id))?.context("Missing")?;
    assert_eq!(retrieved, user);
    Ok(())
}

#[test]
fn test_pet_record() -> anyhow::Result<()> {
    let db = ManualStorage::default();
    let mut database: Database<_, Manifest> = Database::new(db)?;

    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    let pet_key = database.put(&pet)?;

    let retrieved: Option<Pet> = database.get(&pet_key)?;
    let retrieved = retrieved.context("Missing")?;
    assert_eq!(retrieved, pet);
    Ok(())
}

#[test]
fn test_get_owner_of_pet() -> anyhow::Result<()> {
    let db = ManualStorage::default();
    let mut database: Database<_, Manifest> = Database::new(db)?;

    let mut user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(user.id),
    };

    user.id = database.insert(&user)?.0;
    let pet_key = database.put(&pet)?;

    let retrieved = database.get(&pet_key)?.context("Missing")?;
    assert_eq!(retrieved, pet);

    let owner = database.get(&pet.owner)?.context("Missing")?;
    assert_eq!(owner, user);
    Ok(())
}

#[test]
fn test_index() -> anyhow::Result<()> {
    let db = ManualStorage::default();
    let mut database: Database<_, Manifest> = Database::new(db)?;

    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    let _user_key = database.insert(&user)?;

    let mut indexer = IndexBuilder::new(bincode::config::standard());
    user.index_keys(&mut indexer)?;
    let index_keys = indexer.into_index_keys();
    assert_eq!(index_keys.len(), 1);
    assert_eq!(index_keys[0].0, 1);
    assert_eq!(
        index_keys[0].1,
        encode_to_vec(&user.name, bincode::config::standard()).context("Missing")?,
    );

    let retrieved = database.get(&UserKey(user.id))?.context("Missing")?;
    assert_eq!(retrieved, user);

    assert_eq!(database.dissolve().data.len(), 2);
    Ok(())
}

#[test]
fn test_iter() -> anyhow::Result<()> {
    let db = ManualStorage::default();
    let mut database: Database<_, Manifest> = Database::new(db)?;

    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    let pet_key = database.put(&pet)?;

    let retrieved = database
        .iter_keys(PetKey(1)..PetKey(u64::MAX))?
        .next()
        .context("Missing")??;

    assert_eq!(retrieved, pet_key);
    Ok(())
}

#[test]
fn test_iter_index() -> anyhow::Result<()> {
    let mut database: Database<_, Manifest> = Database::new(ManualStorage::default())?;

    let user = User {
        id: 42,
        name: "Al".to_string(),
        email: "alice@example.com".to_string(),
    };

    // Before inserting the user.
    let retrieved = database
        .iter_by_index(UserNameIndex("A".to_string())..UserNameIndex("Bob".to_string()))?
        .collect::<Vec<_>>();
    assert!(retrieved.is_empty());

    // After inserting the user.
    database.insert(&user)?;
    let retrieved = database
        .iter_by_index(UserNameIndex("A".to_string())..UserNameIndex("Bob".to_string()))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(retrieved, vec![UserKey(42)]);

    // After inserting the same user again.
    database.insert(&user)?;
    let retrieved = database
        .iter_by_index(UserNameIndex("A".to_string())..UserNameIndex("Bob".to_string()))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(retrieved, vec![UserKey(42)]);
    Ok(())
}
