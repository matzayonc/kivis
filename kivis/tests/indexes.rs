use anyhow::Context;
use bincode::serde::encode_to_vec;
use kivis::{Database, DatabaseEntry, Index, MemoryStorage, Record, manifest};

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
fn test_user_record() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let user = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let user_key = store.put(user.clone())?;

    let retrieved = store.get(&user_key)?.context("Missing")?;
    assert_eq!(retrieved, user);
    assert_eq!(user_key, UserKey(1));
    Ok(())
}

#[test]
fn test_pet_record() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    let pet_key = store.put(pet.clone())?;

    let retrieved = store.get(&pet_key)?.context("Missing")?;
    assert_eq!(retrieved, pet);
    Ok(())
}

#[test]
fn test_get_owner_of_pet() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let user = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let user_key = store.put(user.clone())?;
    let pet = Pet {
        name: "Fido".to_string(),
        owner: user_key.clone(),
    };
    let pet_key = store.put(pet.clone())?;

    let userr = store.get(&user_key)?.context("Missing")?;
    assert_eq!(user, userr);

    let retrieved = store.get(&pet_key)?.context("Missing")?;
    assert_eq!(retrieved, pet);

    let owner = store.get(&pet.owner)?.context("Missing")?;
    assert_eq!(owner, user);
    Ok(())
}

#[test]
fn test_index() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let user = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    let user_key = store.put(user.clone())?;

    let serializer = bincode::config::standard();
    let mut buffer = Vec::new();
    user.index_key(&mut buffer, UserNameIndex::INDEX, &serializer)?;
    assert_eq!(
        buffer,
        encode_to_vec(&user.name, bincode::config::standard()).context("Missing")?
    );

    let retrieved = store.get(&user_key)?.context("Missing")?;
    assert_eq!(retrieved, user);

    assert_eq!(store.dissolve().len(), 2);
    Ok(())
}

#[test]
fn test_keys_iter() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let pet = Pet {
        name: "Fido".to_string(),
        owner: UserKey(1),
    };

    store.put(pet)?;

    let retrieved = store
        .iter_keys(PetKey(0)..PetKey(u64::MAX))?
        .next()
        .context("Missing")??;

    assert_eq!(retrieved, PetKey(1));
    Ok(())
}

#[test]
fn test_iter_index() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let user = User {
        name: "Al".to_string(),
        email: "alice@example.com".to_string(),
    };

    store.put(user)?;

    let retrieved = store
        .iter_by_index(UserNameIndex("A".to_string())..UserNameIndex("Bob".to_string()))?
        .next()
        .transpose()?;
    let retrieved = retrieved.context("Missing")?;
    assert_eq!(retrieved, UserKey(1));
    Ok(())
}

#[test]
fn test_iter_index_exact() -> anyhow::Result<()> {
    let mut store = Database::<MemoryStorage, Manifest>::new(MemoryStorage::default())?;

    let names = [
        "Al", "Al", // The target name
        "Ak", "Am", // Previous and next names
        "Ala", "A",     // Shorter and longer names
        "Alice", // Alice for tradition
    ];
    for name in names {
        store.put(User {
            name: name.to_string(),
            email: format!("{}@example.com", name.to_lowercase()),
        })?;
    }

    let als = store
        .iter_by_index_exact(UserNameIndex("Al".into()))?
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(als.len(), 2);
    let al_1 = store.get(&als[0])?.context("Missing")?;
    let al_2 = store.get(&als[1])?.context("Missing")?;
    assert_eq!(al_1.name, "Al");
    assert_eq!(al_2.name, "Al");
    Ok(())
}
