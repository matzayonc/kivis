use kivis::{manifest, Database, DatabaseError, DeriveKey, MemoryStorage, Record};

/// A user record with an indexed name field
#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct User {
    #[index]
    name: String,
    email: String,
}

/// A pet record that references a user as its owner
#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
struct Pet {
    name: String,
    owner: UserKey,
    #[index]
    favourite_toy: ToyKey,
}

#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
struct Toy {
    #[key]
    kind: ToyKind,
    #[key]
    color: u8,
}
#[derive(
    Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
enum ToyKind {
    #[default]
    Ball,
    Mouse,
}

manifest![Pets: User, Pet, Toy];

fn main() -> Result<(), DatabaseError<kivis::MemoryStorageError>> {
    // Create a new in-memory database instance
    let mut store: Database<_, Pets> = Database::new(MemoryStorage::new()).unwrap();

    // Users can be added to a store.
    let alice = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let bob = User {
        name: "Bob".to_string(),
        email: "bob@example.com".to_string(),
    };
    let alice_key = store.put(&alice)?;
    let bob_key = store.put(&bob)?;

    let toy = Toy {
        kind: ToyKind::Ball,
        color: 7,
    };
    let alex = Pet {
        name: "Alex".to_string(),
        owner: alice_key,
        favourite_toy: Toy::key(&toy),
    };
    store.put(&alex)?;
    store.insert(&toy)?;

    // Records can be retrieved by indexed name
    let users_named_bob = store
        .iter_by_index(UserNameIndex("Bob".into())..UserNameIndex("Boba".into()))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(users_named_bob, vec![bob_key]);

    // Pets by their favourite type of toy.
    let pet = store
        .iter_by_index(
            PetFavouriteToyIndex(ToyKey(ToyKind::Ball, 7))
                ..PetFavouriteToyIndex(ToyKey(ToyKind::Ball, 8)),
        )?
        .next()
        .unwrap()?;
    let pet = store.get(&pet)?.unwrap();
    let owner = store.get(&pet.owner)?.unwrap();

    assert_eq!(owner.name, "Alice");

    Ok(())
}
