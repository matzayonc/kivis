use kivis::{Database, DatabaseError, MemoryStorage, Record, Recordable};

/// A user record with an indexed name field
#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[table(1)]
pub struct User {
    #[index]
    name: String,
    email: String,
}

/// A pet record that references a user as its owner
#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[table(2)]
struct Pet {
    name: String,
    owner: UserKey,
    #[index]
    favourite_toy: ToyKey,
}

#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[table(2)]
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

fn main() -> Result<(), DatabaseError<kivis::MemoryStorageError>> {
    // Create a new in-memory database instance
    let mut store = Database::new(MemoryStorage::new());

    // Users can be added to a store.
    let alice = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let bob = User {
        name: "Bob".to_string(),
        email: "bob@example.com".to_string(),
    };
    let alice_key = store.insert(alice)?;
    let bob_key = store.insert(bob)?;

    let toy = Toy {
        kind: ToyKind::Ball,
        color: 7,
    };
    let alex = Pet {
        name: "Alex".to_string(),
        owner: alice_key,
        favourite_toy: toy.maybe_key().unwrap(), // TODO: Shouldn't have to unwrap.
    };
    store.insert(alex)?;
    store.insert(toy)?;

    // Records can be retrieved by indexed name
    let users_named_bob = store
        .iter_by_index(UserNameIndex("Bob".into())..UserNameIndex("Boba".into()))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(users_named_bob, vec![bob_key]);

    // Pets by their favourite type of toy.
    // let pet = store
    //     .iter_by_index(
    //         PetFavouriteToyIndex(ToyKey(ToyKind::Ball, 7))
    //             ..PetFavouriteToyIndex(ToyKey(ToyKind::Ball, 8)),
    //     )?
    //     .next()
    //     .unwrap()?;
    // let pet = store.get::<Pet>(&pet)?.unwrap(); // TODO: Shouldn't have to specify type
    // let owner = store.get::<User>(&pet.owner)?.unwrap();
    // assert_eq!(owner.name, "Alice");

    Ok(())
}
