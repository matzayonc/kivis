use fs_store::FileStore;
use kivis::{Database, DatabaseError, Record, manifest};

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
    cat: bool,
}

manifest![Park: User, Pet];

#[test]
fn test_flow() -> Result<(), DatabaseError<kivis::MemoryStorageError>> {
    const PATH: &str = "./data/fs-store";

    // Clean up any existing data for a fresh start
    let data_path = std::path::Path::new(PATH);
    if data_path.exists() {
        std::fs::remove_dir_all(data_path).ok();
    }

    // Create a new file-based database instance
    let file_store = FileStore::new(PATH).expect("Failed to create file store");
    let mut store: Database<_, Park> = Database::new(file_store);

    // Users can be added to the file store
    let alice = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let bob = User {
        name: "Bob".to_string(),
        email: "bob@example.com".to_string(),
    };

    let alice_key = store.put(alice.clone())?;
    let bob_key = store.put(bob.clone())?;

    // Pets can reference users as owners
    let fluffy = Pet {
        name: "Fluffy".to_string(),
        owner: alice_key.clone(),
        cat: true,
    };
    let rover = Pet {
        name: "Rover".to_string(),
        owner: bob_key.clone(),
        cat: false,
    };

    let fluffy_key = store.put(fluffy.clone())?;
    let _rover_key = store.put(rover.clone())?;

    // Retrieve records by key
    let retrieved_alice = store.get(&alice_key)?.unwrap();
    let retrieved_fluffy = store.get(&fluffy_key)?.unwrap();

    assert_eq!(retrieved_alice.name, "Alice");
    assert_eq!(retrieved_fluffy.name, "Fluffy");
    assert_eq!(retrieved_fluffy.owner, alice_key.clone());

    // Query by indexed fields
    let users_named_alice = store
        .iter_by_index(UserNameIndex("Alice".into())..UserNameIndex("Alicf".into()))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(users_named_alice, vec![alice_key.clone()]);

    // The data persists to files
    let data_dir = std::path::Path::new(PATH);
    if let Ok(entries) = std::fs::read_dir(data_dir) {
        let file_count = entries.count();
        println!("✓ File-based storage working - {file_count} files persisted");
    }

    Ok(())
}
