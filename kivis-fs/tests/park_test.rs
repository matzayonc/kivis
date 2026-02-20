use kivis::{Database, Lexicographic, Record, manifest};
use kivis_fs::FileStore;
use tempfile::tempdir;

/// A user record with an indexed name field
#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct User {
    #[index]
    name: Lexicographic<String>,
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
fn test_flow() -> anyhow::Result<()> {
    // Create a temporary directory for the test
    let temp_dir = tempdir()?;
    let data_path = temp_dir.path();

    // Create a new file-based database instance
    let file_store = FileStore::new(data_path).expect("Failed to create file store");
    let mut store: Database<_, Park> = Database::new(file_store)?;

    // Users can be added to the file store
    let alice = User {
        name: "Alice".into(),
        email: "alice@example.com".to_string(),
    };
    let bob = User {
        name: "Bob".into(),
        email: "bob@example.com".into(),
    };

    let alice_key = store.put(alice)?;
    let bob_key = store.put(bob)?;

    // Pets can reference users as owners
    let fluffy = Pet {
        name: "Fluffy".into(),
        owner: alice_key.clone(),
        cat: true,
    };
    let rover = Pet {
        name: "Rover".into(),
        owner: bob_key.clone(),
        cat: false,
    };

    let fluffy_key = store.put(fluffy)?;
    let _rover_key = store.put(rover)?;

    // Retrieve records by key
    let retrieved_alice = store.get(&alice_key)?.expect("Alice not found");
    let retrieved_fluffy = store.get(&fluffy_key)?.expect("Fluffy not found");

    assert_eq!(retrieved_alice.name, "Alice");
    assert_eq!(retrieved_fluffy.name, "Fluffy");
    assert_eq!(retrieved_fluffy.owner, alice_key.clone());

    // Query by indexed fields
    let users_named_alice = store
        .iter_by_index(UserNameIndex("Alice".into())..UserNameIndex("Bob".into()))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(users_named_alice, vec![alice_key.clone()]);

    // The data persists to files
    if let Ok(entries) = std::fs::read_dir(data_path) {
        let file_count = entries.count();
        println!("✓ File-based storage working - {file_count} files persisted");
    }

    Ok(())
}
