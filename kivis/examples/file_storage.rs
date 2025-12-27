use bincode::{
    config::Configuration,
    error::{DecodeError, EncodeError},
};
use kivis::{Database, DatabaseError, Record, Storage, manifest};
use std::path::PathBuf;
use std::{fmt::Display, fs};

#[derive(Debug, PartialEq, Eq)]
enum FileStoreError {
    Io,
    Serialization,
    Deserialization,
}

impl Display for FileStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io => write!(f, "IO error"),
            Self::Serialization => write!(f, "Serialization error"),
            Self::Deserialization => write!(f, "Deserialization error"),
        }
    }
}

impl From<EncodeError> for FileStoreError {
    fn from(_: EncodeError) -> Self {
        Self::Serialization
    }
}

impl From<DecodeError> for FileStoreError {
    fn from(_: DecodeError) -> Self {
        Self::Deserialization
    }
}

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

manifest![Manifest: User, Pet];

#[derive(Debug)]
struct FileStore {
    data_dir: PathBuf,
}

impl FileStore {
    fn new(data_dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let data_dir = data_dir.into();
        fs::create_dir_all(&data_dir)?;
        Ok(Self { data_dir })
    }

    fn key_to_filename(&self, key: &[u8]) -> PathBuf {
        let hex_key = hex::encode(key);
        self.data_dir.join(format!("{hex_key}.dat"))
    }
}

impl Storage for FileStore {
    type Serializer = Configuration;
    type StoreError = FileStoreError;

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Self::StoreError> {
        let file_path = self.key_to_filename(&key);
        fs::write(file_path, value).map_err(|_| FileStoreError::Io)?;
        Ok(())
    }

    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        let file_path = self.key_to_filename(&key);
        match fs::read(file_path) {
            Ok(data) => Ok(Some(data)),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(FileStoreError::Io),
        }
    }

    fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StoreError> {
        let file_path = self.key_to_filename(&key);
        match fs::read(&file_path) {
            Ok(data) => {
                fs::remove_file(file_path).map_err(|_| FileStoreError::Io)?;
                Ok(Some(data))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(FileStoreError::Io),
        }
    }

    fn iter_keys(
        &self,
        range: std::ops::Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError> {
        let entries = fs::read_dir(&self.data_dir).map_err(|_| FileStoreError::Io)?;

        let mut keys: Vec<Vec<u8>> = Vec::new();
        for entry in entries.flatten() {
            if let Some(filename) = entry.file_name().to_str()
                && let Some(hex_key) = filename.strip_suffix(".dat")
                && let Ok(key) = hex::decode(hex_key)
                && key >= range.start
                && key < range.end
            {
                keys.push(key);
            }
        }

        keys.sort();
        keys.reverse(); // Match the Reverse order used in MemoryStorage
        Ok(keys.into_iter().map(Ok))
    }
}

fn main() -> Result<(), DatabaseError<FileStore>> {
    // Clean up any existing data for a fresh start
    let data_path = std::path::Path::new("./data/example");
    if data_path.exists() {
        std::fs::remove_dir_all(data_path).ok();
    }

    // Create a new file-based database instance
    let file_store = FileStore::new("./data/example").expect("Failed to create file store");
    let mut store: Database<_, Manifest> = Database::new(file_store)?;

    // Users can be added to the file store
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

    let fluffy_key = store.put(&fluffy)?;
    let _rover_key = store.put(&rover)?;

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
    let data_dir = std::path::Path::new("./data/example");
    if let Ok(entries) = std::fs::read_dir(data_dir) {
        let file_count = entries.count();
        println!("✓ File-based storage working - {file_count} files persisted");
    }

    Ok(())
}
