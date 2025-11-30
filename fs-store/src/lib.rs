use bincode::{config::Configuration, error::{DecodeError, EncodeError}};
use kivis::Storage;
use std::{fmt::Display, fs};
use std::path::PathBuf;

#[derive(Debug, PartialEq, Eq)]
pub enum FileStoreError {
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

pub struct FileStore {
    data_dir: PathBuf,
}

impl FileStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> std::io::Result<Self> {
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
