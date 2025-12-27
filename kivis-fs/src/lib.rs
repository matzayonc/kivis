use kivis::{Storage, Unifier};
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Display, fs, path::PathBuf};

#[derive(Debug)]
pub enum FileStoreError {
    Io(std::io::Error),
    Serialization(csv::Error),
}

impl PartialEq for FileStoreError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Io(a), Self::Io(b)) => a.kind() == b.kind() && a.to_string() == b.to_string(),
            (Self::Serialization(a), Self::Serialization(b)) => a.to_string() == b.to_string(),
            _ => false,
        }
    }
}

impl Eq for FileStoreError {}

impl Display for FileStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Serialization(e) => write!(f, "Serialization error: {}", e),
        }
    }
}

impl From<csv::Error> for FileStoreError {
    fn from(e: csv::Error) -> Self {
        Self::Serialization(e)
    }
}

impl From<std::io::Error> for FileStoreError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CsvSerializer;

impl CsvSerializer {
    /// URL-encode a string to make it filesystem-safe while keeping it human-readable.
    /// Commas and other special characters are encoded as %XX.
    fn encode_for_filename(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | ',' => c.to_string(),
                _ => format!("%{:02X}", c as u32),
            })
            .collect()
    }

    /// Decode a URL-encoded string back to its original form.
    fn decode_from_filename(encoded: &str) -> Option<String> {
        let mut result = String::new();
        let mut chars = encoded.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '%' {
                let hex: String = chars.by_ref().take(2).collect();
                if hex.len() == 2
                    && let Ok(code) = u32::from_str_radix(&hex, 16)
                    && let Some(decoded) = char::from_u32(code)
                {
                    result.push(decoded);
                    continue;
                }
                return None;
            }
            result.push(c);
        }
        Some(result)
    }
}

impl Unifier for CsvSerializer {
    type D = String;
    type SerError = csv::Error;
    type DeError = csv::Error;

    fn serialize_key(&self, data: impl Serialize) -> Result<Self::D, Self::SerError> {
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .quote_style(csv::QuoteStyle::Necessary)
            .from_writer(Vec::new());
        writer.serialize(data)?;
        writer.flush()?;
        let bytes = writer
            .into_inner()
            .map_err(|e| csv::Error::from(std::io::Error::other(e.to_string())))?;
        let mut result = String::from_utf8_lossy(&bytes).into_owned();
        if result.ends_with('\n') {
            result.pop();
        }
        Ok(Self::encode_for_filename(&result))
    }

    fn deserialize_key<T: DeserializeOwned>(&self, data: &Self::D) -> Result<T, Self::DeError> {
        let decoded = Self::decode_from_filename(data).ok_or_else(|| {
            csv::Error::from(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid encoded key",
            ))
        })?;

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(decoded.as_bytes());
        let mut iter = reader.deserialize();
        iter.next().ok_or_else(|| {
            csv::Error::from(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "no data",
            ))
        })?
    }
}

/// A file-based storage implementation that stores each key-value pair as a separate file.
/// Uses CSV serialization with URL-encoded filenames for human-readable storage.
#[derive(Debug)]
pub struct FileStore {
    data_dir: PathBuf,
}

impl FileStore {
    /// Creates a new FileStore instance at the specified directory.
    /// Creates the directory if it doesn't exist.
    pub fn new(data_dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let data_dir = data_dir.into();
        fs::create_dir_all(&data_dir)?;
        Ok(Self { data_dir })
    }

    fn key_to_filename(&self, key: &str) -> PathBuf {
        self.data_dir.join(format!("{key}.dat"))
    }

    fn filename_to_key(&self, filename: &str) -> Option<String> {
        filename.strip_suffix(".dat").map(String::from)
    }
}

impl Storage for FileStore {
    type Serializer = CsvSerializer;
    type StoreError = FileStoreError;

    fn insert(&mut self, key: String, value: String) -> Result<(), Self::StoreError> {
        let file_path = self.key_to_filename(&key);
        fs::write(file_path, value)?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>, Self::StoreError> {
        let file_path = self.key_to_filename(&key);
        match fs::read_to_string(file_path) {
            Ok(data) => Ok(Some(data)),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn remove(&mut self, key: String) -> Result<Option<String>, Self::StoreError> {
        let file_path = self.key_to_filename(&key);
        match fs::read_to_string(&file_path) {
            Ok(data) => {
                fs::remove_file(file_path)?;
                Ok(Some(data))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn iter_keys(
        &self,
        range: std::ops::Range<String>,
    ) -> Result<impl Iterator<Item = Result<String, Self::StoreError>>, Self::StoreError> {
        let entries = fs::read_dir(&self.data_dir)?;

        let mut keys: Vec<String> = Vec::new();
        for entry in entries.flatten() {
            if let Some(filename) = entry.file_name().to_str()
                && let Some(key) = self.filename_to_key(filename)
                && key >= range.start
                && key < range.end
            {
                keys.push(key);
            }
        }

        keys.sort();
        keys.reverse();
        Ok(keys.into_iter().map(Ok))
    }
}
