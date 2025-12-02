//! File-based storage implementation for kivis database.
//!
//! This crate provides a simple, human-readable file-based storage backend for kivis.
//! Each key-value pair is stored as a separate file, with CSV serialization and
//! URL-encoded filenames for maximum compatibility and readability.
//!
//! # Example
//!
//! ```no_run
//! use kivis::Database;
//! use kivis_fs::FileStore;
//!
//! let file_store = FileStore::new("./data").expect("Failed to create storage");
//! let db = Database::new(file_store).expect("Failed to create database");
//! ```

use kivis::{Storage, Unifier};
use serde::{Serialize, de::DeserializeOwned};
use std::{error::Error, fmt::Display, fs, io, path::PathBuf};

/// Errors that can occur during file storage operations.
#[derive(Debug, PartialEq, Eq)]
pub enum FileStoreError {
    /// An I/O error occurred while reading or writing to the filesystem.
    Io(String),
    /// Failed to serialize data.
    Serialization(String),
    /// Failed to deserialize data.
    Deserialization(String),
}

impl Display for FileStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(msg) => write!(f, "I/O error: {msg}"),
            Self::Serialization(msg) => write!(f, "Serialization error: {msg}"),
            Self::Deserialization(msg) => write!(f, "Deserialization error: {msg}"),
        }
    }
}

impl Error for FileStoreError {}

impl From<csv::Error> for FileStoreError {
    fn from(error: csv::Error) -> Self {
        if error.is_io_error() {
            Self::Io(error.to_string())
        } else {
            Self::Serialization(error.to_string())
        }
    }
}

impl From<io::Error> for FileStoreError {
    fn from(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

/// CSV-based serializer for file storage keys and values.
///
/// Serializes data to CSV format and encodes it for safe use in filenames.
/// This allows for human-readable storage while maintaining filesystem compatibility.
#[derive(Debug, Clone, Copy, Default)]
pub struct CsvSerializer;

impl CsvSerializer {
    /// URL-encodes a string to make it filesystem-safe while keeping it human-readable.
    ///
    /// Special characters are encoded as `%XX` where XX is the hexadecimal Unicode code point.
    /// Alphanumeric characters, hyphens, underscores, dots, and commas are preserved.
    ///
    /// # Examples
    ///
    /// ```
    /// # use kivis_fs::CsvSerializer;
    /// assert_eq!(CsvSerializer::encode_for_filename("hello"), "hello");
    /// assert_eq!(CsvSerializer::encode_for_filename("hello world"), "hello world");
    /// assert_eq!(CsvSerializer::encode_for_filename("user@example.com"), "user%40example.com");
    /// ```
    pub fn encode_for_filename(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | ',' | ' ' => c.to_string(),
                _ => format!("%{:02X}", c as u32),
            })
            .collect()
    }

    /// Decodes a URL-encoded string back to its original form.
    ///
    /// Returns `None` if the encoded string is malformed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use kivis_fs::CsvSerializer;
    /// assert_eq!(
    ///     CsvSerializer::decode_from_filename("hello%20world"),
    ///     Some("hello world".to_string())
    /// );
    /// assert_eq!(CsvSerializer::decode_from_filename("invalid%"), None);
    /// ```
    pub fn decode_from_filename(encoded: &str) -> Option<String> {
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
            .map_err(|e| csv::Error::from(io::Error::other(e.to_string())))?;

        let mut result = String::from_utf8_lossy(&bytes).into_owned();

        // Remove trailing newline added by CSV writer
        if result.ends_with('\n') {
            result.pop();
        }

        Ok(Self::encode_for_filename(&result))
    }

    fn deserialize_key<T: DeserializeOwned>(&self, data: &Self::D) -> Result<T, Self::DeError> {
        let decoded = Self::decode_from_filename(data).ok_or_else(|| {
            csv::Error::from(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid encoded key",
            ))
        })?;

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(decoded.as_bytes());

        let mut iter = reader.deserialize();
        iter.next().ok_or_else(|| {
            csv::Error::from(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "no data in key",
            ))
        })?
    }
}

/// A file-based storage implementation for kivis database.
///
/// Each key-value pair is stored as a separate file in a designated directory.
/// Filenames are derived from URL-encoded keys with a `.dat` extension,
/// and file contents contain CSV-serialized values.
///
/// # Example
///
/// ```no_run
/// use kivis_fs::FileStore;
///
/// let store = FileStore::new("./my-data").expect("Failed to create storage");
/// ```
#[derive(Debug)]
pub struct FileStore {
    data_dir: PathBuf,
}

impl FileStore {
    /// The file extension used for all data files.
    const FILE_EXTENSION: &'static str = ".dat";

    /// Creates a new `FileStore` instance at the specified directory.
    ///
    /// The directory will be created if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created due to insufficient
    /// permissions or other I/O issues.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kivis_fs::FileStore;
    ///
    /// let store = FileStore::new("./data")?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn new(data_dir: impl Into<PathBuf>) -> io::Result<Self> {
        let data_dir = data_dir.into();
        fs::create_dir_all(&data_dir)?;
        Ok(Self { data_dir })
    }

    /// Converts a serialized key to a file path.
    fn key_to_filename(&self, key: &str) -> PathBuf {
        self.data_dir.join(format!("{key}{}", Self::FILE_EXTENSION))
    }

    /// Extracts a key from a filename, removing the file extension.
    fn filename_to_key(&self, filename: &str) -> Option<String> {
        filename
            .strip_suffix(Self::FILE_EXTENSION)
            .map(String::from)
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
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
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
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn scan_keys(
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

        // Sort in descending order for consistent iteration
        keys.sort_unstable();
        keys.reverse();

        Ok(keys.into_iter().map(Ok))
    }
}
