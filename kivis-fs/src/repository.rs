use kivis::Repository;
use std::{fs, path::PathBuf};

use crate::error::FileStoreError;

/// A file-based storage implementation that stores each key-value pair as a separate file.
///
/// Uses CSV serialization with URL-encoded filenames for human-readable storage.
/// Each record is stored in a `.dat` file within the configured directory.
#[derive(Debug)]
pub struct FileStore {
    /// The directory where all data files are stored.
    data_dir: PathBuf,
}

impl Repository for FileStore {
    type K = String;
    type V = String;
    type Error = FileStoreError;

    fn insert_entry(&mut self, key: &str, value: &str) -> Result<(), Self::Error> {
        let file_path = self.key_to_filename(key);
        fs::write(file_path, value)?;
        Ok(())
    }

    fn get_entry(&self, key: &str) -> Result<Option<Self::V>, Self::Error> {
        let file_path = self.key_to_filename(key);
        match fs::read_to_string(file_path) {
            Ok(data) => Ok(Some(data)),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn remove_entry(&mut self, key: &str) -> Result<Option<Self::V>, Self::Error> {
        let file_path = self.key_to_filename(key);
        match fs::read_to_string(&file_path) {
            Ok(data) => {
                fs::remove_file(file_path)?;
                Ok(Some(data))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Keys in `range`, ascending (`start` inclusive, `end` exclusive).
    ///
    /// The directory is listed on every call; this backend is meant for small data sets.
    // A `Vec` iterator is double-ended; refining the return type keeps this compatible
    // with the core once `Repository::scan_range` requires `DoubleEndedIterator`.
    #[allow(refining_impl_trait)]
    fn scan_range(
        &self,
        range: std::ops::Range<Self::K>,
    ) -> Result<impl DoubleEndedIterator<Item = Result<Self::K, Self::Error>>, Self::Error> {
        let mut keys: Vec<String> = Vec::new();
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            if let Some(filename) = entry.file_name().to_str()
                && let Some(key) = self.filename_to_key(filename)
                && key >= range.start
                && key < range.end
            {
                keys.push(key);
            }
        }

        keys.sort();
        Ok(keys.into_iter().map(Ok))
    }
}

impl FileStore {
    /// Creates a new FileStore instance at the specified directory.
    ///
    /// Creates the directory if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created.
    pub fn new(data_dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let data_dir = data_dir.into();
        fs::create_dir_all(&data_dir)?;
        Ok(Self { data_dir })
    }

    /// Converts a key string to a filesystem path.
    ///
    /// Every stored key ends with the [`KeyCodec`](crate::KeyCodec) terminator `.`, which
    /// doubles as the dot of the `.dat` extension so names read as `<key>dat`
    /// rather than `<key>..dat`.
    fn key_to_filename(&self, key: &str) -> PathBuf {
        let separator = if key.ends_with('.') { "" } else { "." };
        self.data_dir.join(format!("{key}{separator}dat"))
    }

    /// Extracts the key from a filename by removing the `dat` extension (keeping the dot,
    /// which belongs to the key).
    fn filename_to_key(&self, filename: &str) -> Option<String> {
        filename
            .strip_suffix("dat")
            .filter(|key| key.ends_with('.'))
            .map(String::from)
    }
}
