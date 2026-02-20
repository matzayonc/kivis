use kivis::{BufferOverflowOr, Unifier};
use serde::{Serialize, de::DeserializeOwned};

/// A CSV-based serializer that encodes data for filesystem storage.
///
/// Serializes records to CSV format and encodes them for safe use as filenames.
/// This makes the stored data human-readable while ensuring filesystem compatibility.
#[derive(Debug, Clone, Copy, Default)]
pub struct CsvSerializer;

impl CsvSerializer {
    /// URL-encode a string to make it filesystem-safe while keeping it human-readable.
    ///
    /// Alphanumeric characters, hyphens, underscores, dots, and commas are preserved.
    /// All other characters are encoded as `%XX` where XX is the hexadecimal value.
    fn encode_for_filename(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | ',' => c.to_string(),
                _ => format!("%{:02X}", c as u32),
            })
            .collect()
    }

    /// Decode a URL-encoded string back to its original form.
    ///
    /// Returns `None` if the encoded string is malformed.
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

    fn serialize(
        &self,
        buffer: &mut Self::D,
        data: impl Serialize,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        let start = buffer.len();
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .quote_style(csv::QuoteStyle::Necessary)
            .from_writer(Vec::new());
        writer.serialize(data)?;
        writer.flush().map_err(Self::SerError::from)?;
        let bytes = writer
            .into_inner()
            .map_err(|e| csv::Error::from(std::io::Error::other(e.to_string())))?;
        let mut result = String::from_utf8_lossy(&bytes).into_owned();
        if result.ends_with('\n') {
            result.pop();
        }
        buffer.push_str(&Self::encode_for_filename(&result));
        Ok((start, buffer.len()))
    }

    fn deserialize<T: DeserializeOwned>(&self, data: &Self::D) -> Result<T, Self::DeError> {
        let decoded = Self::decode_from_filename(data).ok_or_else(|| {
            csv::Error::from(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid encoded data",
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
