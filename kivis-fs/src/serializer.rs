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
    /// All other characters are percent-encoded using UTF-8 bytes.
    fn encode_for_filename(s: &str) -> String {
        let mut result = String::new();
        for c in s.chars() {
            match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | ',' => {
                    result.push(c);
                }
                _ => {
                    // Encode as UTF-8 bytes
                    let mut buf = [0u8; 4];
                    let bytes = c.encode_utf8(&mut buf).as_bytes();
                    for &byte in bytes {
                        result.push_str(&format!("%{:02X}", byte));
                    }
                }
            }
        }
        result
    }

    /// Decode a URL-encoded string back to its original form.
    ///
    /// Returns `None` if the encoded string is malformed.
    fn decode_from_filename(encoded: &str) -> Option<String> {
        let mut result = Vec::new();
        let mut chars = encoded.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '%' {
                let hex: String = chars.by_ref().take(2).collect();
                if hex.len() == 2
                    && let Ok(byte) = u8::from_str_radix(&hex, 16)
                {
                    result.push(byte);
                    continue;
                }
                return None;
            }
            // Safe character - encode as UTF-8
            let mut buf = [0u8; 4];
            let bytes = c.encode_utf8(&mut buf).as_bytes();
            result.extend_from_slice(bytes);
        }
        String::from_utf8(result).ok()
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Property test: encoding and then decoding should return the original string
        #[test]
        fn encode_decode_roundtrip(s in "\\PC*") {
            let encoded = CsvSerializer::encode_for_filename(&s);
            let decoded = CsvSerializer::decode_from_filename(&encoded);
            prop_assert_eq!(decoded, Some(s));
        }

        /// Property test: safe characters should remain unchanged
        #[test]
        fn safe_chars_unchanged(s in "[a-zA-Z0-9_.,\\-]+") {
            let encoded = CsvSerializer::encode_for_filename(&s);
            prop_assert_eq!(encoded, s);
        }

        /// Property test: encoding should always produce decodable output
        #[test]
        fn encoding_always_decodable(s in "\\PC*") {
            let encoded = CsvSerializer::encode_for_filename(&s);
            prop_assert!(CsvSerializer::decode_from_filename(&encoded).is_some());
        }
    }

    #[test]
    fn test_encode_special_chars() {
        assert_eq!(
            CsvSerializer::encode_for_filename("hello world"),
            "hello%20world"
        );
        assert_eq!(
            CsvSerializer::encode_for_filename("test@file"),
            "test%40file"
        );
        assert_eq!(
            CsvSerializer::encode_for_filename("file/path"),
            "file%2Fpath"
        );
    }

    #[test]
    fn test_decode_special_chars() {
        assert_eq!(
            CsvSerializer::decode_from_filename("hello%20world"),
            Some("hello world".to_string())
        );
        assert_eq!(
            CsvSerializer::decode_from_filename("test%40file"),
            Some("test@file".to_string())
        );
        assert_eq!(
            CsvSerializer::decode_from_filename("file%2Fpath"),
            Some("file/path".to_string())
        );
    }

    #[test]
    fn test_decode_invalid() {
        assert_eq!(CsvSerializer::decode_from_filename("test%"), None);
        assert_eq!(CsvSerializer::decode_from_filename("test%G"), None);
        assert_eq!(CsvSerializer::decode_from_filename("test%GG"), None);
    }

    #[test]
    fn test_safe_chars() {
        let safe = "test_file-123.csv,data";
        assert_eq!(CsvSerializer::encode_for_filename(safe), safe);
        assert_eq!(
            CsvSerializer::decode_from_filename(safe),
            Some(safe.to_string())
        );
    }
}
