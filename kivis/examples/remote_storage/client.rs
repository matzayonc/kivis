// Client that communicates with the remote storage server via HTTP
// This demonstrates how to implement the Storage trait using HTTP requests

use bincode::config::Configuration;
use kivis::{BufferOverflowError, Repository, Storage};
use serde::{Deserialize, Serialize};
use std::ops::Range;

/// A client that connects to a remote storage server via HTTP
#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    client: reqwest::blocking::Client,
}

/// Error type for Client operations
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("Serialization error: {0:?}")]
    Serialization(#[from] bincode::error::EncodeError),
    #[error("Deserialization error: {0:?}")]
    Deserialization(#[from] bincode::error::DecodeError),
    #[error("JSON error: {0}")]
    Json(String),
    #[error("Server error: {0}")]
    Server(String),
    #[error("Buffer overflow error")]
    BufferOverflow(#[from] BufferOverflowError),
}

#[derive(Debug, Serialize, Deserialize)]
struct InsertRequest {
    key: String,
    value: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct GetResponse {
    value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemoveResponse {
    value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct KeysResponse {
    keys: Vec<String>,
}

impl Client {
    /// Create a new client connected to the specified server URL
    pub fn new(base_url: u16) -> Self {
        Self {
            base_url: format!("http://127.0.0.1:{}", base_url),
            client: reqwest::blocking::Client::new(),
        }
    }
}

impl Storage for Client {
    type KeyUnifier = Configuration;
    type ValueUnifier = Configuration;
}
impl Repository for Client {
    type K = [u8];
    type V = [u8];
    type Error = ClientError;
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let request = InsertRequest {
            key: hex::encode(key),
            value: hex::encode(value),
        };

        let response = self
            .client
            .post(format!("{}/insert", self.base_url))
            .json(&request)
            .send()
            .map_err(|e| ClientError::Http(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(ClientError::Server(format!(
                "Insert failed with status: {}",
                response.status()
            )))
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let key_hex = hex::encode(key);

        let response = self
            .client
            .get(format!("{}/get/{}", self.base_url, key_hex))
            .send()
            .map_err(|e| ClientError::Http(e.to_string()))?;

        if response.status().is_success() {
            let get_response: GetResponse = response
                .json()
                .map_err(|e| ClientError::Json(e.to_string()))?;

            Ok(get_response
                .value
                .and_then(|hex_val| hex::decode(&hex_val).ok()))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(ClientError::Server(format!(
                "Get failed with status: {}",
                response.status()
            )))
        }
    }

    fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let key_hex = hex::encode(key);

        let response = self
            .client
            .delete(format!("{}/remove/{}", self.base_url, key_hex))
            .send()
            .map_err(|e| ClientError::Http(e.to_string()))?;

        if response.status().is_success() {
            let remove_response: RemoveResponse = response
                .json()
                .map_err(|e| ClientError::Json(e.to_string()))?;

            Ok(remove_response
                .value
                .and_then(|hex_val| hex::decode(&hex_val).ok()))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(ClientError::Server(format!(
                "Remove failed with status: {}",
                response.status()
            )))
        }
    }

    fn iter_keys(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::Error>>, Self::Error> {
        // Use hex encoding for binary data to avoid URL encoding issues
        let start = hex::encode(&range.start);
        let end = hex::encode(&range.end);

        let response = self
            .client
            .get(format!("{}/keys/{}/{}", self.base_url, start, end))
            .send()
            .map_err(|e| ClientError::Http(e.to_string()))?;

        if response.status().is_success() {
            let keys_response: KeysResponse = response
                .json()
                .map_err(|e| ClientError::Json(e.to_string()))?;

            let keys: Vec<Result<Vec<u8>, ClientError>> = keys_response
                .keys
                .into_iter()
                .filter_map(|k| hex::decode(&k).ok())
                .map(Ok)
                .collect();

            Ok(keys.into_iter())
        } else {
            Err(ClientError::Server(format!(
                "Keys iteration failed with status: {}",
                response.status()
            )))
        }
    }
}
