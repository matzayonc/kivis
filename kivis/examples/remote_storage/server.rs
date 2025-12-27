// Server module that exposes MemoryStorage operations via HTTP API using Axum
// This demonstrates how to create a remote storage server

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
};
use kivis::MemoryStorage;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// Shared state containing the storage backend
type SharedStorage = Arc<Mutex<MemoryStorage>>;

/// Request body for insert operations
#[derive(Debug, Serialize, Deserialize)]
pub struct InsertRequest {
    pub key: String,
    pub value: String,
}

/// Response body for get operations
#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponse {
    pub value: Option<String>,
}

/// Response body for remove operations
#[derive(Debug, Serialize, Deserialize)]
pub struct RemoveResponse {
    pub value: Option<String>,
}

/// Response body for key iteration
#[derive(Debug, Serialize, Deserialize)]
pub struct KeysResponse {
    pub keys: Vec<String>,
}

/// Create a new Axum router with storage endpoints
pub fn create_router(storage: MemoryStorage) -> Router {
    let shared_storage = Arc::new(Mutex::new(storage));

    Router::new()
        .route("/insert", post(insert_handler))
        .route("/get/:key", get(get_handler))
        .route("/remove/:key", delete(remove_handler))
        .route("/keys/:start/:end", get(keys_handler))
        .with_state(shared_storage)
}

/// Handler for inserting key-value pairs
async fn insert_handler(
    State(storage): State<SharedStorage>,
    Json(request): Json<InsertRequest>,
) -> impl IntoResponse {
    // Decode hex-encoded key and value
    let key = match hex::decode(&request.key) {
        Ok(k) => k,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid hex key"),
    };
    let value = match hex::decode(&request.value) {
        Ok(v) => v,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid hex value"),
    };

    let mut storage = storage.lock().unwrap();

    // Use the Storage trait's insert method
    match kivis::Storage::insert(&mut *storage, key, value) {
        Ok(_) => (StatusCode::OK, "Inserted successfully"),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to insert"),
    }
}

/// Handler for getting values by key
async fn get_handler(
    State(storage): State<SharedStorage>,
    Path(key_hex): Path<String>,
) -> impl IntoResponse {
    let key = match hex::decode(&key_hex) {
        Ok(k) => k,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(GetResponse { value: None })),
    };

    let storage = storage.lock().unwrap();

    match kivis::Storage::get(&*storage, key) {
        Ok(Some(value)) => {
            let value_hex = hex::encode(&value);
            (
                StatusCode::OK,
                Json(GetResponse {
                    value: Some(value_hex),
                }),
            )
        }
        Ok(None) => (StatusCode::NOT_FOUND, Json(GetResponse { value: None })),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(GetResponse { value: None }),
        ),
    }
}

/// Handler for removing values by key
async fn remove_handler(
    State(storage): State<SharedStorage>,
    Path(key_hex): Path<String>,
) -> impl IntoResponse {
    let key = match hex::decode(&key_hex) {
        Ok(k) => k,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(RemoveResponse { value: None }),
            );
        }
    };

    let mut storage = storage.lock().unwrap();

    match kivis::Storage::remove(&mut *storage, key) {
        Ok(Some(value)) => {
            let value_hex = hex::encode(&value);
            (
                StatusCode::OK,
                Json(RemoveResponse {
                    value: Some(value_hex),
                }),
            )
        }
        Ok(None) => (StatusCode::NOT_FOUND, Json(RemoveResponse { value: None })),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RemoveResponse { value: None }),
        ),
    }
}

/// Handler for iterating keys in a range
async fn keys_handler(
    State(storage): State<SharedStorage>,
    Path((start, end)): Path<(String, String)>,
) -> impl IntoResponse {
    // Decode hex-encoded keys
    let start_key = match hex::decode(&start) {
        Ok(k) => k,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(KeysResponse { keys: vec![] })),
    };
    let end_key = match hex::decode(&end) {
        Ok(k) => k,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(KeysResponse { keys: vec![] })),
    };

    let storage = storage.lock().unwrap();

    // Collect keys while we still hold the lock
    let keys_result: Result<Vec<String>, kivis::MemoryStorageError> = (|| {
        let iter = kivis::Storage::iter_keys(&*storage, start_key..end_key)?;
        let keys: Vec<String> = iter
            .filter_map(|result| result.ok())
            .map(|key| hex::encode(&key))
            .collect();
        Ok(keys)
    })();

    match keys_result {
        Ok(keys) => (StatusCode::OK, Json(KeysResponse { keys })),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(KeysResponse { keys: vec![] }),
        ),
    }
}
