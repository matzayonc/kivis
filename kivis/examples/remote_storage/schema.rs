use kivis::{manifest, Record};

/// A user in the remote storage system
#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct User {
    #[index]
    pub email: String,
}

/// A file stored by a user
#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct File {
    pub owner: UserKey,
    pub content: String,
}

manifest![RemoteStorageDatabase: User, File];
