use kivis::{Record, manifest};
use serde::{Deserialize, Serialize};

#[derive(Record, Debug, Clone, Serialize, Deserialize)]
struct User {
    name: u16,
}

#[derive(Record, Debug, Clone, Serialize, Deserialize)]
struct Post {
    title: u16,
}

#[derive(Record, Debug, Clone, Serialize, Deserialize)]
struct Comment {
    content: u16,
}

// Test the manifest macro with multiple types
manifest![TestManifest: User, Post, Comment];

#[test]
fn test_manifest_fields() {
    // Create an instance of the manifest
    let manifest = TestManifest {
        last_user: Some(UserKey(1)),
        last_post: Some(PostKey(2)),
        last_comment: Some(CommentKey(3)),
    };

    // Test that the fields exist and work
    assert!(manifest.last_user.is_some());
    assert!(manifest.last_post.is_some());
    assert!(manifest.last_comment.is_some());

    println!("Manifest created successfully with fields!");
}

#[test]
fn test_empty_manifest() {
    // Test the empty case
    manifest![EmptyManifest:];

    let _empty = EmptyManifest;
    println!("Empty manifest created successfully!");
}
