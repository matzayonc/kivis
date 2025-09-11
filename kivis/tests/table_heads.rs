use kivis::{manifest, Database, MemoryStorage, Record, TableHeads};

#[derive(Record, Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct User {
    name: String,
    email: String,
}

#[derive(Record, Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Post {
    title: String,
    content: String,
    author: UserKey,
}

manifest![TestManifest: User, Post];

#[test]
fn test_table_heads_basic() {
    let db: Database<MemoryStorage, TestManifest> = Database::default();
    let mut table_heads = TableHeads::new(&db).unwrap();

    // Load the heads for User and Post tables
    table_heads.load_head::<User, _>(&db).unwrap();
    table_heads.load_head::<Post, _>(&db).unwrap();

    // Now we can get next IDs without database reads
    let user_id1 = table_heads.next_id::<User>().unwrap();
    let user_id2 = table_heads.next_id::<User>().unwrap();
    let post_id1 = table_heads.next_id::<Post>().unwrap();

    // IDs should be different
    assert_ne!(user_id1, user_id2);

    // Create some records with the pre-allocated IDs
    let _user1 = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    let _user2 = User {
        name: "Bob".to_string(),
        email: "bob@example.com".to_string(),
    };
    let _post1 = Post {
        title: "Hello World".to_string(),
        content: "First post!".to_string(),
        author: user_id1.clone(),
    };

    // Insert them with pre-allocated IDs (would use transaction in real usage)
    // This would be done differently in the actual implementation
    // but this shows the concept

    println!("Pre-allocated User ID 1: {:?}", user_id1);
    println!("Pre-allocated User ID 2: {:?}", user_id2);
    println!("Pre-allocated Post ID 1: {:?}", post_id1);
}

#[test]
fn test_table_heads_refresh() {
    let mut db: Database<MemoryStorage, TestManifest> = Database::default();
    let mut table_heads = TableHeads::new(&db).unwrap();

    // Load the head for User table
    table_heads.load_head::<User, _>(&db).unwrap();

    // Add a user to the database outside of TableHeads
    let user = User {
        name: "Charlie".to_string(),
        email: "charlie@example.com".to_string(),
    };
    let _user_key = db.put(user).unwrap();

    // Refresh the head to account for the externally added record
    table_heads.refresh_head::<User, _>(&db).unwrap();

    // Get the next ID - it should account for the record we added
    let next_user_id = table_heads.next_id::<User>().unwrap();

    println!("Next User ID after refresh: {:?}", next_user_id);
}
