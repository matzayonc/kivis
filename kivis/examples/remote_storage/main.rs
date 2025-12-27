// Example demonstrating a simple remote storage system with users and files
// This example shows:
// - Multiple related record types
// - Foreign key relationships
// - Indexed fields for queries
// - Basic CRUD operations
// - Separating schema into a module
// - Client-server architecture via HTTP
// - Custom Storage trait implementation using HTTP client
//
// This example starts an HTTP server in the background and connects to it via a client

mod client;
mod schema;
mod server;

use client::Client;
use kivis::{Database, DatabaseError, MemoryStorage};
use schema::*;
use std::thread;
use std::time::Duration;
use tokio::sync::oneshot;

fn main() -> Result<(), DatabaseError<Client>> {
    // Channel to receive the port number from the server thread
    let (tx, rx) = oneshot::channel();

    // Start the server in a background thread with its own tokio runtime
    thread::spawn(move || {
        let storage = MemoryStorage::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            // Bind to get port number before serving
            let app = server::create_router(storage);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();

            // Send the port to the main thread
            tx.send(port).unwrap();

            // Start serving (this runs forever)
            axum::serve(listener, app).await.unwrap();
        });
    });

    // Wait for the server to start and get the port number
    let port = rx
        .blocking_recv()
        .expect("Failed to receive port from server");

    // Give the server a moment to fully initialize
    thread::sleep(Duration::from_millis(500));

    println!("🚀 Server started on http://127.0.0.1:{}", port);

    // Create a new client that connects to the HTTP server on the random port
    let mut db: Database<_, RemoteStorageDatabase> = Database::new(Client::new(port))?;

    // Create users
    let alice = User {
        email: "alice@example.com".to_string(),
    };
    let bob = User {
        email: "bob@example.com".to_string(),
    };

    let alice_key = db.put(alice)?;
    let bob_key = db.put(bob)?;

    // Create files
    let file1 = File {
        owner: alice_key.clone(),
        content: "Hello from Alice's file!".to_string(),
    };
    let file2 = File {
        owner: bob_key.clone(),
        content: "Bob's document content here.".to_string(),
    };
    let file3 = File {
        owner: alice_key.clone(),
        content: "Another file from Alice.".to_string(),
    };

    let file1_key = db.put(file1.clone())?;
    let _file2_key = db.put(file2.clone())?;
    let file3_key = db.put(file3.clone())?;

    println!("✓ Created users and files");

    // Find user by email using index
    let alice_by_email = db
        .iter_by_index(
            UserEmailIndex("alice@example.com".into())..UserEmailIndex("alice@example.con".into()),
        )?
        .next()
        .unwrap()?;

    let retrieved_alice = db.get(&alice_by_email)?.unwrap();
    assert_eq!(retrieved_alice.email, "alice@example.com");

    // Retrieve a file with its owner information
    let file = db.get(&file1_key)?.unwrap();
    let file_owner = db.get(&file.owner)?.unwrap();
    assert_eq!(file_owner.email, "alice@example.com");

    println!("✓ Queried data using indexes and foreign keys");

    // Update a file (change content)
    db.remove(&file1_key)?;
    let mut updated_file = file1.clone();
    updated_file.content = "Updated content from Alice!".to_string();
    let updated_key = db.put(updated_file)?;

    // Delete a file
    db.remove(&file3_key)?;
    assert_eq!(db.get(&file3_key)?, None);

    // Verify the updated file still exists
    let final_file = db.get(&updated_key)?.unwrap();
    assert_eq!(final_file.content, "Updated content from Alice!");

    println!("✓ Updated and deleted files");

    println!(
        "\n✅ All operations performed via HTTP to server on port {}",
        port
    );

    Ok(())
}
