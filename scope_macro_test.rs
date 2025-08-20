use kivis::{scope_impl, Scope};

// Define some example structs
#[derive(Debug)]
struct User {
    id: u64,
    name: String,
}

#[derive(Debug)]
struct Post {
    id: u64,
    title: String,
    content: String,
}

#[derive(Debug)]
struct Comment {
    id: u64,
    post_id: u64,
    content: String,
}

#[derive(Debug)]
struct Tag {
    id: u64,
    name: String,
}

// Use the macro to implement Scope for all these types
scope_impl![User, Post, Comment, Tag];

fn main() {
    // Test that the macro worked correctly
    println!("User SCOPE: {}", User::SCOPE); // Should be 0
    println!("Post SCOPE: {}", Post::SCOPE); // Should be 1
    println!("Comment SCOPE: {}", Comment::SCOPE); // Should be 2
    println!("Tag SCOPE: {}", Tag::SCOPE); // Should be 3

    // Test with single type
    struct SingleType;
    scope_impl![SingleType];
    println!("SingleType SCOPE: {}", SingleType::SCOPE); // Should be 0

    // Test with trailing comma
    struct A;
    struct B;
    scope_impl![A, B,];
    println!("A SCOPE: {}", A::SCOPE); // Should be 0
    println!("B SCOPE: {}", B::SCOPE); // Should be 1
}
