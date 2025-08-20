use kivis::{manifest, Scope};

// Define some test structs
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
manifest![User, Post, Comment, Tag];

#[test]
fn test_manifest_macro_multiple_types() {
    // Test that the macro correctly assigns scope values based on position
    assert_eq!(User::SCOPE, 0);
    assert_eq!(Post::SCOPE, 1);
    assert_eq!(Comment::SCOPE, 2);
    assert_eq!(Tag::SCOPE, 3);
}

#[test]
fn test_manifest_macro_single_type() {
    struct SingleType;
    manifest![SingleType];
    assert_eq!(SingleType::SCOPE, 0);
}

#[test]
fn test_manifest_macro_with_trailing_comma() {
    struct A;
    struct B;
    manifest![A, B,];
    assert_eq!(A::SCOPE, 0);
    assert_eq!(B::SCOPE, 1);
}

#[test]
fn test_manifest_macro_empty_struct() {
    struct EmptyStruct;
    manifest![EmptyStruct];
    assert_eq!(EmptyStruct::SCOPE, 0);
}

#[test]
fn test_manifest_macro_large_list() {
    struct T0; struct T1; struct T2; struct T3; struct T4;
    struct T5; struct T6; struct T7; struct T8; struct T9;
    
    manifest![T0, T1, T2, T3, T4, T5, T6, T7, T8, T9];
    
    assert_eq!(T0::SCOPE, 0);
    assert_eq!(T1::SCOPE, 1);
    assert_eq!(T2::SCOPE, 2);
    assert_eq!(T3::SCOPE, 3);
    assert_eq!(T4::SCOPE, 4);
    assert_eq!(T5::SCOPE, 5);
    assert_eq!(T6::SCOPE, 6);
    assert_eq!(T7::SCOPE, 7);
    assert_eq!(T8::SCOPE, 8);
    assert_eq!(T9::SCOPE, 9);
}
