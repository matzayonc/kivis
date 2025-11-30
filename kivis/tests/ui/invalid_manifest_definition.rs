use kivis::manifest;

struct User;
struct Post;

// This should fail - missing manifest name and colon
manifest![User, Post];

fn main() {}
