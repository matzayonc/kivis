#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/conflicting_key_strategies.rs");
    t.compile_fail("tests/ui/invalid_manifest_definition.rs");
    t.pass("tests/ui/no_std.rs");
}
