#[test]
fn ui() {
    let t = trybuild::TestCases::new();

    // Test that valid code compiles
    t.pass("tests/ui/valid_single_table.rs");
    t.pass("tests/ui/valid_multiple_tables.rs");

    // Test that duplicate table IDs cause compile errors
    t.compile_fail("tests/ui/duplicate_table_ids.rs");
}
