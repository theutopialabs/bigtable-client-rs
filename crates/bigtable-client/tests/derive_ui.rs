//! Compile-time derive contract tests.

#[test]
fn derive_accepts_supported_shapes_and_rejects_invalid_attributes() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/pass/*.rs");
    tests.compile_fail("tests/ui/fail/*.rs");
}
