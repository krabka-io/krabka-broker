//! Locating repository fixtures under Cargo and Bazel.

use std::path::PathBuf;

/// The repository root, under Cargo or under a Bazel test sandbox.
///
/// # Panics
/// Panics if neither build system supplies its source-directory variables.
pub fn repo_root() -> PathBuf {
    if let Ok(dir) = std::env::var("CARGO_MANIFEST_DIR") {
        return PathBuf::from(dir).join("../..");
    }
    let srcdir = std::env::var("TEST_SRCDIR")
        .expect("CARGO_MANIFEST_DIR (cargo) or TEST_SRCDIR (bazel) must be set");
    let workspace =
        std::env::var("TEST_WORKSPACE").expect("TEST_WORKSPACE accompanies TEST_SRCDIR");
    PathBuf::from(srcdir).join(workspace)
}

/// Read a source-controlled text fixture below the repository root.
///
/// # Panics
/// Panics if the fixture cannot be read, retaining its full path in the diagnostic.
pub fn read_text(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

/// The checked-in JSON schema used by the reference and example config oracles.
///
/// # Panics
/// Panics if the schema cannot be read or parsed as JSON.
pub fn checked_in_schema() -> serde_json::Value {
    serde_json::from_str(&read_text("docs/config-schema.json"))
        .expect("docs/config-schema.json is JSON")
}
