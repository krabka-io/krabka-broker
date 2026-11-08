//! Locating repository fixtures under Cargo and Bazel.

use std::{env, fs, path::PathBuf};

/// This crate's directory, wherever the test is running from.
///
/// Cargo exports `CARGO_MANIFEST_DIR` to a test process, so under Cargo this is
/// the path `env!` would have produced. It is read rather than expanded because
/// `env!` bakes an absolute build path into the binary, which ties the test to
/// the directory it was compiled in -- `rules_rust` rejects such a binary
/// outright, and under Cargo it only works when launched from that same path.
///
/// Bazel sets no such variable; it stages a target's `data` under
/// `$TEST_SRCDIR/$TEST_WORKSPACE/<package>`. Falling back to that is what lets
/// the TLS suites find their fixtures under both.
///
/// # Panics
///
/// Panics when neither Cargo's variable nor Bazel's pair is set, which means the
/// test was launched by something that stages fixtures differently again.
#[must_use]
pub fn manifest_dir() -> PathBuf {
    if let Ok(dir) = env::var("CARGO_MANIFEST_DIR") {
        return PathBuf::from(dir);
    }
    let srcdir = env::var("TEST_SRCDIR")
        .expect("CARGO_MANIFEST_DIR (cargo) or TEST_SRCDIR (bazel) must be set");
    let workspace = env::var("TEST_WORKSPACE").expect("TEST_WORKSPACE accompanies TEST_SRCDIR");
    PathBuf::from(srcdir).join(workspace).join("crates/broker")
}

/// The repository root, under Cargo or under a Bazel test sandbox: two levels
/// above [`manifest_dir`].
///
/// # Panics
/// Panics if neither build system supplies its source-directory variables.
pub fn repo_root() -> PathBuf { manifest_dir().join("../..") }

/// Read a source-controlled text fixture below the repository root.
///
/// # Panics
/// Panics if the fixture cannot be read, retaining its full path in the diagnostic.
pub fn read_text(relative: &str) -> String {
    let path = repo_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

/// The checked-in JSON schema used by the reference and example config oracles.
///
/// # Panics
/// Panics if the schema cannot be read or parsed as JSON.
pub fn checked_in_schema() -> serde_json::Value {
    serde_json::from_str(&read_text("docs/config-schema.json"))
        .expect("docs/config-schema.json is JSON")
}
