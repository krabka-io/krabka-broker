//! The checked-in corpora under `bench/` that this crate's file suites load:
//! `bench/scenarios/` for `scenario_files` and `bench/soak/` for
//! `contract_files` and `soak_check_cli`.
//!
//! A suite declares `mod corpus;` and uses only part of it.

#![allow(dead_code)]

use std::{env, fs, path::PathBuf};

/// Where `bench/<name>` is, under whichever runner started this test.
///
/// Bazel is asked first, and that order matters. Both runners export
/// `CARGO_MANIFEST_DIR`, so reading it first answers for Bazel too, and under
/// Bazel it holds the package-relative `crates/bench-driver` rather than an
/// absolute path. Walking `..` out of that depends on the working directory and
/// on whether the runfiles entry is a symbolic link.
///
/// `TEST_SRCDIR` is set by Bazel alone and is absolute. Joined with
/// `TEST_WORKSPACE` it is the runfiles root, and Bazel stages a target's `data`
/// under it by package path, so `//bench:scenarios` is exactly
/// `bench/scenarios` there. No `..`, and no dependence on the working
/// directory.
///
/// Cargo sets no `TEST_SRCDIR`, and it runs a test with the crate directory as
/// the working directory, so the relative form is right there.
///
/// # Errors
///
/// When neither Bazel's pair nor Cargo's variable is set, which means the test
/// was launched by something that stages data differently again.
pub fn bench_dir(name: &str) -> Result<PathBuf, String> {
    if let Ok(srcdir) = env::var("TEST_SRCDIR") {
        let workspace = env::var("TEST_WORKSPACE")
            .map_err(|error| format!("TEST_WORKSPACE accompanies TEST_SRCDIR: {error}"))?;
        return Ok([srcdir.as_str(), workspace.as_str(), "bench", name]
            .iter()
            .collect());
    }
    let manifest = env::var("CARGO_MANIFEST_DIR").map_err(|error| {
        format!("TEST_SRCDIR (bazel) or CARGO_MANIFEST_DIR (cargo) must be set: {error}")
    })?;
    Ok([manifest.as_str(), "..", "..", "bench", name]
        .iter()
        .collect())
}

/// Every `*.yaml` file under [`bench_dir`]`(name)`, sorted so the trial order
/// does not depend on the file system.
///
/// `Path::is_file` reads the metadata of the link target, so a file that Bazel
/// staged as a symbolic link counts the same as a plain file.
///
/// # Errors
///
/// When the directory cannot be read or holds no YAML file. An empty corpus is
/// almost always a wrong path, and a harness that ran zero trials would pass
/// silently.
pub fn yaml_files(name: &str) -> Result<Vec<PathBuf>, String> {
    let root = bench_dir(name)?;
    let entries =
        fs::read_dir(&root).map_err(|error| format!("cannot read {}: {error}", root.display()))?;
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|error| format!("cannot read an entry in {}: {error}", root.display()))?
            .path();
        if path.extension().is_some_and(|ext| ext == "yaml") && path.is_file() {
            paths.push(path);
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err(format!("no YAML file found in {}", root.display()));
    }
    Ok(paths)
}
