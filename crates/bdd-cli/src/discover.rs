//! Finding the feature files under the paths a command names, and the paths
//! each one is known by.

use crate::fs::{self, EntryKind, FileSystem};

/// The manifest that marks a crate's directory.
const MANIFEST: &str = "Cargo.toml";

/// Directory names a walk does not enter: build output, and hidden
/// directories such as `.git`.
const SKIPPED_DIR: &str = "target";

/// One feature file and the paths the commands name it by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureFile {
    /// The absolute path.
    pub absolute: String,
    /// The path relative to the current directory, as output shows it.
    pub shown: String,
    /// The absolute path of the nearest ancestor directory that holds a
    /// `Cargo.toml`, or the file's own directory when none does.
    pub manifest_dir: String,
    /// The path relative to [`manifest_dir`](Self::manifest_dir), as
    /// `krabka_bdd::scenarios!` sees it.
    pub lint_path: String,
}

/// Every `.feature` file under `inputs`, each a file or a directory taken
/// from `cwd`, sorted by shown path and without repeats.
///
/// # Errors
///
/// Returns a message for an input that does not exist, a named file that is
/// not a `.feature` file, or a directory that cannot be listed.
pub fn discover(
    filesystem: &dyn FileSystem,
    cwd: &str,
    inputs: &[String],
) -> Result<Vec<FeatureFile>, String> {
    let mut found = Vec::new();
    for input in inputs {
        let path = fs::absolute(cwd, input);
        match filesystem.kind(&path) {
            None => return Err(format!("{input}: no such file or directory")),
            Some(EntryKind::File) if !is_feature(&path) => {
                return Err(format!("{input}: not a .feature file"));
            }
            Some(EntryKind::File) => found.push(path),
            Some(EntryKind::Dir) => walk(filesystem, &path, &mut found)?,
        }
    }
    let mut files: Vec<FeatureFile> = found
        .into_iter()
        .map(|path| feature_file(filesystem, cwd, path))
        .collect();
    files.sort_by(|left, right| left.shown.cmp(&right.shown));
    files.dedup_by(|left, right| left.absolute == right.absolute);
    Ok(files)
}

/// The nearest directory at or above the one holding `file` that holds a
/// `Cargo.toml`.
#[must_use]
pub fn manifest_dir(filesystem: &dyn FileSystem, file: &str) -> Option<String> {
    let mut dir = fs::parent(file);
    while let Some(current) = dir {
        if filesystem.kind(&fs::join(current, MANIFEST)) == Some(EntryKind::File) {
            return Some(current.to_owned());
        }
        dir = fs::parent(current);
    }
    None
}

fn feature_file(filesystem: &dyn FileSystem, cwd: &str, absolute: String) -> FeatureFile {
    let manifest_dir = manifest_dir(filesystem, &absolute)
        .or_else(|| fs::parent(&absolute).map(str::to_owned))
        .unwrap_or_else(|| "/".to_owned());
    FeatureFile {
        shown: fs::relative(cwd, &absolute),
        lint_path: fs::relative(&manifest_dir, &absolute),
        manifest_dir,
        absolute,
    }
}

fn walk(filesystem: &dyn FileSystem, dir: &str, found: &mut Vec<String>) -> Result<(), String> {
    let mut names = filesystem.list(dir)?;
    names.sort();
    for name in names {
        let path = fs::join(dir, &name);
        match filesystem.kind(&path) {
            Some(EntryKind::Dir) if !name.starts_with('.') && name != SKIPPED_DIR => {
                walk(filesystem, &path, found)?;
            }
            Some(EntryKind::File) if is_feature(&name) => found.push(path),
            _ => {}
        }
    }
    Ok(())
}

fn is_feature(path: &str) -> bool {
    path.rsplit_once('.')
        .is_some_and(|(stem, extension)| extension == "feature" && !fs::file_name(stem).is_empty())
}

#[cfg(test)]
mod tests {
    use {super::*, crate::fs::memory::MemoryFs, assert2::assert, rstest::rstest};

    fn tree() -> MemoryFs {
        MemoryFs::new([
            ("/repo/Cargo.toml", "[workspace]"),
            ("/repo/crates/log/Cargo.toml", "[package]"),
            ("/repo/crates/log/tests/features/roll/roll.feature", ""),
            ("/repo/crates/log/tests/features/roll/notes.md", ""),
            ("/repo/crates/log/tests/features/clean/clean.feature", ""),
            ("/repo/crates/log/target/copy/x.feature", ""),
            ("/repo/crates/log/.hidden/y.feature", ""),
            ("/repo/docs/loose.feature", ""),
            ("/outside/stray.feature", ""),
        ])
    }

    fn file(shown: &str, absolute: &str, manifest_dir: &str, lint_path: &str) -> FeatureFile {
        FeatureFile {
            absolute: absolute.to_owned(),
            shown: shown.to_owned(),
            manifest_dir: manifest_dir.to_owned(),
            lint_path: lint_path.to_owned(),
        }
    }

    #[rstest]
    #[case::a_crate_from_the_repo_root("/repo", &["crates"], vec![
        file(
            "crates/log/tests/features/clean/clean.feature",
            "/repo/crates/log/tests/features/clean/clean.feature",
            "/repo/crates/log",
            "tests/features/clean/clean.feature",
        ),
        file(
            "crates/log/tests/features/roll/roll.feature",
            "/repo/crates/log/tests/features/roll/roll.feature",
            "/repo/crates/log",
            "tests/features/roll/roll.feature",
        ),
    ])]
    #[case::from_inside_the_crate("/repo/crates/log/tests", &["features/roll/roll.feature"], vec![
        file(
            "features/roll/roll.feature",
            "/repo/crates/log/tests/features/roll/roll.feature",
            "/repo/crates/log",
            "tests/features/roll/roll.feature",
        ),
    ])]
    #[case::falls_back_to_the_workspace_manifest("/repo", &["docs"], vec![
        file("docs/loose.feature", "/repo/docs/loose.feature", "/repo", "docs/loose.feature"),
    ])]
    #[case::outside_the_cwd_and_any_manifest("/repo", &["../outside"], vec![
        file("../outside/stray.feature", "/outside/stray.feature", "/outside", "stray.feature"),
    ])]
    #[case::repeats_collapse("/repo", &["crates/log/tests/features/roll", "./crates/log/tests/features/roll/roll.feature"], vec![
        file(
            "crates/log/tests/features/roll/roll.feature",
            "/repo/crates/log/tests/features/roll/roll.feature",
            "/repo/crates/log",
            "tests/features/roll/roll.feature",
        ),
    ])]
    fn discover_finds_features_and_their_paths(
        #[case] cwd: &str,
        #[case] inputs: &[&str],
        #[case] expected: Vec<FeatureFile>,
    ) {
        let inputs: Vec<String> = inputs.iter().map(|input| (*input).to_owned()).collect();
        assert!(discover(&tree(), cwd, &inputs) == Ok(expected));
    }

    #[rstest]
    #[case::missing("nowhere", "nowhere: no such file or directory")]
    #[case::not_a_feature(
        "crates/log/tests/features/roll/notes.md",
        "crates/log/tests/features/roll/notes.md: not a .feature file"
    )]
    fn discover_rejects_a_bad_input(#[case] input: &str, #[case] expected: &str) {
        let result = discover(&tree(), "/repo", &[input.to_owned()]);
        assert!(result == Err(expected.to_owned()));
    }

    #[rstest]
    #[case::crate_manifest(
        "/repo/crates/log/tests/features/a/a.feature",
        Some("/repo/crates/log")
    )]
    #[case::workspace_manifest("/repo/docs/a.feature", Some("/repo"))]
    #[case::none("/outside/stray.feature", None)]
    fn manifest_dir_is_the_nearest_ancestor(#[case] file: &str, #[case] expected: Option<&str>) {
        assert!(manifest_dir(&tree(), file).as_deref() == expected);
    }
}
