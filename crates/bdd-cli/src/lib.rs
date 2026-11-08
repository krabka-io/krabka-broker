//! The `krabka-bdd` command-line tool over Krabka's Gherkin feature files.
//!
//! Each command is a pure function over a [`FileSystem`] seam and the
//! directory paths are taken from, so its unit tests run on an in-memory
//! tree. The binary parses the arguments, stands the disk in for the seam and
//! prints the [`Report`].

pub mod discover;
pub mod docs;
pub mod feature;
pub mod fs;
pub mod lint;
pub mod map;
pub mod terms;

use {self::fs::FileSystem, krabka_bdd_model::glossary::Glossary};

/// What a command prints, and whether it fails.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    /// The text for standard output.
    pub text: String,
    /// Whether the command exits with status 1.
    pub failed: bool,
}

/// The directory relative paths are taken from and shown against.
///
/// That is `workspace`, the `BUILD_WORKSPACE_DIRECTORY` that `bazel run`
/// sets, when it is set and not empty, or else `cwd`.
#[must_use]
pub fn base_dir(workspace: Option<&str>, cwd: &str) -> String {
    workspace
        .filter(|dir| !dir.is_empty())
        .map_or_else(|| fs::absolute("/", cwd), |dir| fs::absolute(cwd, dir))
}

/// Runs `krabka-bdd map` from `cwd`.
///
/// # Errors
///
/// Returns a message for a path that does not exist or a file that cannot
/// be read.
pub fn run_map(filesystem: &dyn FileSystem, cwd: &str, paths: &[String]) -> Result<Report, String> {
    let files = discover::discover(filesystem, cwd, paths)?;
    Ok(map::render(&feature::load(filesystem, files)?))
}

/// Runs `krabka-bdd docs` from `cwd`, linking features from `out_dir`.
///
/// # Errors
///
/// Returns a message for a path that does not exist or a file that cannot
/// be read.
pub fn run_docs(
    filesystem: &dyn FileSystem,
    cwd: &str,
    out_dir: &str,
    paths: &[String],
) -> Result<Report, String> {
    let files = discover::discover(filesystem, cwd, paths)?;
    let loaded = feature::load(filesystem, files)?;
    Ok(docs::render(&loaded, cwd, &fs::absolute(cwd, out_dir)))
}

/// Runs `krabka-bdd glossary` from `cwd`.
///
/// # Errors
///
/// Returns a message for a path that does not exist or a file that cannot
/// be read.
pub fn run_glossary(
    filesystem: &dyn FileSystem,
    cwd: &str,
    glossary: &Glossary,
    min_steps: usize,
    paths: &[String],
) -> Result<Report, String> {
    let files = discover::discover(filesystem, cwd, paths)?;
    let loaded = feature::load(filesystem, files)?;
    Ok(terms::render(&loaded, glossary, min_steps))
}

/// Runs `krabka-bdd tags-table`.
///
/// # Errors
///
/// Returns the parse error of the embedded glossary.
pub fn run_tags_table() -> Result<Report, String> {
    Ok(Report {
        text: Glossary::embedded()?.tag_table_markdown(),
        failed: false,
    })
}

#[cfg(test)]
mod tests {
    use {super::*, crate::fs::memory::MemoryFs, assert2::assert, rstest::rstest};

    #[rstest]
    #[case::unset(None, "/repo/crates", "/repo/crates")]
    #[case::empty(Some(""), "/repo/crates", "/repo/crates")]
    #[case::bazel_run(Some("/repo"), "/cache/runfiles/_main", "/repo")]
    #[case::normalized(Some("/repo/./x/.."), "/elsewhere", "/repo")]
    fn base_dir_prefers_the_bazel_workspace(
        #[case] workspace: Option<&str>,
        #[case] cwd: &str,
        #[case] expected: &str,
    ) {
        assert!(base_dir(workspace, cwd) == expected);
    }

    const ROLL: &str = "\
Feature: Segments roll on size

  Scenario: A full segment rolls
    Given a segment has reached its size limit
    When a batch is appended
    Then a new segment is opened
";

    fn repo() -> MemoryFs {
        MemoryFs::new([
            ("/repo/Cargo.toml", "[workspace]"),
            ("/repo/crates/log/Cargo.toml", "[package]"),
            ("/repo/crates/log/tests/features/roll/roll.feature", ROLL),
        ])
    }

    #[test]
    fn map_docs_and_glossary_run_over_a_tree() {
        let paths = vec!["crates".to_owned()];
        let map = run_map(&repo(), "/repo", &paths).unwrap();
        assert!(
            map.text
                .starts_with("# Example maps\n\n## 🟨 Segments roll on size\n")
        );
        assert!(!map.failed);
        let docs = run_docs(&repo(), "/repo", "docs", &paths).unwrap();
        assert!(docs.text.contains(
            "### [Segments roll on size](../crates/log/tests/features/roll/roll.feature)"
        ));
        let glossary = Glossary::embedded().unwrap();
        let report = run_glossary(&repo(), "/repo", glossary, 1, &paths).unwrap();
        assert!(report.text.contains("| batch | 1 |\n"));
        assert!(!report.text.contains("| segment |"));
        assert!(!report.failed);
    }

    #[test]
    fn a_missing_path_is_an_error() {
        let result = run_map(&repo(), "/repo", &["gone".to_owned()]);
        assert!(result == Err("gone: no such file or directory".to_owned()));
    }

    #[test]
    fn tags_table_is_the_glossary_table() {
        let report = run_tags_table().unwrap();
        assert!(report.text.starts_with("| Tag | Meaning |\n"));
        assert!(report.text.contains("| `@formulated` |"));
    }
}
