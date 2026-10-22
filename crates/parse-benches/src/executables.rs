//! Which compiled benchmark executables a measurement pass ran.
//!
//! The nightly criterion lane measures a reference and a candidate revision on
//! the same host. Cargo decides freshness per target directory, so when both
//! revisions share one, a candidate that differs only in source can be judged
//! fresh and the reference's binary runs again under the candidate's name. The
//! lane therefore builds each revision into its own target directory before it
//! measures either, and this module keeps the evidence:
//!
//! - [`manifest_from_cargo_messages`] records each bench executable that
//!   `cargo bench --no-run --message-format=json` built, by canonical path and
//!   SHA-256, in an [`ExecutableManifest`] for one commit.
//! - [`verify_pass`] reads the `Running` lines Cargo printed during one pass,
//!   and accepts the pass only when every executable it ran is in the manifest
//!   and still has the recorded digest.
//! - [`compare_summaries`](crate::compare_summaries) refuses a comparison whose
//!   reference and candidate share an executable path, or whose passes on one
//!   side ran different executables.

use {
    crate::ParseBenchesError,
    serde::{Deserialize, Serialize},
    sha2::{Digest, Sha256},
    std::{
        fs,
        path::{Path, PathBuf},
    },
};

/// One compiled benchmark executable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BenchExecutable {
    /// The Cargo bench target name, such as `log`.
    pub target: String,
    /// The canonical path of the executable.
    pub path: PathBuf,
    /// The SHA-256 of its bytes, in lowercase hex.
    pub sha256: String,
}

/// The bench executables that one revision was built into.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutableManifest {
    /// The commit the executables were built from.
    pub commit: String,
    /// The executables, sorted by target name and path.
    pub executables: Vec<BenchExecutable>,
}

/// The fields of one Cargo JSON message that identify a bench executable.
#[derive(Deserialize)]
struct CargoMessage {
    reason: String,
    #[serde(default)]
    target: Option<CargoTarget>,
    #[serde(default)]
    executable: Option<PathBuf>,
}

#[derive(Deserialize)]
struct CargoTarget {
    name: String,
    kind: Vec<String>,
}

/// The SHA-256 of the file at `path`, in lowercase hex.
///
/// # Errors
///
/// Returns [`ParseBenchesError::Io`] when the file cannot be read.
pub fn file_digest(path: &Path) -> Result<String, ParseBenchesError> {
    Ok(hex::encode(Sha256::digest(fs::read(path)?)))
}

/// Record the bench executables in Cargo's `--message-format=json` output.
///
/// Every `compiler-artifact` message for a `bench` target that names an
/// executable contributes one entry, with its path canonicalized and its
/// current bytes digested.
///
/// # Errors
///
/// Returns [`ParseBenchesError::Json`] for a line that is not a Cargo JSON
/// message, [`ParseBenchesError::Io`] for an executable that cannot be read,
/// and [`ParseBenchesError::NoBenchExecutables`] when no bench executable was
/// built.
pub fn manifest_from_cargo_messages(
    messages: &str,
    commit: &str,
) -> Result<ExecutableManifest, ParseBenchesError> {
    let mut executables = Vec::new();
    for line in messages.lines().filter(|line| !line.trim().is_empty()) {
        let message: CargoMessage = serde_json::from_str(line)?;
        let (Some(target), Some(executable)) = (message.target, message.executable) else {
            continue;
        };
        if message.reason != "compiler-artifact" || !target.kind.iter().any(|kind| kind == "bench")
        {
            continue;
        }
        let path = fs::canonicalize(&executable)?;
        executables.push(BenchExecutable {
            target: target.name,
            sha256: file_digest(&path)?,
            path,
        });
    }
    if executables.is_empty() {
        return Err(ParseBenchesError::NoBenchExecutables);
    }
    executables.sort();
    executables.dedup();
    Ok(ExecutableManifest {
        commit: commit.to_string(),
        executables,
    })
}

/// The executables Cargo reports running in `cargo_log`, resolved against
/// `source_dir`.
///
/// Cargo prints `Running benches/log.rs (target/release/deps/log-0123)` to
/// standard error before it starts each bench executable. The path is relative
/// to the directory Cargo ran in when the target directory is below it, and
/// absolute otherwise.
#[must_use]
pub fn ran_executables(cargo_log: &str, source_dir: &Path) -> Vec<PathBuf> {
    cargo_log
        .lines()
        .filter_map(|line| {
            let rest = line.trim().strip_prefix("Running ")?;
            let inner = rest.strip_suffix(')')?;
            let (_, path) = inner.rsplit_once(" (")?;
            Some(source_dir.join(path))
        })
        .collect()
}

/// Check that one measurement pass ran only the manifest's executables,
/// unchanged, and return the executables it ran.
///
/// # Errors
///
/// Returns [`ParseBenchesError::CommitMismatch`] when the manifest was built
/// from another commit, [`ParseBenchesError::NoExecutableRan`] when the log
/// names no executable, [`ParseBenchesError::UnrecordedExecutable`] when the
/// pass ran an executable the manifest does not list, and
/// [`ParseBenchesError::ExecutableChanged`] when a listed executable's bytes
/// no longer match its digest.
pub fn verify_pass(
    manifest: &ExecutableManifest,
    cargo_log: &str,
    source_dir: &Path,
    commit: &str,
) -> Result<Vec<BenchExecutable>, ParseBenchesError> {
    if manifest.commit != commit {
        return Err(ParseBenchesError::CommitMismatch {
            manifest: manifest.commit.clone(),
            pass: commit.to_string(),
        });
    }
    let ran = ran_executables(cargo_log, source_dir);
    if ran.is_empty() {
        return Err(ParseBenchesError::NoExecutableRan);
    }
    let mut verified = Vec::new();
    for path in ran {
        let canonical = fs::canonicalize(&path)
            .map_err(|_| ParseBenchesError::UnrecordedExecutable(path.clone()))?;
        let recorded = manifest
            .executables
            .iter()
            .find(|executable| executable.path == canonical)
            .ok_or_else(|| ParseBenchesError::UnrecordedExecutable(canonical.clone()))?;
        if file_digest(&canonical)? != recorded.sha256 {
            return Err(ParseBenchesError::ExecutableChanged(canonical));
        }
        verified.push(recorded.clone());
    }
    verified.sort();
    verified.dedup();
    Ok(verified)
}
