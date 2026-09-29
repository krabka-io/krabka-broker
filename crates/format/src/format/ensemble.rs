//! What the log directories of one run already hold, and whether that set is
//! consistent.
//!
//! This is `MetaPropertiesEnsemble` in Kafka's `Formatter`: every directory is
//! read before any is written, and the formatted ones must agree on the
//! cluster id and must not share a directory id. A directory falls into one of
//! three sets. It is formatted when its `meta.properties.json` reads, empty
//! when it has no `meta.properties.json`, and in error when the file is there
//! but does not read.
//!
//! An empty directory is where krabka is stricter than Kafka. Kafka formats a
//! directory with no `meta.properties` whatever else it holds. `format`
//! refuses one that holds a file it did not write, so a mistyped path cannot
//! seed a directory full of someone else's data. The files an interrupted run
//! left behind do not count against it: a run writes `meta.properties.json`
//! last, so a directory with only those files is one that a run did not
//! finish, and the next run removes them and starts again.

use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use super::{
    META_PROPERTIES,
    output::{META_PROPERTIES_TMP, META_PROPERTIES_VERSION, ZERO_CHECKPOINT_NAME},
};
use crate::ids::{ClusterId, DirectoryId};

/// The directory that holds the metadata log, under the metadata log
/// directory.
pub(super) const CLUSTER_METADATA: &str = "__cluster_metadata";

/// The top-level files that a run writes before `meta.properties.json`.
const PARTIAL_FILES: [&str; 3] = [
    "bootstrap.json",
    "bootstrap.records.bin",
    META_PROPERTIES_TMP,
];

/// The identity a formatted directory records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(super) struct MetaProperties {
    pub(super) cluster_id: ClusterId,
    pub(super) directory_id: DirectoryId,
}

/// The state of every log directory of a run.
#[derive(Debug, Default)]
pub(super) struct Ensemble {
    /// Directories with no `meta.properties.json`, in the order given.
    pub(super) empty: Vec<PathBuf>,
    /// Directories whose `meta.properties.json` reads, sorted by path.
    pub(super) formatted: Vec<(PathBuf, MetaProperties)>,
    /// Directories whose `meta.properties.json` is there but does not read,
    /// sorted by path.
    pub(super) errors: Vec<PathBuf>,
}

/// Why a directory cannot be surveyed.
#[derive(Debug)]
pub(super) enum SurveyError {
    /// The directory holds files that `format` did not write.
    Foreign(PathBuf),
    /// The directory cannot be listed.
    Io(PathBuf, io::Error),
}

impl Ensemble {
    /// Reads every directory in `dirs`.
    pub(super) fn load(dirs: &[PathBuf]) -> Result<Self, SurveyError> {
        let mut ensemble = Self::default();
        for dir in dirs {
            let path = dir.join(META_PROPERTIES);
            match std::fs::read(&path) {
                Ok(bytes) => match parse_meta_properties(&bytes) {
                    Ok(meta) => ensemble.formatted.push((dir.clone(), meta)),
                    Err(error) => {
                        tracing::error!(path = %path.display(), %error, "cannot read meta.properties.json");
                        ensemble.errors.push(dir.clone());
                    }
                },
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    match holds_only_partial_output(dir) {
                        Ok(true) => ensemble.empty.push(dir.clone()),
                        Ok(false) => return Err(SurveyError::Foreign(dir.clone())),
                        Err(error) => return Err(SurveyError::Io(dir.clone(), error)),
                    }
                }
                Err(error) => {
                    tracing::error!(path = %path.display(), %error, "cannot read meta.properties.json");
                    ensemble.errors.push(dir.clone());
                }
            }
        }
        ensemble.formatted.sort_by(|a, b| a.0.cmp(&b.0));
        ensemble.errors.sort();
        Ok(ensemble)
    }

    /// Checks that the formatted directories agree, as Kafka's
    /// `MetaPropertiesEnsemble.verify` does, and returns the cluster id of the
    /// set: `expected` when given, else the first formatted directory's.
    ///
    /// Each error message is Kafka's, word for word.
    pub(super) fn verify(&self, expected: Option<ClusterId>) -> Result<Option<ClusterId>, String> {
        let mut cluster_id = expected;
        let mut seen: HashMap<DirectoryId, &Path> = HashMap::new();
        for (dir, meta) in &self.formatted {
            match cluster_id {
                None => cluster_id = Some(meta.cluster_id),
                Some(expected) if expected != meta.cluster_id => {
                    return Err(format!(
                        "Invalid cluster.id in: {}. Expected {expected}, but read {}",
                        dir.join(META_PROPERTIES).display(),
                        meta.cluster_id,
                    ));
                }
                Some(_) => {}
            }
            if meta.directory_id.is_reserved() {
                return Err(format!(
                    "Invalid reserved directory ID {} found in {}",
                    meta.directory_id,
                    dir.display(),
                ));
            }
            if let Some(previous) = seen.insert(meta.directory_id, dir) {
                // Kafka prints the `Optional` wrapper of the id here.
                return Err(format!(
                    "Duplicate directory ID Optional[{}] found. It was the ID of {}, but also of {}",
                    meta.directory_id,
                    previous.display(),
                    dir.display(),
                ));
            }
        }
        Ok(cluster_id)
    }
}

/// The format stamp alone, read before the ids so that a file of another
/// version is reported as such rather than as an id that does not parse.
#[derive(Deserialize)]
struct Stamp {
    version: u64,
}

/// Decodes a `meta.properties.json`, refusing a format stamp other than this
/// build's.
fn parse_meta_properties(bytes: &[u8]) -> Result<MetaProperties, String> {
    let stamp: Stamp = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    if stamp.version != META_PROPERTIES_VERSION {
        return Err(format!(
            "unsupported meta.properties version {}; this build writes version \
             {META_PROPERTIES_VERSION}",
            stamp.version,
        ));
    }
    serde_json::from_slice(bytes).map_err(|e| e.to_string())
}

/// Whether `dir` is absent, empty, or holds only what an interrupted run
/// writes before `meta.properties.json`.
fn holds_only_partial_output(dir: &Path) -> io::Result<bool> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let file_type = entry.file_type()?;
        let own = if file_type.is_file() {
            PARTIAL_FILES.iter().any(|own| name == *own)
        } else if file_type.is_dir() && name == CLUSTER_METADATA {
            holds_only_the_zero_checkpoint(&entry.path())?
        } else {
            false
        };
        if !own {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether `cluster_metadata` holds nothing but the checkpoint directory and,
/// in it, nothing but the offset-zero checkpoint.
fn holds_only_the_zero_checkpoint(cluster_metadata: &Path) -> io::Result<bool> {
    let checkpoint_dir = krabka_raft::kraft::checkpoint_dir(cluster_metadata);
    for entry in std::fs::read_dir(cluster_metadata)? {
        let entry = entry?;
        if entry.path() != checkpoint_dir || !entry.file_type()?.is_dir() {
            return Ok(false);
        }
    }
    if !checkpoint_dir.exists() {
        return Ok(true);
    }
    for entry in std::fs::read_dir(&checkpoint_dir)? {
        let entry = entry?;
        if entry.file_name() != ZERO_CHECKPOINT_NAME || !entry.file_type()?.is_file() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Removes what an interrupted run left in `dir`.
///
/// Call it only on a directory that [`Ensemble::load`] put in the empty set,
/// which holds nothing else.
pub(super) fn remove_partial_output(dir: &Path) -> io::Result<()> {
    for name in PARTIAL_FILES {
        match std::fs::remove_file(dir.join(name)) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
    }
    match std::fs::remove_dir_all(dir.join(CLUSTER_METADATA)) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use uuid::Uuid;

    use super::*;

    fn meta(cluster: u128, directory: u128) -> MetaProperties {
        MetaProperties {
            cluster_id: ClusterId(Uuid::from_u128(cluster)),
            directory_id: DirectoryId(Uuid::from_u128(directory)),
        }
    }

    fn formatted(entries: &[(&str, MetaProperties)]) -> Ensemble {
        Ensemble {
            formatted: entries
                .iter()
                .map(|(dir, meta)| (PathBuf::from(dir), *meta))
                .collect(),
            ..Ensemble::default()
        }
    }

    /// A `verify` case: what it is, the formatted set, `--cluster-id`, and the
    /// expected result.
    type Case = (&'static str, Ensemble, Option<ClusterId>, Verdict);
    type Verdict = Result<Option<ClusterId>, String>;

    /// The formatted set agrees on one cluster id and gives each directory
    /// its own id, with Kafka's message for each way it can fail.
    #[test]
    fn verify_matches_kafka_meta_properties_ensemble() {
        let c1 = ClusterId(Uuid::from_u128(0xc1));
        let c2 = ClusterId(Uuid::from_u128(0xc2));
        // (what, formatted set, --cluster-id, expected result)
        let cases: Vec<Case> = vec![
            ("nothing formatted", formatted(&[]), None, Ok(None)),
            (
                "nothing formatted, id given",
                formatted(&[]),
                Some(c1),
                Ok(Some(c1)),
            ),
            (
                "the set supplies the id",
                formatted(&[("/a", meta(0xc1, 0x1000)), ("/b", meta(0xc1, 0x2000))]),
                None,
                Ok(Some(c1)),
            ),
            (
                "the given id matches",
                formatted(&[("/a", meta(0xc1, 0x1000))]),
                Some(c1),
                Ok(Some(c1)),
            ),
            (
                "the given id disagrees",
                formatted(&[("/a", meta(0xc1, 0x1000))]),
                Some(c2),
                Err(format!(
                    "Invalid cluster.id in: /a/meta.properties.json. Expected {c2}, but read {c1}"
                )),
            ),
            (
                "two directories disagree",
                formatted(&[("/a", meta(0xc1, 0x1000)), ("/b", meta(0xc2, 0x2000))]),
                None,
                Err(format!(
                    "Invalid cluster.id in: /b/meta.properties.json. Expected {c1}, but read {c2}"
                )),
            ),
            (
                "a reserved directory id",
                formatted(&[("/a", meta(0xc1, 2))]),
                None,
                Err("Invalid reserved directory ID AAAAAAAAAAAAAAAAAAAAAg found in /a".into()),
            ),
            (
                "a shared directory id",
                formatted(&[("/a", meta(0xc1, 0x1000)), ("/b", meta(0xc1, 0x1000))]),
                None,
                Err(format!(
                    "Duplicate directory ID Optional[{}] found. It was the ID of /a, but also of /b",
                    DirectoryId(Uuid::from_u128(0x1000))
                )),
            ),
        ];
        for (what, ensemble, expected, want) in cases {
            check!(ensemble.verify(expected) == want, "{what}");
        }
    }

    /// Only what a run writes before `meta.properties.json` makes a directory
    /// with no marker count as empty.
    #[test]
    fn a_directory_is_empty_only_when_it_holds_partial_output() {
        fn checkpoint(root: &Path) -> PathBuf {
            krabka_raft::kraft::checkpoint_dir(&root.join(CLUSTER_METADATA))
                .join(ZERO_CHECKPOINT_NAME)
        }
        // (what, files to create relative to the directory, empty?)
        type Layout = fn(&Path) -> Vec<PathBuf>;
        let cases: [(&str, Layout, bool); 7] = [
            ("nothing", |_| vec![], true),
            (
                "the bootstrap files",
                |d| vec![d.join("bootstrap.json"), d.join("bootstrap.records.bin")],
                true,
            ),
            (
                "a half-written marker",
                |d| vec![d.join(META_PROPERTIES_TMP)],
                true,
            ),
            ("the offset-zero checkpoint", |d| vec![checkpoint(d)], true),
            (
                "someone else's file",
                |d| vec![d.join("orders-0").join("0.log")],
                false,
            ),
            (
                "a metadata log",
                |d| {
                    vec![
                        krabka_raft::kraft::checkpoint_dir(&d.join(CLUSTER_METADATA))
                            .join("00000000000000000000.log"),
                    ]
                },
                false,
            ),
            (
                "a stray top-level file",
                |d| vec![d.join("notes.txt")],
                false,
            ),
        ];
        for (what, layout, empty) in cases {
            let tmp = tempfile::tempdir().expect("tempdir");
            let dir = tmp.path().join("data");
            std::fs::create_dir_all(&dir).expect("mkdir");
            for file in layout(&dir) {
                std::fs::create_dir_all(file.parent().expect("parent")).expect("mkdir");
                std::fs::write(&file, b"x").expect("write");
            }
            check!(
                holds_only_partial_output(&dir).expect("survey") == empty,
                "{what}"
            );
            if empty {
                remove_partial_output(&dir).expect("remove");
                check!(
                    std::fs::read_dir(&dir).expect("list").next().is_none(),
                    "{what}: the partial output is gone"
                );
            }
        }
        check!(holds_only_partial_output(Path::new("/nonexistent/krabka-format")).expect("survey"));
    }
}
