//! Reads what `krabka format` writes: Kafka's `meta.properties` in every log
//! directory, on every start, and `bootstrap.records.bin` on the first start.
//!
//! [`initialize_log_dirs`] is Kafka's `KafkaRaftServer.initializeLogDirs`. It
//! reads `meta.properties` through [`krabka_format::MetaProperties`], which
//! reads and writes the file as Kafka does.
//!
//! The framing of `bootstrap.records.bin` matches `crates/format`:
//!   [`u32_le` length][serde_wincode-encoded MetadataRecord]
//! The pair repeats until EOF.

use std::path::{Path, PathBuf};

use krabka_format::{ClusterId, DirectoryId, META_PROPERTIES, MetaProperties};
use krabka_metadata::MetadataRecord;
use serde_wincode::SerdeCompat;
use wincode::Deserialize;

use crate::{NodeId, error::BrokerError};

/// The identity of this node that its metadata log directory records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalIdentity {
    /// The cluster that every directory of the node belongs to.
    pub cluster_id: uuid::Uuid,
    /// The metadata log directory's id: the node's KIP-853 voter identity.
    pub directory_id: uuid::Uuid,
}

/// Reads `<log_dir>/meta.properties`, which `krabka format` and
/// `kafka-storage format` write.
///
/// # Errors
///
/// Returns [`BrokerError::BootstrapFile`] when there is no such file, or when
/// it cannot be read or does not parse.
pub fn read_meta_properties(log_dir: &Path) -> Result<MetaProperties, BrokerError> {
    let path = log_dir.join(META_PROPERTIES);
    match MetaProperties::read(log_dir) {
        Ok(Some(meta)) => Ok(meta),
        Ok(None) => Err(BrokerError::BootstrapFile {
            path,
            source: std::io::Error::from(std::io::ErrorKind::NotFound).into(),
        }),
        Err(error) => Err(BrokerError::BootstrapFile {
            path,
            source: Box::new(error),
        }),
    }
}

/// Reads this replica's stable directory id from `meta.properties`.
///
/// KIP-853 identifies each voter by `(node_id, directory_id)`, so the broker
/// must recover its id across restarts instead of minting a fresh one.
///
/// # Errors
///
/// Returns the error of [`read_meta_properties`], or
/// [`BrokerError::BootstrapFile`] when the file has no `directory.id`.
pub fn read_directory_id(log_dir: &Path) -> Result<uuid::Uuid, BrokerError> {
    read_meta_properties(log_dir)?
        .directory_id
        .map(Into::into)
        .ok_or_else(|| BrokerError::BootstrapFile {
            path: log_dir.join(META_PROPERTIES),
            source: format!("No directory id found in {}", log_dir.display()).into(),
        })
}

/// Kafka's `KafkaRaftServer.initializeLogDirs`: reads the `meta.properties`
/// of the metadata log directory and of every data directory, checks that
/// they belong to this node, and gives each directory an id that it keeps.
///
/// The steps and their messages are Kafka's:
///
/// 1. Every directory is read. One that has no `meta.properties` is empty,
///    and one whose file does not read is in error. At least one file has to
///    read (`No readable meta.properties files found.`).
/// 2. The files agree on the cluster id, and each records `node_id`, as
///    `MetaPropertiesEnsemble.verify` checks. A directory of another node is
///    refused with `Stored node id <stored> doesn't match previous node id
///    <node_id> in <dir>/meta.properties. ...`.
/// 3. The metadata log directory is not in error, and no data directory
///    holds a `__cluster_metadata-0` ([`check_metadata_location`]).
/// 4. A file without a `directory.id` gets a new id, and the file is written
///    again.
///
/// Two steps are krabka's. When `configured_cluster_id` is given, it has to
/// be the cluster id of the files (`INCONSISTENT_CLUSTER_ID`). And a data
/// directory with no `meta.properties` is a disk that was added after the
/// format: it gets a `meta.properties` with the node's ids and a new
/// directory id, where Kafka refuses to start. The metadata log directory
/// has to be formatted, because only `krabka format` writes the bootstrap
/// records that it holds.
///
/// # Errors
///
/// Returns [`BrokerError::Startup`] with Kafka's message for each refusal,
/// [`BrokerError::BootstrapFile`] for a configured cluster id that the files
/// do not record, and [`BrokerError::Io`] when a file cannot be written.
pub fn initialize_log_dirs(
    metadata_log_dir: &Path,
    log_dirs: &[PathBuf],
    node_id: NodeId,
    configured_cluster_id: Option<uuid::Uuid>,
) -> Result<LocalIdentity, BrokerError> {
    let node_id = i32::try_from(node_id.0).map_err(|_| {
        BrokerError::Startup(format!(
            "node id {node_id} is outside the range of Kafka's node.id"
        ))
    })?;
    // Kafka's `Loader` keeps the directories in a `TreeSet`.
    let mut dirs: Vec<PathBuf> = log_dirs.to_vec();
    dirs.push(metadata_log_dir.to_path_buf());
    dirs.sort();
    dirs.dedup();

    let mut empty = Vec::new();
    let mut errors = Vec::new();
    let mut formatted: Vec<(PathBuf, MetaProperties)> = Vec::new();
    for dir in dirs {
        match MetaProperties::read(&dir) {
            Ok(Some(meta)) => formatted.push((dir, meta)),
            Ok(None) => empty.push(dir),
            Err(error) => {
                tracing::error!(
                    path = %dir.join(META_PROPERTIES).display(),
                    %error,
                    "Error while reading meta.properties file"
                );
                errors.push(dir);
            }
        }
    }
    if formatted.is_empty() {
        return Err(BrokerError::Startup(
            "No readable meta.properties files found.".to_owned(),
        ));
    }
    // A set with a readable file has the cluster id of that file.
    let Some(cluster_id) = krabka_format::verify_ensemble(
        formatted.iter().map(|(dir, meta)| (dir.as_path(), meta)),
        None,
        Some(node_id),
    )
    .map_err(BrokerError::Startup)?
    else {
        return Err(BrokerError::Startup(
            "No readable meta.properties files found.".to_owned(),
        ));
    };
    if errors.iter().any(|dir| dir == metadata_log_dir) {
        return Err(BrokerError::Startup(format!(
            "Encountered I/O error in metadata log directory {}. Cannot continue.",
            metadata_log_dir.display()
        )));
    }
    if let Some(configured) = configured_cluster_id
        && configured != cluster_id.0
    {
        return Err(BrokerError::BootstrapFile {
            path: metadata_log_dir.join(META_PROPERTIES),
            source: format!(
                "INCONSISTENT_CLUSTER_ID: configured cluster id {} does not match {cluster_id}",
                ClusterId(configured),
            )
            .into(),
        });
    }
    check_metadata_location(log_dirs, metadata_log_dir)?;
    let unformatted = || {
        BrokerError::Startup(format!(
            "No `meta.properties` found in {} (have you run `krabka-format` to format the \
             directory?)",
            metadata_log_dir.display()
        ))
    };
    if empty.iter().any(|dir| dir == metadata_log_dir) {
        return Err(unformatted());
    }

    let mut used: Vec<DirectoryId> = formatted
        .iter()
        .filter_map(|(_, meta)| meta.directory_id)
        .collect();
    let mut local_directory_id = None;
    let added = empty.into_iter().map(|dir| {
        let meta = MetaProperties {
            cluster_id,
            node_id,
            directory_id: None,
        };
        (dir, meta)
    });
    for (dir, mut meta) in formatted.into_iter().chain(added) {
        if meta.directory_id.is_none() {
            let directory_id = DirectoryId::random_unused(&used);
            used.push(directory_id);
            meta.directory_id = Some(directory_id);
            tracing::info!(
                path = %dir.join(META_PROPERTIES).display(),
                %directory_id,
                "Rewriting meta.properties"
            );
            std::fs::create_dir_all(&dir)?;
            meta.write(&dir)?;
        }
        if dir == metadata_log_dir {
            local_directory_id = meta.directory_id;
        }
    }
    Ok(LocalIdentity {
        cluster_id: cluster_id.0,
        directory_id: local_directory_id.ok_or_else(unformatted)?.into(),
    })
}

/// Kafka's check, in `KafkaRaftServer.initializeLogDirs`, that the metadata
/// partition `__cluster_metadata-0` is in the metadata log directory only.
///
/// A data directory that holds one was the metadata log directory of an
/// earlier configuration. Kafka refuses to start rather than run on a second,
/// stale copy of the metadata log, and so does krabka.
///
/// # Errors
/// Returns [`BrokerError::Startup`] with Kafka's message when a directory of
/// `log_dirs` other than `metadata_log_dir` holds `__cluster_metadata-0`.
pub fn check_metadata_location(
    log_dirs: &[std::path::PathBuf],
    metadata_log_dir: &Path,
) -> Result<(), BrokerError> {
    for log_dir in log_dirs
        .iter()
        .filter(|dir| dir.as_path() != metadata_log_dir)
    {
        let cluster_metadata = krabka_raft::metadata_partition_dir(log_dir);
        if cluster_metadata.exists() {
            return Err(BrokerError::Startup(format!(
                "Found unexpected metadata location in data directory `{}` (the configured \
                 metadata directory is {}).",
                cluster_metadata.display(),
                metadata_log_dir.display(),
            )));
        }
    }
    Ok(())
}

/// Extracts the initial voter set from the bootstrap records.
///
/// The last `V1Voters` record wins. This mirrors how the controller applies a
/// stream of `VotersRecord` values, where the most recent one is
/// authoritative. The function returns an empty set when the records hold no
/// `V1Voters` record, which is the joiner path.
#[must_use]
pub fn initial_voters(records: &[MetadataRecord]) -> krabka_metadata::VoterSet {
    records
        .iter()
        .rev()
        .find_map(|r| match r {
            MetadataRecord::V1Voters(v) => Some(v.voters.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// # Errors
/// Returns an error when log I/O fails, when a record or index is corrupt, or
/// when the requested offset violates the segment state.
pub fn load_bootstrap_records(log_dir: &Path) -> Result<Vec<MetadataRecord>, BrokerError> {
    let path = log_dir.join("bootstrap.records.bin");
    if !path.exists() {
        return Ok(vec![]);
    }
    let bytes = std::fs::read(&path).map_err(|e| BrokerError::BootstrapFile {
        path: path.clone(),
        source: Box::new(e),
    })?;
    let mut out = Vec::new();
    let mut cur = &bytes[..];
    while !cur.is_empty() {
        if cur.len() < 4 {
            return Err(BrokerError::BootstrapFile {
                path,
                source: "truncated length prefix".into(),
            });
        }
        let len = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]) as usize;
        cur = &cur[4..];
        if cur.len() < len {
            return Err(BrokerError::BootstrapFile {
                path,
                source: "truncated record body".into(),
            });
        }
        let rec = <SerdeCompat<MetadataRecord>>::deserialize(&cur[..len]).map_err(|e| {
            BrokerError::BootstrapFile {
                path: path.clone(),
                source: Box::new(std::io::Error::other(format!("decode: {e}"))),
            }
        })?;
        out.push(rec);
        cur = &cur[len..];
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::ScramCredentialRecord;
    use krabka_security::SaslMechanism;

    use super::*;
    use crate::test_support::write_bootstrap_records;

    /// The metadata partition may sit in the metadata log directory, whether
    /// or not that is a data directory, and in no data directory besides.
    #[test]
    fn the_metadata_partition_is_only_in_the_metadata_log_directory() {
        // (what, data dirs holding `__cluster_metadata-0`, metadata dir, refused dir)
        let cases: [(&str, &[&str], &str, Option<&str>); 4] = [
            ("none", &[], "meta", None),
            (
                "the metadata dir is a data dir",
                &["data-a"],
                "data-a",
                None,
            ),
            ("a separate metadata dir", &[], "meta", None),
            (
                "a stale copy in a data dir",
                &["data-b"],
                "meta",
                Some("data-b"),
            ),
        ];
        for (what, holding, metadata, refused) in cases {
            let root = tempfile::tempdir().unwrap();
            let log_dirs = vec![root.path().join("data-a"), root.path().join("data-b")];
            for dir in holding {
                std::fs::create_dir_all(krabka_raft::metadata_partition_dir(
                    &root.path().join(dir),
                ))
                .unwrap();
            }
            let metadata_log_dir = root.path().join(metadata);
            let want = refused.map(|dir| {
                format!(
                    "startup failed: Found unexpected metadata location in data directory `{}` \
                     (the configured metadata directory is {}).",
                    krabka_raft::metadata_partition_dir(&root.path().join(dir)).display(),
                    metadata_log_dir.display(),
                )
            });
            check!(
                check_metadata_location(&log_dirs, &metadata_log_dir)
                    .err()
                    .map(|error| error.to_string())
                    == want,
                "{what}"
            );
        }
    }

    #[test]
    fn returns_empty_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let got = load_bootstrap_records(dir.path()).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn decodes_v1_scram_credential() {
        let dir = tempfile::tempdir().unwrap();
        let rec = MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha512,
            salt: vec![1; 16],
            stored_key: vec![2; 64],
            server_key: vec![3; 64],
            iterations: 4096,
        });
        write_bootstrap_records(dir.path(), &[rec]);
        let got = load_bootstrap_records(dir.path()).unwrap();
        assert!(got.len() == 1);
        match &got[0] {
            MetadataRecord::V1ScramCredential(r) => assert!(r.user == "alice"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn initial_voters_roundtrips_seeded_set() {
        use krabka_metadata::{Voter, VoterEndpoint, VoterSet, VotersRecord};
        let dir = tempfile::tempdir().unwrap();
        let seeded = VoterSet::from_voters([Voter {
            id: krabka_audit::NodeId(7),
            directory_id: uuid::Uuid::from_u128(7),
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "h7".into(),
                port: 9093,
            }],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        }]);
        // Frame the records exactly like `krabka format` does.
        write_bootstrap_records(
            dir.path(),
            &[
                MetadataRecord::V1KRaftVersion(krabka_metadata::KRaftVersionRecord {
                    kraft_version: 1,
                }),
                MetadataRecord::V1Voters(VotersRecord {
                    voters: seeded.clone(),
                }),
            ],
        );

        let records = load_bootstrap_records(dir.path()).unwrap();
        assert!(records.len() == 2);
        assert!(initial_voters(&records) == seeded);
    }

    #[test]
    fn initial_voters_empty_when_no_voters_record() {
        let recs = vec![MetadataRecord::V1KRaftVersion(
            krabka_metadata::KRaftVersionRecord { kraft_version: 1 },
        )];
        assert!(initial_voters(&recs).is_empty());
    }

    /// The file `kafka-storage format` writes for node 2 on Java 21 and
    /// later, byte for byte: `Properties.store` puts an empty comment, the
    /// date, and the keys in their natural order.
    const KAFKA_WRITTEN: &str = "#\n#Thu Feb 29 12:34:56 UTC 2024\n\
                                 cluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n\
                                 directory.id=U-hCdTzaSPyFB3swwc7Qdw\n\
                                 node.id=2\n\
                                 version=1\n";

    const CLUSTER: &str = "AQIDBAUGBwgJCgsMDQ4PEA";

    fn kafka_meta(node_id: i32, directory_id: Option<&str>) -> MetaProperties {
        MetaProperties {
            cluster_id: CLUSTER.parse().unwrap(),
            node_id,
            directory_id: directory_id.map(|id| id.parse().unwrap()),
        }
    }

    /// The broker reads the `meta.properties` that Kafka writes, in the form
    /// of each Java version, and the hand-written form of a test fixture.
    #[test]
    fn reads_the_file_kafka_writes() {
        let cases = [
            ("Java 21 and later", KAFKA_WRITTEN),
            (
                "Java 17, in hash order",
                "#\n#Thu Feb 29 12:34:56 UTC 2024\nnode.id=2\nversion=1\n\
                 directory.id=U-hCdTzaSPyFB3swwc7Qdw\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n",
            ),
            (
                "hand-written, no comments",
                "version=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=2\n\
                 directory.id=U-hCdTzaSPyFB3swwc7Qdw\n",
            ),
        ];
        for (what, text) in cases {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join(META_PROPERTIES), text).unwrap();
            check!(
                read_meta_properties(dir.path()).ok()
                    == Some(kafka_meta(2, Some("U-hCdTzaSPyFB3swwc7Qdw"))),
                "{what}"
            );
            check!(
                read_directory_id(dir.path()).ok()
                    == Some(uuid::Uuid::from_u128(
                        0x53e8_4275_3cda_48fc_8507_7b30_c1ce_d077
                    )),
                "{what}"
            );
        }
    }

    /// A directory without the file, or a file without a directory id, has
    /// no directory id to read.
    #[test]
    fn read_directory_id_needs_a_file_with_one() {
        let absent = tempfile::tempdir().unwrap();
        check!(matches!(
            read_directory_id(absent.path()),
            Err(BrokerError::BootstrapFile { .. })
        ));
        let without = tempfile::tempdir().unwrap();
        kafka_meta(2, None).write(without.path()).unwrap();
        let error = read_directory_id(without.path()).unwrap_err().to_string();
        check!(error.ends_with(&format!(
            ": No directory id found in {}",
            without.path().display()
        )));
    }

    /// What a directory of [`initialize_log_dirs`] holds before the call.
    #[derive(Clone, Copy)]
    enum Holds {
        /// Nothing.
        Nothing,
        /// This `meta.properties`.
        File(&'static str),
        /// A file and a `__cluster_metadata-0` directory.
        FileAndMetadataLog(&'static str),
    }

    /// One `initialize_log_dirs` case: what it is, the metadata log directory
    /// and the data directories with what each holds, the configured cluster
    /// id, and the error, or `None` for a start.
    type InitCase = (
        &'static str,
        Holds,
        [Holds; 2],
        Option<uuid::Uuid>,
        Option<&'static str>,
    );

    /// Kafka's `initializeLogDirs` checks, with krabka's two additions: a
    /// configured cluster id, and a data directory added after the format.
    #[test]
    fn initialize_log_dirs_applies_kafkas_checks() {
        const NODE_2: &str = "version=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=2\n\
                              directory.id=AAAAAAAAAAAAAAAAAAABAA\n";
        const NODE_2_DATA: &str = "version=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=2\n\
                                   directory.id=AAAAAAAAAAAAAAAAAAACAA\n";
        const NODE_3: &str = "version=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=3\n\
                              directory.id=AAAAAAAAAAAAAAAAAAADAA\n";
        const NO_DIRECTORY_ID: &str = "version=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=2\n";
        const UNREADABLE: &str = "version=one\n";
        let cases: [InitCase; 9] = [
            (
                "a formatted node",
                Holds::File(NODE_2),
                [Holds::File(NODE_2_DATA), Holds::Nothing],
                Some(uuid::Uuid::from_u128(
                    0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
                )),
                None,
            ),
            (
                "a data directory added after the format",
                Holds::File(NODE_2),
                [Holds::Nothing, Holds::Nothing],
                None,
                None,
            ),
            (
                "a file without a directory id",
                Holds::File(NO_DIRECTORY_ID),
                [Holds::File(NODE_2_DATA), Holds::Nothing],
                None,
                None,
            ),
            (
                "the directory of another node",
                Holds::File(NODE_2),
                [Holds::File(NODE_3), Holds::Nothing],
                None,
                Some(
                    "startup failed: Stored node id 3 doesn't match previous node id 2 in \
                     {data-a}/meta.properties. If you moved your data, make sure your \
                     configured node id matches. If you intend to create a new node, you should \
                     remove all data in your data directories.",
                ),
            ),
            (
                "nothing formatted",
                Holds::Nothing,
                [Holds::Nothing, Holds::Nothing],
                None,
                Some("startup failed: No readable meta.properties files found."),
            ),
            (
                "an unreadable metadata log directory",
                Holds::File(UNREADABLE),
                [Holds::File(NODE_2_DATA), Holds::Nothing],
                None,
                Some(
                    "startup failed: Encountered I/O error in metadata log directory {meta}. \
                     Cannot continue.",
                ),
            ),
            (
                "an unformatted metadata log directory",
                Holds::Nothing,
                [Holds::File(NODE_2_DATA), Holds::Nothing],
                None,
                Some(
                    "startup failed: No `meta.properties` found in {meta} (have you run \
                     `krabka-format` to format the directory?)",
                ),
            ),
            (
                "another configured cluster id",
                Holds::File(NODE_2),
                [Holds::Nothing, Holds::Nothing],
                Some(uuid::Uuid::from_u128(0xc2)),
                Some(
                    "bootstrap file \"{meta}/meta.properties\": INCONSISTENT_CLUSTER_ID: \
                     configured cluster id AAAAAAAAAAAAAAAAAAAAwg does not match \
                     AQIDBAUGBwgJCgsMDQ4PEA",
                ),
            ),
            (
                "a stale metadata log in a data directory",
                Holds::File(NODE_2),
                [Holds::FileAndMetadataLog(NODE_2_DATA), Holds::Nothing],
                None,
                Some(
                    "startup failed: Found unexpected metadata location in data directory \
                     `{data-a}/__cluster_metadata-0` (the configured metadata directory is \
                     {meta}).",
                ),
            ),
        ];
        for (what, metadata, data, configured, refusal) in cases {
            let root = tempfile::tempdir().unwrap();
            let meta_dir = root.path().join("meta");
            let data_dirs = vec![root.path().join("data-a"), root.path().join("data-b")];
            for (dir, holds) in
                std::iter::once((&meta_dir, metadata)).chain(data_dirs.iter().zip(data))
            {
                match holds {
                    Holds::Nothing => {}
                    Holds::File(text) => {
                        std::fs::create_dir_all(dir).unwrap();
                        std::fs::write(dir.join(META_PROPERTIES), text).unwrap();
                    }
                    Holds::FileAndMetadataLog(text) => {
                        std::fs::create_dir_all(krabka_raft::metadata_partition_dir(dir)).unwrap();
                        std::fs::write(dir.join(META_PROPERTIES), text).unwrap();
                    }
                }
            }
            let got = initialize_log_dirs(&meta_dir, &data_dirs, NodeId(2), configured);

            let Some(refusal) = refusal else {
                // Every directory now records this node and an id of its own,
                // and the metadata log directory's id is the one returned.
                let identity = got.unwrap_or_else(|e| panic!("{what}: {e}"));
                let metas: Vec<MetaProperties> = std::iter::once(&meta_dir)
                    .chain(&data_dirs)
                    .map(|dir| read_meta_properties(dir).unwrap())
                    .collect();
                let ids: std::collections::HashSet<DirectoryId> =
                    metas.iter().filter_map(|meta| meta.directory_id).collect();
                check!(ids.len() == metas.len(), "{what}: {metas:?}");
                check!(
                    metas
                        .iter()
                        .map(|meta| MetaProperties {
                            directory_id: None,
                            ..*meta
                        })
                        .collect::<Vec<_>>()
                        == vec![kafka_meta(2, None); 3],
                    "{what}"
                );
                check!(
                    identity
                        == LocalIdentity {
                            cluster_id: CLUSTER.parse::<ClusterId>().unwrap().0,
                            directory_id: metas[0].directory_id.unwrap().into(),
                        },
                    "{what}"
                );
                if let Holds::File(text) = metadata
                    && text == NODE_2
                {
                    check!(
                        std::fs::read_to_string(meta_dir.join(META_PROPERTIES)).unwrap() == text,
                        "{what}: a file with a directory id is left alone"
                    );
                }
                continue;
            };
            let want = refusal
                .replace("{meta}", &meta_dir.display().to_string())
                .replace("{data-a}", &data_dirs[0].display().to_string());
            check!(got.map_err(|e| e.to_string()) == Err(want), "{what}");
        }
    }

    #[test]
    fn refuses_truncated_length_prefix() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("bootstrap.records.bin"), [0u8, 0u8, 0u8]).unwrap();
        let err = load_bootstrap_records(dir.path()).unwrap_err();
        assert!(matches!(err, BrokerError::BootstrapFile { .. }));
    }

    #[test]
    fn refuses_truncated_record_body() {
        let dir = tempfile::tempdir().unwrap();
        // Length prefix says 100 bytes follow; only write 4.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&100u32.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 4]);
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();
        assert!(matches!(
            load_bootstrap_records(dir.path()),
            Err(BrokerError::BootstrapFile { .. })
        ));
    }

    #[test]
    fn refuses_undecodable_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = Vec::new();
        // Length prefix=8, body=random bytes that aren't valid bincode for MetadataRecord.
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&[0xFFu8; 8]);
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();
        assert!(matches!(
            load_bootstrap_records(dir.path()),
            Err(BrokerError::BootstrapFile { .. })
        ));
    }

    #[test]
    fn zero_length_record_has_body_decode_error_not_prefix_truncation() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("bootstrap.records.bin"), 0u32.to_le_bytes()).unwrap();
        let err = load_bootstrap_records(dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("decode:"), "unexpected error: {msg}");
        assert!(!msg.contains("truncated length prefix"));
    }
}
