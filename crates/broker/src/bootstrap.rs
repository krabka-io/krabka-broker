//! Reads what `krabka format` writes: Kafka's `meta.properties` in every log
//! directory, on every start, and `bootstrap.records.bin` on the first start.
//!
//! [`initialize_log_dirs`] is Kafka's `KafkaRaftServer.initializeLogDirs`. It
//! reads `meta.properties` through [`krabka_format::MetaProperties`], which
//! reads and writes the file as Kafka does.
//!
//! The framing of `bootstrap.records.bin` matches `crates/format`: a
//! big-endian `i16` version, [`BOOTSTRAP_RECORDS_VERSION`], once at the start
//! of the file, then
//!   [`u32_le` length][serde_wincode-encoded MetadataRecord]
//! repeated until EOF.

use {
    crate::{NodeId, error::BrokerError},
    krabka_format::{ClusterId, DirectoryId, META_PROPERTIES, MetaProperties},
    krabka_metadata::MetadataRecord,
    serde_wincode::SerdeCompat,
    std::{
        fs,
        io::{self, ErrorKind},
        path::{Path, PathBuf},
    },
    wincode::Deserialize,
};

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
            source: io::Error::from(ErrorKind::NotFound).into(),
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

/// A log directory and the `meta.properties` that it holds.
struct DirMeta {
    dir: PathBuf,
    meta: MetaProperties,
}

/// Kafka's `KafkaRaftServer.initializeLogDirs`.
///
/// It reads the `meta.properties` of the metadata log directory and of every
/// data directory, checks that they belong to this node, and gives each
/// directory an id that it keeps.
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
    let mut formatted: Vec<DirMeta> = Vec::new();
    for dir in dirs {
        match MetaProperties::read(&dir) {
            Ok(Some(meta)) => formatted.push(DirMeta { dir, meta }),
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
        formatted
            .iter()
            .map(|entry| (entry.dir.as_path(), &entry.meta)),
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
        .filter_map(|entry| entry.meta.directory_id)
        .collect();
    let mut local_directory_id = None;
    let added = empty.into_iter().map(|dir| {
        let meta = MetaProperties {
            cluster_id,
            node_id,
            directory_id: None,
        };
        DirMeta { dir, meta }
    });
    for DirMeta { dir, mut meta } in formatted.into_iter().chain(added) {
        if meta.directory_id.is_none() {
            let directory_id = DirectoryId::random_unused(&used);
            used.push(directory_id);
            meta.directory_id = Some(directory_id);
            tracing::info!(
                path = %dir.join(META_PROPERTIES).display(),
                %directory_id,
                "Rewriting meta.properties"
            );
            fs::create_dir_all(&dir)?;
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
    log_dirs: &[PathBuf],
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

/// The `bootstrap.records.bin` format version this build reads, and the one
/// `krabka format` writes: the big-endian `i16` at the front of the file.
///
/// It is part of the 1.x on-disk contract: a 1.x broker reads every version
/// an earlier 1.x `krabka format` wrote. `crates/format` writes the same
/// number, and a golden-bytes test on each side pins the layout.
pub const BOOTSTRAP_RECORDS_VERSION: i16 = 0;

/// The name of the bootstrap record stream in the metadata log directory.
const BOOTSTRAP_RECORDS_FILE: &str = "bootstrap.records.bin";

/// Why the version header of `bootstrap.records.bin` is refused. It is the
/// source of the [`BrokerError::BootstrapFile`] the read returns.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BootstrapRecordsVersionError {
    /// The file is too short to hold a version.
    #[error(
        "{BOOTSTRAP_RECORDS_FILE} has no version header: it predates krabka 1.0, so the node \
         must be reformatted"
    )]
    Missing,
    /// The file names a version this build does not read. A file written
    /// before 1.0 starts with a record length instead, which reads as an
    /// unknown version.
    #[error(
        "unrecognized version {found} of {BOOTSTRAP_RECORDS_FILE}: a file without a version \
         header predates krabka 1.0, so the node must be reformatted"
    )]
    Unsupported {
        /// The version the first two bytes name.
        found: i16,
    },
}

/// Encode `records` as `krabka format` writes `bootstrap.records.bin`: the
/// version header, then each record as a `u32` little-endian length and its
/// wincode bytes.
///
/// # Errors
/// Returns a message when a record does not serialize or is longer than a
/// `u32` length can say.
pub fn encode_bootstrap_records(records: &[MetadataRecord]) -> Result<Vec<u8>, String> {
    use wincode::Serialize as _;

    let mut bytes = BOOTSTRAP_RECORDS_VERSION.to_be_bytes().to_vec();
    for record in records {
        let blob = <SerdeCompat<MetadataRecord>>::serialize(record)
            .map_err(|e| format!("serialize record: {e}"))?;
        let len = u32::try_from(blob.len())
            .map_err(|_| format!("record too large: {} bytes", blob.len()))?;
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(&blob);
    }
    Ok(bytes)
}

/// Reads `<log_dir>/bootstrap.records.bin`. A missing file holds no records.
///
/// # Errors
/// Returns [`BrokerError::BootstrapFile`] when the file cannot be read, when
/// its version header is missing or unknown (the source is a
/// [`BootstrapRecordsVersionError`]), or when a record is truncated or does
/// not decode.
pub fn load_bootstrap_records(log_dir: &Path) -> Result<Vec<MetadataRecord>, BrokerError> {
    let path = log_dir.join(BOOTSTRAP_RECORDS_FILE);
    if !path.exists() {
        return Ok(vec![]);
    }
    let bytes = fs::read(&path).map_err(|e| BrokerError::BootstrapFile {
        path: path.clone(),
        source: Box::new(e),
    })?;
    let Some((&header, mut cur)) = bytes.split_first_chunk::<2>() else {
        return Err(BrokerError::BootstrapFile {
            path,
            source: Box::new(BootstrapRecordsVersionError::Missing),
        });
    };
    let found = i16::from_be_bytes(header);
    if found != BOOTSTRAP_RECORDS_VERSION {
        return Err(BrokerError::BootstrapFile {
            path,
            source: Box::new(BootstrapRecordsVersionError::Unsupported { found }),
        });
    }
    let mut out = Vec::new();
    while !cur.is_empty() {
        let Some((prefix, rest)) = cur.split_first_chunk::<4>() else {
            return Err(BrokerError::BootstrapFile {
                path,
                source: "truncated length prefix".into(),
            });
        };
        let len = u32::from_le_bytes(*prefix) as usize;
        let Some((body, rest)) = rest.split_at_checked(len) else {
            return Err(BrokerError::BootstrapFile {
                path,
                source: "truncated record body".into(),
            });
        };
        let rec = <SerdeCompat<MetadataRecord>>::deserialize(body).map_err(|e| {
            BrokerError::BootstrapFile {
                path: path.clone(),
                source: Box::new(io::Error::other(format!("decode: {e}"))),
            }
        })?;
        out.push(rec);
        cur = rest;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::write_bootstrap_records,
        assert2::{assert, check},
        krabka_metadata::ScramCredentialRecord,
        krabka_security::SaslMechanism,
        std::{
            collections::{BTreeMap, HashSet},
            iter,
        },
        uuid::Uuid,
    };

    /// The metadata partition may sit in the metadata log directory, whether
    /// or not that is a data directory, and in no data directory besides.
    #[test]
    fn the_metadata_partition_is_only_in_the_metadata_log_directory() {
        // (what, data dirs holding `__cluster_metadata-0`, metadata dir, refused dir)
        struct Case {
            what: &'static str,
            /// The data directories that hold `__cluster_metadata-0`.
            holding: &'static [&'static str],
            metadata: &'static str,
            /// The data directory the check refuses, if any.
            refused: Option<&'static str>,
        }
        let cases: [Case; 4] = [
            Case {
                what: "none",
                holding: &[],
                metadata: "meta",
                refused: None,
            },
            Case {
                what: "the metadata dir is a data dir",
                holding: &["data-a"],
                metadata: "data-a",
                refused: None,
            },
            Case {
                what: "a separate metadata dir",
                holding: &[],
                metadata: "meta",
                refused: None,
            },
            Case {
                what: "a stale copy in a data dir",
                holding: &["data-b"],
                metadata: "meta",
                refused: Some("data-b"),
            },
        ];
        for Case {
            what,
            holding,
            metadata,
            refused,
        } in cases
        {
            let root = tempfile::tempdir().unwrap();
            let log_dirs = vec![root.path().join("data-a"), root.path().join("data-b")];
            for dir in holding {
                fs::create_dir_all(krabka_raft::metadata_partition_dir(&root.path().join(dir)))
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
        write_bootstrap_records(dir.path(), &[
            MetadataRecord::V1KRaftVersion(krabka_metadata::KRaftVersionRecord {
                kraft_version: 1,
            }),
            MetadataRecord::V1Voters(VotersRecord {
                voters: seeded.clone(),
            }),
        ]);

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

    const CLUSTER: &str = "AQIDBAUGBwgJCgsMDQ4PEA";

    fn kafka_meta(node_id: i32, directory_id: Option<&str>) -> MetaProperties {
        MetaProperties {
            cluster_id: CLUSTER.parse().unwrap(),
            node_id,
            directory_id: directory_id.map(|id| id.parse().unwrap()),
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

    fn write_records_file(dir: &Path, bytes: &[u8]) {
        fs::write(dir.join(BOOTSTRAP_RECORDS_FILE), bytes).unwrap();
    }

    /// The records the golden file holds.
    fn golden_records() -> Vec<MetadataRecord> {
        vec![MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: "metadata.version".into(),
                level: 30,
            },
        )]
    }

    /// `golden_records` as a 1.0 `krabka format` writes them: the version
    /// header, then one `u32` little-endian length and the wincode record.
    const GOLDEN_BYTES: &[u8] = include_bytes!("../../format/tests/fixtures/bootstrap.records.bin");

    #[test]
    fn the_encoder_produces_the_golden_bytes() {
        assert!(encode_bootstrap_records(&golden_records()).unwrap() == GOLDEN_BYTES);
    }

    #[test]
    fn the_golden_bytes_decode_to_their_records() {
        let dir = tempfile::tempdir().unwrap();
        write_records_file(dir.path(), GOLDEN_BYTES);
        assert!(load_bootstrap_records(dir.path()).unwrap() == golden_records());
    }

    /// A file holding only the header has no records.
    #[test]
    fn a_header_alone_holds_no_records() {
        let dir = tempfile::tempdir().unwrap();
        write_records_file(dir.path(), &BOOTSTRAP_RECORDS_VERSION.to_be_bytes());
        assert!(load_bootstrap_records(dir.path()).unwrap().is_empty());
    }

    /// A missing header and an unknown version are refused, each with a
    /// typed source. A file `krabka format` wrote before 1.0 starts with the
    /// first record's `u32` little-endian length, which reads as an unknown
    /// version.
    fn refused_records(bytes: &[u8]) -> RefusedRecords {
        let dir = tempfile::tempdir().unwrap();
        write_records_file(dir.path(), bytes);
        let error = load_bootstrap_records(dir.path()).unwrap_err();
        RefusedRecords { dir, error }
    }

    /// The directory that holds a refused records file, and the refusal.
    struct RefusedRecords {
        dir: tempfile::TempDir,
        error: BrokerError,
    }

    #[test]
    fn a_missing_or_unknown_version_is_refused() {
        struct Case {
            what: &'static str,
            bytes: Vec<u8>,
            want: BootstrapRecordsVersionError,
        }

        let mut pre_1_0 = Vec::new();
        for record in golden_records() {
            use wincode::Serialize as _;
            let blob = <SerdeCompat<MetadataRecord>>::serialize(&record).unwrap();
            pre_1_0.extend_from_slice(&u32::try_from(blob.len()).unwrap().to_le_bytes());
            pre_1_0.extend_from_slice(&blob);
        }
        let pre_1_0_version = i16::from_be_bytes([pre_1_0[0], pre_1_0[1]]);
        let mut version_one = GOLDEN_BYTES.to_vec();
        version_one[..2].copy_from_slice(&1_i16.to_be_bytes());
        let cases: [Case; 5] = [
            Case {
                what: "empty",
                bytes: Vec::new(),
                want: BootstrapRecordsVersionError::Missing,
            },
            Case {
                what: "one byte",
                bytes: vec![0],
                want: BootstrapRecordsVersionError::Missing,
            },
            Case {
                what: "version 1",
                bytes: version_one,
                want: BootstrapRecordsVersionError::Unsupported { found: 1 },
            },
            Case {
                what: "version -1",
                bytes: vec![0xff, 0xff],
                want: BootstrapRecordsVersionError::Unsupported { found: -1 },
            },
            Case {
                what: "the pre-1.0 layout",
                bytes: pre_1_0,
                want: BootstrapRecordsVersionError::Unsupported {
                    found: pre_1_0_version,
                },
            },
        ];
        for Case { what, bytes, want } in cases {
            let RefusedRecords { dir, error } = refused_records(&bytes);
            let BrokerError::BootstrapFile { path, source } = error else {
                panic!("{what}: not a bootstrap file error: {error}");
            };
            check!(path == dir.path().join(BOOTSTRAP_RECORDS_FILE), "{what}");
            check!(
                source.downcast_ref::<BootstrapRecordsVersionError>() == Some(&want),
                "{what}"
            );
            check!(source.to_string().contains("predates krabka 1.0"), "{what}");
        }
    }

    /// The framing after the header: a short length, a short body, and a
    /// body that does not decode are each refused with their own message.
    #[test]
    fn malformed_framing_after_the_header_is_refused() {
        struct Case {
            what: &'static str,
            bytes: Vec<u8>,
            want: &'static str,
        }

        let header = BOOTSTRAP_RECORDS_VERSION.to_be_bytes();
        let framed = |tail: &[u8]| [&header[..], tail].concat();
        let cases: [Case; 4] = [
            Case {
                what: "a truncated length",
                bytes: framed(&[0, 0, 0]),
                want: "truncated length prefix",
            },
            Case {
                what: "a truncated body",
                bytes: framed(&[&100u32.to_le_bytes()[..], &[0; 4]].concat()),
                want: "truncated record body",
            },
            Case {
                what: "an undecodable body",
                bytes: framed(&[&8u32.to_le_bytes()[..], &[0xff; 8]].concat()),
                want: "decode:",
            },
            Case {
                what: "a zero-length body",
                bytes: framed(&0u32.to_le_bytes()),
                want: "decode:",
            },
        ];
        for Case { what, bytes, want } in cases {
            let RefusedRecords { dir: _dir, error } = refused_records(&bytes);
            let BrokerError::BootstrapFile { source, .. } = error else {
                panic!("{what}: not a bootstrap file error: {error}");
            };
            check!(source.to_string().starts_with(want), "{what}: {source}");
        }
    }

    #[derive(Default)]
    struct BootstrapStorage {
        root: Option<tempfile::TempDir>,
        supplied_files: BTreeMap<String, String>,
        configured_cluster: Option<Uuid>,
        initialized: Option<Result<LocalIdentity, BrokerError>>,
        properties: Option<Result<MetaProperties, BrokerError>>,
        directory_id: Option<Result<Uuid, BrokerError>>,
    }

    impl BootstrapStorage {
        fn directory(&self, role: &str) -> PathBuf {
            match role {
                "meta" | "data-a" | "data-b" => self
                    .root
                    .as_ref()
                    .expect("storage root was given")
                    .path()
                    .join(role),
                _ => panic!("unknown log directory role {role}"),
            }
        }
    }

    #[rstest::fixture]
    fn bootstrap_storage() -> BootstrapStorage {
        // Each storage layout starts without files or initialization observations.
        BootstrapStorage::default()
    }

    #[rstest_bdd_macros::given("a fresh unformatted broker storage root")]
    fn fresh_bootstrap_storage(bootstrap_storage: &mut BootstrapStorage) {
        bootstrap_storage.root = Some(tempfile::tempdir().expect("fresh broker log root"));
    }

    #[rstest_bdd_macros::given("the {directory} log directory contains meta.properties:")]
    fn supplied_bootstrap_properties(
        bootstrap_storage: &mut BootstrapStorage,
        directory: String,
        docstring: String,
    ) {
        let path = bootstrap_storage.directory(&directory);
        fs::create_dir_all(&path).expect("create supplied log directory");
        let text = format!("{}\n", docstring.trim_matches('\n'));
        fs::write(path.join(META_PROPERTIES), &text).expect("write supplied Kafka properties");
        bootstrap_storage.supplied_files.insert(directory, text);
    }

    #[rstest_bdd_macros::given(
        "the {directory} data directory also contains a cluster metadata log"
    )]
    fn misplaced_bootstrap_metadata_log(bootstrap_storage: &BootstrapStorage, directory: String) {
        fs::create_dir_all(krabka_raft::metadata_partition_dir(
            &bootstrap_storage.directory(&directory),
        ))
        .expect("create misplaced metadata partition");
    }

    #[rstest_bdd_macros::given("the broker configured cluster UUID {cluster}")]
    fn configured_bootstrap_cluster(bootstrap_storage: &mut BootstrapStorage, cluster: String) {
        bootstrap_storage.configured_cluster =
            Some(cluster.parse().expect("configured cluster UUID"));
    }

    #[rstest_bdd_macros::when("node 2 initializes its metadata and two data log directories")]
    fn initialize_supplied_log_directories(bootstrap_storage: &mut BootstrapStorage) {
        bootstrap_storage.initialized = Some(initialize_log_dirs(
            &bootstrap_storage.directory("meta"),
            &[
                bootstrap_storage.directory("data-a"),
                bootstrap_storage.directory("data-b"),
            ],
            NodeId(2),
            bootstrap_storage.configured_cluster,
        ));
    }

    #[rstest_bdd_macros::when("the broker reads the metadata properties and directory UUID")]
    fn read_supplied_bootstrap_properties(bootstrap_storage: &mut BootstrapStorage) {
        bootstrap_storage.properties =
            Some(read_meta_properties(&bootstrap_storage.directory("meta")));
        bootstrap_storage.directory_id =
            Some(read_directory_id(&bootstrap_storage.directory("meta")));
    }

    #[rstest_bdd_macros::then(
        "the Kafka properties identify node 2, cluster AQIDBAUGBwgJCgsMDQ4PEA and directory U-hCdTzaSPyFB3swwc7Qdw"
    )]
    fn kafka_bootstrap_properties(bootstrap_storage: &BootstrapStorage) {
        check!(
            bootstrap_storage
                .properties
                .as_ref()
                .expect("properties were read")
                .as_ref()
                .ok()
                == Some(&kafka_meta(2, Some("U-hCdTzaSPyFB3swwc7Qdw")))
        );
        check!(
            bootstrap_storage
                .directory_id
                .as_ref()
                .expect("directory id was read")
                .as_ref()
                .ok()
                == Some(&Uuid::from_u128(0x53e8_4275_3cda_48fc_8507_7b30_c1ce_d077))
        );
    }

    #[rstest_bdd_macros::then(
        "all three directories identify node 2 and the same cluster with distinct directory IDs"
    )]
    fn initialized_bootstrap_identity(bootstrap_storage: &BootstrapStorage) {
        let identity = bootstrap_storage
            .initialized
            .as_ref()
            .expect("directories were initialized")
            .as_ref()
            .expect("valid storage starts");
        let meta_dir = bootstrap_storage.directory("meta");
        let data_dirs = [
            bootstrap_storage.directory("data-a"),
            bootstrap_storage.directory("data-b"),
        ];
        let metas: Vec<MetaProperties> = iter::once(&meta_dir)
            .chain(&data_dirs)
            .map(|dir| read_meta_properties(dir).expect("initialized directory properties"))
            .collect();
        let ids: HashSet<DirectoryId> = metas.iter().filter_map(|meta| meta.directory_id).collect();
        check!(ids.len() == metas.len());
        check!(
            metas
                .iter()
                .map(|meta| MetaProperties {
                    directory_id: None,
                    ..*meta
                })
                .collect::<Vec<_>>()
                == vec![kafka_meta(2, None); 3]
        );
        check!(
            *identity
                == LocalIdentity {
                    cluster_id: CLUSTER.parse::<ClusterId>().expect("Kafka cluster id").0,
                    directory_id: metas[0].directory_id.expect("metadata directory id").into()
                }
        );
    }

    #[rstest_bdd_macros::then("the existing metadata properties file is preserved byte for byte")]
    fn preserved_bootstrap_properties(bootstrap_storage: &BootstrapStorage) {
        let expected = bootstrap_storage
            .supplied_files
            .get("meta")
            .expect("existing metadata properties were given");
        check!(
            fs::read_to_string(bootstrap_storage.directory("meta").join(META_PROPERTIES))
                .expect("read initialized metadata properties")
                == *expected
        );
    }

    #[rstest_bdd_macros::then("log directory initialization fails with {message}")]
    fn refused_bootstrap_directories(bootstrap_storage: &BootstrapStorage, message: String) {
        let expected = message
            .replace(
                "{meta}",
                &bootstrap_storage.directory("meta").display().to_string(),
            )
            .replace(
                "{data-a}",
                &bootstrap_storage.directory("data-a").display().to_string(),
            );
        check!(
            bootstrap_storage
                .initialized
                .as_ref()
                .expect("directories were initialized")
                .as_ref()
                .map_err(ToString::to_string)
                == Err(expected)
        );
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/bootstrap",
        fixtures = [bootstrap_storage: BootstrapStorage]
    );
}
