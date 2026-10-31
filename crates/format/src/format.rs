//! `krabka format` subcommand.
//!
//! Formats every log directory of a node, as `kafka-storage format` does:
//! each directory gets the same cluster id and its own directory id. The set
//! is `--metadata-log-dir`, the counterpart of Kafka's `metadata.log.dir`,
//! and every `--log-dir`, the entries of Kafka's `log.dirs`. Without
//! `--metadata-log-dir`, the first `--log-dir` is the metadata log directory,
//! as `metadata.log.dir` defaults to the first entry of `log.dirs`.
//!
//! ## Output
//!
//! Non-Raft metadata is written as a bootstrap stream. The active controller
//! writes it to the metadata log when it activates on an empty log, and a
//! broker never writes it. Dynamic KIP-853 modes also write the offset-zero
//! bootstrap checkpoint, and a controller takes the records from that
//! checkpoint first, as Kafka's does. Only the metadata log directory
//! receives them. Kafka trunk also writes its bootstrap snapshot only into a
//! metadata directory, and the controller reads the bootstrap records only
//! from there:
//!
//! - `bootstrap.json` — a human-readable manifest with the cluster id and a
//!   base64'd `serde_wincode` blob per metadata record.
//! - `bootstrap.records.bin` — the same records concatenated as
//!   length-prefixed `serde_wincode<SerdeCompat<MetadataRecord>>` payloads, so
//!   the controller can stream them without touching JSON.
//! - `__cluster_metadata-0/00000000000000000000-0000000000.checkpoint` — the
//!   KIP-630/KIP-853 bootstrap snapshot for dynamic membership, for a dynamic
//!   format only. It holds the control state, then the same records in the
//!   same order, as Kafka's `Formatter.writeBoostrapSnapshot` writes them. The
//!   path is Kafka's.
//!
//! Every directory receives Kafka's `meta.properties`, with the cluster id,
//! the node id, and the directory's own id, written last. A data directory
//! receives nothing else.
//!
//! ## Exit codes
//!
//! `kafka-storage format` exits 1 for every failure. `krabka format` keeps a
//! code per cause, so an orchestrator can tell an operator error from an I/O
//! fault:
//!
//! | Code | Cause |
//! | :--- | :--- |
//! | 0 | Every directory is formatted, or was already and `--ignore-formatted` is set. |
//! | 2 | An `--add-scram` iteration count is below 4096, or clap refused the command line. |
//! | 3 | A directory is already formatted without `--ignore-formatted`, holds files `format` did not write, or disagrees with the others on the cluster id. |
//! | 4 | A write failed, or the quorum flags name an invalid voter set. |
//! | 5 | A `--feature`, `--release-version`, or quorum-mode combination is invalid. |

use {
    krabka_metadata::{
        AclEntry, KRaftVersionRecord, MetadataRecord, ScramCredentialRecord, VoterSet,
        VotersRecord, metadata_version,
    },
    krabka_security::scram::{MIN_SCRAM_ITERATIONS, hash_scram_password_with_salt},
    ring::rand::{SecureRandom, SystemRandom},
    std::{
        fmt::Display,
        fs,
        io::{self, Write},
        path::{Path, PathBuf},
    },
};

mod acl;
mod args;
mod ensemble;
mod features;
mod output;
mod quorum;
mod scram;
#[cfg(test)]
mod tests;

pub use self::{
    args::{FormatArgs, ScramSpec, parse_node_id},
    features::LATEST_PRODUCTION_METADATA_VERSION,
    output::FAIL_AFTER_ENV,
};
use {
    self::{
        ensemble::{Ensemble, FormattedDirectory, SurveyError, remove_partial_output},
        features::{ResolvedFeatures, resolve_format_features},
        output::{Fault, write_bootstrap_files, write_dynamic_checkpoint, write_meta_properties},
        quorum::{build_initial_voters, is_dynamic_format},
    },
    crate::{
        ids::{ClusterId, DirectoryId},
        meta_properties::MetaProperties,
    },
};

/// Exit codes. The table in the module documentation gives the cause of each.
const EXIT_OK: i32 = 0;
const EXIT_LOW_ITERATIONS: i32 = 2;
const EXIT_DIRTY_LOG_DIR: i32 = 3;
const EXIT_BOOTSTRAP_FAIL: i32 = 4;
const EXIT_INVALID_FEATURE: i32 = 5;

/// Formats the metadata log directory and `args.log_dirs`, returning the
/// process exit code.
///
/// Every failure a caller can cause -- an unwritable directory, a malformed
/// `--add-scram` spec, an unknown feature -- is reported on stderr and returned
/// as a non-zero code rather than raised.
///
/// # Panics
///
/// Panics if `--initial-controllers` was given without the node's own identity
/// appearing in it. `is_dynamic_format` rejects that combination before this
/// point, so reaching the panic means that validation and this branch have
/// drifted apart.
pub async fn run(args: FormatArgs) -> i32 { run_with_records(args, Vec::new()).await }

// `async` matches the entry point in `main.rs`; the body is sync today
// (purely fs + crypto) but a real raft-log bootstrap would await tokio I/O.
// The body yields an `i32` (not a future), so `#[instrument]` is safe here
// w.r.t. `clippy::async_yields_async`.
/// Formats the metadata log directory and `args.log_dirs` with `extra` seeded
/// alongside the records the flags produce, returning the process exit code.
///
/// A cluster restored from tiered-storage archives has to come up with its
/// topics already present, so the restore tool hands the topic and partition
/// records it recovered to the formatter instead of repeating the bootstrap
/// write itself. The extra records join the one seed stream, so they reach both
/// the offset-zero checkpoint and the bootstrap files.
///
/// `extra` lands directly after the feature records and ahead of the
/// `--add-scram` and `--add-acl` records. The finalized feature levels,
/// `metadata.version` first, decide how every later record is read, so they
/// lead the stream and a topic record can only come after them. Seeded topics
/// ahead of seeded ACL entries then match the order a live cluster writes them
/// in, where a topic exists before an entry names it as a resource. The
/// KIP-853 control state -- the `KRaft` version and the initial voters -- is a
/// separate stream that the checkpoint applies before any of these.
///
/// Ordering inside `extra` is the caller's. A `MetadataImage` derives a topic's
/// partition count from the partition records that apply after it, so each
/// `TopicRecord` must come before its own partitions.
///
/// # Panics
///
/// Panics if `--initial-controllers` was given without the node's own identity
/// appearing in it. `is_dynamic_format` rejects that combination before this
/// point, so reaching the panic means that validation and this branch have
/// drifted apart.
pub async fn run_with_records(args: FormatArgs, extra: Vec<MetadataRecord>) -> i32 {
    run_with_records_to(args, extra, &mut io::stdout()).await
}

/// [`run_with_records`], with Kafka's progress lines written to `progress`
/// instead of stdout.
///
/// A tool that formats as one step of its own command, such as
/// `krabka-restore`, keeps its stdout for its own output: a `--report json`
/// that a script pipes to `jq` must hold the report and nothing else.
///
/// # Panics
///
/// As [`run_with_records`].
#[tracing::instrument(
    level = "info",
    name = "cli.format",
    skip_all,
    fields(
        log_dirs = ?args.log_dirs,
        metadata_log_dir = ?args.metadata_log_dir,
        standalone = args.standalone,
        extra_records = extra.len(),
    )
)]
pub async fn run_with_records_to(
    args: FormatArgs,
    extra: Vec<MetadataRecord>,
    progress: &mut (dyn Write + Send),
) -> i32 {
    match plan(args, extra).and_then(|plan| plan.execute(&Fault::from_env(), progress)) {
        Ok(()) => EXIT_OK,
        Err(Failure { code, message }) => {
            eprintln!("{message}");
            code
        }
    }
}

/// A failed run: its exit code and the line it prints on stderr.
#[derive(Debug, PartialEq, Eq)]
struct Failure {
    code: i32,
    message: String,
}

/// Prefixes `message` with the command name, as every krabka-specific
/// message is. Kafka's own messages are printed bare.
fn krabka(code: i32, message: impl Display) -> Failure {
    Failure {
        code,
        message: format!("krabka format: {message}"),
    }
}

/// What a run writes, and where. [`plan`] builds it without touching the
/// disk beyond reading it, so every validation failure leaves the directories
/// as they were.
struct Plan {
    cluster_id: ClusterId,
    /// `--node-id`, as the `int` that `meta.properties` records.
    node_id: i32,
    metadata_version: String,
    /// The directories to write, the metadata log directory first when it is
    /// one of them.
    targets: Vec<Target>,
    /// The formatted directories `--ignore-formatted` skips.
    skipped: Vec<PathBuf>,
    /// The directories whose `meta.properties` does not read.
    errors: Vec<PathBuf>,
    raft_control_records: Vec<MetadataRecord>,
    records: Vec<MetadataRecord>,
}

/// One directory to format.
#[derive(Debug, PartialEq, Eq)]
struct Target {
    dir: PathBuf,
    kind: DirectoryKind,
    directory_id: DirectoryId,
}

/// Kafka's `Formatter.DirectoryType`: what a directory holds, which decides
/// whether it gets the bootstrap files and the checkpoint, and how the run
/// describes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectoryKind {
    Data,
    StaticMetadata,
    DynamicMetadata,
    DynamicMetadataVoter,
}

impl DirectoryKind {
    /// Kafka's `DirectoryType.description`.
    fn description(self) -> &'static str {
        match self {
            Self::Data => "data directory",
            Self::StaticMetadata => "metadata directory",
            Self::DynamicMetadata => "dynamic metadata directory",
            Self::DynamicMetadataVoter => "dynamic metadata voter directory",
        }
    }

    /// Kafka's `DirectoryType.isMetadataDirectory`: the directory gets the
    /// bootstrap files.
    fn is_metadata(self) -> bool { self != Self::Data }

    /// The directory also gets the offset-zero checkpoint.
    fn is_dynamic_metadata(self) -> bool {
        matches!(self, Self::DynamicMetadata | Self::DynamicMetadataVoter)
    }
}

/// Validates the command line, reads the directories, and decides what to
/// write.
fn plan(args: FormatArgs, extra: Vec<MetadataRecord>) -> Result<Plan, Failure> {
    // KIP-584 / KIP-778 / KIP-1022 bootstrap: finalize each registered feature
    // at its `--feature` override, else its per-release default for the
    // resolved bootstrap metadata.version (`--feature metadata.version` >
    // `--release-version` > latest stable). A 4.0 format thus seeds
    // metadata.version, group.version, etc. at their 4.0 defaults so a fresh
    // cluster engages each feature with no manual step; a level-0 feature is
    // omitted (absent = disabled), matching `kafka-storage format`.
    let features = resolve_format_features(
        args.release_version.as_deref(),
        &args.feature,
        args.unstable_feature_versions_enable,
    )
    .map_err(|e| krabka(EXIT_INVALID_FEATURE, e))?;
    let metadata_version = metadata_version::from_feature_level(features.metadata_version)
        .map_or_else(
            || features.metadata_version.to_string(),
            |mv| mv.ivn().to_owned(),
        );

    // The `kraft.version` rules come after the release and the feature names,
    // as in `Formatter.run`: it resolves the release, then checks the names in
    // `calculateEffectiveFeatureLevels`, which is where `kraft.version` is
    // reconciled with the quorum flags.
    let dynamic_format = is_dynamic_format(&args).map_err(|e| krabka(EXIT_INVALID_FEATURE, e))?;
    let MetadataIdentity {
        node_id,
        directory_id: metadata_directory_id,
        voters: initial_voters,
        kind: metadata_kind,
    } = resolve_metadata_identity(&args, dynamic_format)?;

    let raft_control_records = raft_control_records(dynamic_format, initial_voters);

    let records = bootstrap_records(&features, extra, &args.add_scram, args.add_acl)?;

    let metadata_log_dir = args
        .metadata_log_dir
        .or_else(|| args.log_dirs.first().cloned())
        .ok_or_else(|| krabka(EXIT_BOOTSTRAP_FAIL, "no log directory to format"))?;
    let ensemble = survey(&metadata_log_dir, args.log_dirs)?;
    let cluster_id = verify_formatted(&ensemble, args.cluster_id, node_id, args.ignore_formatted)?;

    let metadata_target = Target {
        dir: metadata_log_dir,
        kind: metadata_kind,
        directory_id: metadata_directory_id,
    };
    let targets = assign_targets(ensemble.empty, &ensemble.formatted, &metadata_target);
    if targets.is_empty() && ensemble.formatted.is_empty() {
        return Err(Failure {
            code: EXIT_BOOTSTRAP_FAIL,
            message: "No available log directories to format.".to_owned(),
        });
    }

    Ok(Plan {
        cluster_id,
        node_id,
        metadata_version,
        targets,
        skipped: ensemble
            .formatted
            .into_iter()
            .map(|formatted| formatted.dir)
            .collect(),
        errors: ensemble.errors,
        raft_control_records,
        records,
    })
}

/// Who this node is, and what its metadata log directory holds.
struct MetadataIdentity {
    /// `--node-id`, as the `int` that `meta.properties` records.
    node_id: i32,
    /// The directory id of the metadata log directory.
    directory_id: DirectoryId,
    /// The initial voters of a dynamic quorum, empty for any other format.
    voters: VoterSet,
    kind: DirectoryKind,
}

/// Resolves the node id, the metadata log directory's id, and the initial
/// voters, and checks that `--directory-id` agrees with this node's
/// `--initial-controllers` entry.
fn resolve_metadata_identity(
    args: &FormatArgs,
    dynamic_format: bool,
) -> Result<MetadataIdentity, Failure> {
    // `meta.properties` holds the node id as Kafka's `int`. `--node-id`
    // parses only that range, so this refuses nothing that clap accepted.
    let node_id = i32::try_from(args.node_id.0).map_err(|_| {
        krabka(
            EXIT_BOOTSTRAP_FAIL,
            "You must specify a valid non-negative node ID.",
        )
    })?;

    // KIP-853: this node's stable directory id for the metadata log
    // directory. The broker reads it back from `meta.properties` on every
    // boot; it is the identity component of every `Voter` this node ever
    // appears as.
    let generated_directory_id = args.directory_id.unwrap_or_else(DirectoryId::random);
    let voters = build_initial_voters(args, generated_directory_id)
        .map_err(|e| krabka(EXIT_BOOTSTRAP_FAIL, e))?;
    let directory_id = if args.initial_controllers.is_empty() {
        generated_directory_id
    } else {
        DirectoryId(
            voters
                .get(args.node_id)
                .ok_or_else(|| {
                    krabka(
                        EXIT_BOOTSTRAP_FAIL,
                        "--initial-controllers has no entry for this node",
                    )
                })?
                .directory_id,
        )
    };
    if args.directory_id.is_some() && directory_id != generated_directory_id {
        return Err(krabka(
            EXIT_BOOTSTRAP_FAIL,
            "--directory-id must match the local --initial-controllers entry",
        ));
    }
    let kind = metadata_directory_kind(dynamic_format, args, &voters);
    Ok(MetadataIdentity {
        node_id,
        directory_id,
        voters,
        kind,
    })
}

/// The KIP-853 control records. They live in the offset-zero metadata
/// checkpoint, separate from the non-Raft bootstrap record stream: the
/// `KRaft` version of a dynamic format, then its initial voters when it has
/// any.
fn raft_control_records(dynamic_format: bool, initial_voters: VoterSet) -> Vec<MetadataRecord> {
    let mut records = Vec::new();
    if dynamic_format {
        records.push(MetadataRecord::V1KRaftVersion(KRaftVersionRecord {
            kraft_version: 1,
        }));
        if !initial_voters.is_empty() {
            records.push(MetadataRecord::V1Voters(VotersRecord {
                voters: initial_voters,
            }));
        }
    }
    records
}

/// The non-Raft bootstrap record stream: the finalized feature levels, then
/// `extra`, then a credential per `--add-scram`, then an entry per
/// `--add-acl`.
///
/// Caller-seeded records (a restore's recovered topics) follow the feature
/// levels that decide how they are read, and precede the credential and ACL
/// records so a seeded ACL names a topic the image already holds.
fn bootstrap_records(
    features: &ResolvedFeatures,
    extra: Vec<MetadataRecord>,
    scram: &[ScramSpec],
    acls: Vec<AclEntry>,
) -> Result<Vec<MetadataRecord>, Failure> {
    let mut records: Vec<MetadataRecord> =
        krabka_metadata::bootstrap_feature_records_with_overrides(
            features.metadata_version,
            &features.overrides,
        );
    records.extend(extra);
    records.extend(scram_records(scram)?);
    records.extend(acls.into_iter().map(MetadataRecord::V1AccessControlEntry));
    Ok(records)
}

/// Reads every directory of the run, refusing one that holds files `format`
/// did not write, one that cannot be listed, and a metadata log directory
/// whose `meta.properties` does not read.
fn survey(metadata_log_dir: &Path, log_dirs: Vec<PathBuf>) -> Result<Ensemble, Failure> {
    let log_dirs = directory_set(metadata_log_dir, log_dirs);
    let ensemble = Ensemble::load(&log_dirs).map_err(|error| match error {
        SurveyError::Foreign(dir) => krabka(
            EXIT_DIRTY_LOG_DIR,
            format_args!(
                "refusing to overwrite non-empty log directory {}: it holds files krabka format \
                 did not write",
                dir.display()
            ),
        ),
        SurveyError::Io(dir, e) => krabka(
            EXIT_BOOTSTRAP_FAIL,
            format_args!("cannot read log directory {}: {e}", dir.display()),
        ),
    })?;
    if ensemble.errors.iter().any(|dir| dir == metadata_log_dir) {
        return Err(Failure {
            code: EXIT_DIRTY_LOG_DIR,
            message: format!(
                "Encountered I/O error in metadata log directory {}. Cannot continue.",
                metadata_log_dir.display()
            ),
        });
    }
    Ok(ensemble)
}

/// Checks the formatted directories against each other, `expected` and
/// `node_id`, and returns the run's cluster id: the set's, else a new one.
/// Without `ignore_formatted`, any formatted directory then refuses the run.
fn verify_formatted(
    ensemble: &Ensemble,
    expected: Option<ClusterId>,
    node_id: i32,
    ignore_formatted: bool,
) -> Result<ClusterId, Failure> {
    let cluster_id = ensemble
        .verify(expected, node_id)
        .map_err(|message| Failure {
            code: EXIT_DIRTY_LOG_DIR,
            message,
        })?
        .unwrap_or_else(ClusterId::random);
    if !ignore_formatted && let Some(first) = ensemble.formatted.first() {
        return Err(Failure {
            code: EXIT_DIRTY_LOG_DIR,
            message: format!(
                "Log directory {} is already formatted. Use --ignore-formatted to ignore this \
                 directory and format the others.",
                first.dir.display()
            ),
        });
    }
    Ok(cluster_id)
}

/// The directory each of `empty` gets formatted as. The metadata log
/// directory takes `metadata`'s kind and id; every other directory is a data
/// directory with a fresh id that no directory of the run already holds.
fn assign_targets(
    empty: Vec<PathBuf>,
    formatted: &[FormattedDirectory],
    metadata: &Target,
) -> Vec<Target> {
    let mut used: Vec<DirectoryId> = formatted
        .iter()
        .filter_map(|formatted| formatted.meta.directory_id)
        .collect();
    let mut targets = Vec::with_capacity(empty.len());
    for dir in empty {
        let (kind, directory_id) = if dir == metadata.dir {
            (metadata.kind, metadata.directory_id)
        } else {
            (DirectoryKind::Data, DirectoryId::random_unused(&used))
        };
        used.push(directory_id);
        targets.push(Target {
            dir,
            kind,
            directory_id,
        });
    }
    targets
}

/// The directories one run formats: `metadata_log_dir` first, then every
/// `log_dirs` entry in the order given.
///
/// This is Kafka's `StorageTool.configToLogDirectories`, which adds
/// `metadata.log.dir` to the `log.dirs` set. Kafka keeps the set in a
/// `TreeSet`, so a path named twice is formatted once. The metadata log
/// directory can also be one of `log_dirs`, and then it is formatted once, as
/// the metadata log directory.
fn directory_set(metadata_log_dir: &Path, log_dirs: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut set = Vec::with_capacity(log_dirs.len() + 1);
    set.push(metadata_log_dir.to_path_buf());
    for dir in log_dirs {
        if !set.contains(&dir) {
            set.push(dir);
        }
    }
    set
}

/// Kafka's `DirectoryType.calculate` for the metadata log directory.
fn metadata_directory_kind(
    dynamic_format: bool,
    args: &FormatArgs,
    initial_voters: &VoterSet,
) -> DirectoryKind {
    if !dynamic_format {
        DirectoryKind::StaticMetadata
    } else if initial_voters.contains(args.node_id) {
        DirectoryKind::DynamicMetadataVoter
    } else {
        DirectoryKind::DynamicMetadata
    }
}

/// Hashes each `--add-scram` credential. The hashing happens here, on the
/// formatter's side, with `hash_scram_password_with_salt` from
/// `krabka-security`, so the record on disk carries the stretched keys and
/// never the plain password.
fn scram_records(specs: &[ScramSpec]) -> Result<Vec<MetadataRecord>, Failure> {
    // The minimum is positive, so its magnitude is the minimum itself.
    let min = MIN_SCRAM_ITERATIONS.unsigned_abs();
    let mut records = Vec::with_capacity(specs.len());
    for spec in specs {
        if spec.iterations < min {
            return Err(krabka(
                EXIT_LOW_ITERATIONS,
                format_args!(
                    "iterations must be >= {MIN_SCRAM_ITERATIONS}, got {} for user {}",
                    spec.iterations, spec.name,
                ),
            ));
        }
        let mut salt = vec![0u8; 16];
        SystemRandom::new()
            .fill(&mut salt)
            .map_err(|e| krabka(EXIT_BOOTSTRAP_FAIL, format_args!("rng failure: {e}")))?;
        let cred = hash_scram_password_with_salt(
            spec.password.as_bytes(),
            spec.mechanism,
            spec.iterations,
            salt,
        );
        records.push(MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: spec.name.clone(),
            mechanism: spec.mechanism,
            salt: cred.salt,
            stored_key: cred.stored_key,
            server_key: cred.server_key,
            iterations: cred.iterations,
        }));
    }
    Ok(records)
}

impl Plan {
    /// Writes every target directory, printing Kafka's progress lines to
    /// `progress`.
    fn execute(self, fault: &Fault, progress: &mut dyn Write) -> Result<(), Failure> {
        // A progress line is best effort: losing one does not fail the format.
        for dir in &self.errors {
            let _ = writeln!(
                progress,
                "I/O error trying to read log directory {}. Ignoring...",
                dir.display()
            );
        }
        for dir in &self.skipped {
            let _ = writeln!(
                progress,
                "krabka format: {} is already formatted; leaving it alone",
                dir.display()
            );
        }
        if self.targets.is_empty() {
            let _ = writeln!(
                progress,
                "All of the log directories are already formatted."
            );
            return Ok(());
        }
        for target in &self.targets {
            let _ = writeln!(
                progress,
                "Formatting {} {} with metadata.version {}.",
                target.kind.description(),
                target.dir.display(),
                self.metadata_version,
            );
            self.write(target, fault)
                .map_err(|e| krabka(EXIT_BOOTSTRAP_FAIL, e))?;
        }
        let _ = writeln!(
            progress,
            "Formatted {} log director{} with cluster-id {} ({} seed record(s))",
            self.targets.len(),
            if self.targets.len() == 1 { "y" } else { "ies" },
            self.cluster_id,
            self.records.len(),
        );
        Ok(())
    }

    /// Writes one directory.
    ///
    /// Whatever an interrupted run left is removed first, and
    /// `meta.properties` is written last and published by a rename. Its
    /// presence thus means "a format ran here to completion". A run that
    /// fails partway -- an unwritable checkpoint, a killed process -- leaves a
    /// directory without it, which the next run treats as empty and formats
    /// again. Written first, the marker would make the next unconditional
    /// init-container run exit 0 on a directory with no seed checkpoint, no
    /// voter set, and no bootstrap records.
    fn write(&self, target: &Target, fault: &Fault) -> Result<(), String> {
        let dir = &target.dir;
        remove_partial_output(dir)
            .map_err(|e| format!("cannot clear log directory {}: {e}", dir.display()))?;
        fs::create_dir_all(dir)
            .map_err(|e| format!("cannot create log directory {}: {e}", dir.display()))?;
        if target.kind.is_dynamic_metadata() {
            write_dynamic_checkpoint(
                dir,
                self.cluster_id,
                &self.raft_control_records,
                &self.records,
                fault,
            )
            .map_err(|e| format!("checkpoint failed: {e}"))?;
        }
        if target.kind.is_metadata() {
            write_bootstrap_files(dir, self.cluster_id, &self.records, fault)
                .map_err(|e| format!("bootstrap failed: {e}"))?;
        }
        let meta = MetaProperties {
            cluster_id: self.cluster_id,
            node_id: self.node_id,
            directory_id: Some(target.directory_id),
        };
        write_meta_properties(dir, &meta, fault)
    }
}
