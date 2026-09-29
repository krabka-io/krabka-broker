//! `krabka format` subcommand.
//!
//! Formats every log directory of a node, as `kafka-storage format` does:
//! each directory gets the same cluster id and its own directory id. The
//! first `--log-dir` is the metadata log directory, the counterpart of Kafka's
//! `metadata.log.dir`, which defaults to the first entry of `log.dirs`.
//!
//! ## Output
//!
//! Non-Raft metadata is written as a bootstrap stream for the broker to
//! pre-load. Dynamic KIP-853 modes additionally write the authoritative
//! offset-zero metadata checkpoint. Each directory receives:
//!
//! - `bootstrap.json` — a human-readable manifest with the cluster id and a
//!   base64'd `serde_wincode` blob per metadata record.
//! - `bootstrap.records.bin` — the same records concatenated as
//!   length-prefixed `serde_wincode<SerdeCompat<MetadataRecord>>` payloads, so
//!   the broker can stream them without touching JSON.
//! - `meta.properties.json` — the cluster and directory ids, written last.
//!
//! The metadata log directory of a dynamic format also receives
//! `__cluster_metadata/@metadata-0/00000000000000000000-0000000000.checkpoint`,
//! the KIP-630/KIP-853 bootstrap snapshot for dynamic membership.
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

use std::path::PathBuf;

use krabka_metadata::{
    KRaftVersionRecord, MetadataRecord, ScramCredentialRecord, VoterSet, VotersRecord,
};
use krabka_security::scram::{MIN_SCRAM_ITERATIONS, hash_scram_password_with_salt};
use ring::rand::{SecureRandom, SystemRandom};

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
    args::{FormatArgs, ScramSpec},
    features::LATEST_PRODUCTION_METADATA_VERSION,
    output::{FAIL_AFTER_ENV, META_PROPERTIES_VERSION},
};
use self::{
    ensemble::{Ensemble, SurveyError, remove_partial_output},
    features::resolve_format_features,
    output::{Fault, write_bootstrap_files, write_dynamic_checkpoint, write_meta_properties},
    quorum::{build_initial_voters, is_dynamic_format},
};
use crate::ids::{ClusterId, DirectoryId};

/// The file a formatted directory is recognised by: the broker reads the
/// cluster and directory ids back out of it on every boot.
const META_PROPERTIES: &str = "meta.properties.json";

/// Exit codes. The table in the module documentation gives the cause of each.
const EXIT_OK: i32 = 0;
const EXIT_LOW_ITERATIONS: i32 = 2;
const EXIT_DIRTY_LOG_DIR: i32 = 3;
const EXIT_BOOTSTRAP_FAIL: i32 = 4;
const EXIT_INVALID_FEATURE: i32 = 5;

/// Formats `args.log_dirs`, returning the process exit code.
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
pub async fn run(args: FormatArgs) -> i32 {
    run_with_records(args, Vec::new()).await
}

// `async` matches the entry point in `main.rs`; the body is sync today
// (purely fs + crypto) but a real raft-log bootstrap would await tokio I/O.
// The body yields an `i32` (not a future), so `#[instrument]` is safe here
// w.r.t. `clippy::async_yields_async`.
/// Formats `args.log_dirs` with `extra` seeded alongside the records the flags
/// produce, returning the process exit code.
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
#[tracing::instrument(
    level = "info",
    name = "cli.format",
    skip_all,
    fields(
        log_dirs = ?args.log_dirs,
        standalone = args.standalone,
        extra_records = extra.len(),
    )
)]
pub async fn run_with_records(args: FormatArgs, extra: Vec<MetadataRecord>) -> i32 {
    match plan(args, extra).and_then(|plan| plan.execute(&Fault::from_env())) {
        Ok(()) => EXIT_OK,
        Err((code, message)) => {
            eprintln!("{message}");
            code
        }
    }
}

/// A failed run: its exit code and the line it prints on stderr.
type Failure = (i32, String);

/// Prefixes `message` with the command name, as every krabka-specific
/// message is. Kafka's own messages are printed bare.
fn krabka(code: i32, message: impl std::fmt::Display) -> Failure {
    (code, format!("krabka format: {message}"))
}

/// What a run writes, and where. [`plan`] builds it without touching the
/// disk beyond reading it, so every validation failure leaves the directories
/// as they were.
struct Plan {
    cluster_id: ClusterId,
    metadata_version: String,
    /// The directories to write, the metadata log directory first when it is
    /// one of them.
    targets: Vec<Target>,
    /// The formatted directories `--ignore-formatted` skips.
    skipped: Vec<PathBuf>,
    /// The directories whose `meta.properties.json` does not read.
    errors: Vec<PathBuf>,
    raft_control_records: Vec<MetadataRecord>,
    records: Vec<MetadataRecord>,
}

/// One directory to format.
struct Target {
    dir: PathBuf,
    kind: DirectoryKind,
    directory_id: DirectoryId,
}

/// Kafka's `Formatter.DirectoryType`: what a directory holds, which decides
/// whether it gets the checkpoint and how the run describes it.
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
    let (bootstrap_mv, feature_overrides) = resolve_format_features(
        args.release_version.as_deref(),
        &args.feature,
        args.unstable_feature_versions_enable,
    )
    .map_err(|e| krabka(EXIT_INVALID_FEATURE, e))?;
    let metadata_version = krabka_metadata::metadata_version::from_feature_level(bootstrap_mv)
        .map_or_else(|| bootstrap_mv.to_string(), |mv| mv.ivn().to_owned());

    // The `kraft.version` rules come after the release and the feature names,
    // as in `Formatter.run`: it resolves the release, then checks the names in
    // `calculateEffectiveFeatureLevels`, which is where `kraft.version` is
    // reconciled with the quorum flags.
    let dynamic_format = is_dynamic_format(&args).map_err(|e| krabka(EXIT_INVALID_FEATURE, e))?;

    // KIP-853: this node's stable directory id for the metadata log
    // directory. The broker reads it back from `meta.properties.json` on
    // every boot; it is the identity component of every `Voter` this node
    // ever appears as.
    let generated_directory_id = args.directory_id.unwrap_or_else(DirectoryId::random);
    let initial_voters = build_initial_voters(&args, generated_directory_id)
        .map_err(|e| krabka(EXIT_BOOTSTRAP_FAIL, e))?;
    let metadata_directory_id = if args.initial_controllers.is_empty() {
        generated_directory_id
    } else {
        DirectoryId(
            initial_voters
                .get(args.node_id.expect("validated initial controller node id"))
                .expect("validated local initial controller")
                .directory_id,
        )
    };
    if args.directory_id.is_some() && metadata_directory_id != generated_directory_id {
        return Err(krabka(
            EXIT_BOOTSTRAP_FAIL,
            "--directory-id must match the local --initial-controllers entry",
        ));
    }
    let metadata_kind = metadata_directory_kind(dynamic_format, &args, &initial_voters);

    // KIP-853 control records live in the offset-zero metadata checkpoint,
    // separate from the non-Raft bootstrap record stream.
    let mut raft_control_records = Vec::new();
    if dynamic_format {
        raft_control_records.push(MetadataRecord::V1KRaftVersion(KRaftVersionRecord {
            kraft_version: 1,
        }));
        if !initial_voters.is_empty() {
            raft_control_records.push(MetadataRecord::V1Voters(VotersRecord {
                voters: initial_voters,
            }));
        }
    }

    let mut records: Vec<MetadataRecord> =
        krabka_metadata::bootstrap_feature_records_with_overrides(bootstrap_mv, &feature_overrides);
    // Caller-seeded records (a restore's recovered topics) follow the feature
    // levels that decide how they are read, and precede the credential and ACL
    // records so a seeded ACL names a topic the image already holds.
    records.extend(extra);
    records.extend(scram_records(&args)?);
    records.extend(
        args.add_acl
            .into_iter()
            .map(MetadataRecord::V1AccessControlEntry),
    );

    // Kafka keeps the directories in a set, so a path named twice is
    // formatted once.
    let mut log_dirs: Vec<PathBuf> = Vec::with_capacity(args.log_dirs.len());
    for dir in args.log_dirs {
        if !log_dirs.contains(&dir) {
            log_dirs.push(dir);
        }
    }
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
    if ensemble.errors.contains(&log_dirs[0]) {
        return Err((
            EXIT_DIRTY_LOG_DIR,
            format!(
                "Encountered I/O error in metadata log directory {}. Cannot continue.",
                log_dirs[0].display()
            ),
        ));
    }
    let cluster_id = ensemble
        .verify(args.cluster_id)
        .map_err(|message| (EXIT_DIRTY_LOG_DIR, message))?
        .unwrap_or_else(ClusterId::random);
    if !args.ignore_formatted
        && let Some((first, _)) = ensemble.formatted.first()
    {
        return Err((
            EXIT_DIRTY_LOG_DIR,
            format!(
                "Log directory {} is already formatted. Use --ignore-formatted to ignore this \
                 directory and format the others.",
                first.display()
            ),
        ));
    }

    let mut used: Vec<DirectoryId> = ensemble
        .formatted
        .iter()
        .map(|(_, meta)| meta.directory_id)
        .collect();
    let mut targets = Vec::with_capacity(ensemble.empty.len());
    for dir in ensemble.empty {
        let (kind, directory_id) = if dir == log_dirs[0] {
            (metadata_kind, metadata_directory_id)
        } else {
            (DirectoryKind::Data, fresh_directory_id(&used))
        };
        used.push(directory_id);
        targets.push(Target {
            dir,
            kind,
            directory_id,
        });
    }
    if targets.is_empty() && ensemble.formatted.is_empty() {
        return Err((
            EXIT_BOOTSTRAP_FAIL,
            "No available log directories to format.".to_owned(),
        ));
    }

    Ok(Plan {
        cluster_id,
        metadata_version,
        targets,
        skipped: ensemble.formatted.into_iter().map(|(dir, _)| dir).collect(),
        errors: ensemble.errors,
        raft_control_records,
        records,
    })
}

/// Kafka's `DirectoryType.calculate` for the metadata log directory.
fn metadata_directory_kind(
    dynamic_format: bool,
    args: &FormatArgs,
    initial_voters: &VoterSet,
) -> DirectoryKind {
    if !dynamic_format {
        DirectoryKind::StaticMetadata
    } else if args.node_id.is_some_and(|id| initial_voters.contains(id)) {
        DirectoryKind::DynamicMetadataVoter
    } else {
        DirectoryKind::DynamicMetadata
    }
}

/// A random directory id that no other directory of the node holds, as
/// Kafka's `Copier.generateValidDirectoryId` returns.
fn fresh_directory_id(used: &[DirectoryId]) -> DirectoryId {
    loop {
        let id = DirectoryId::random();
        if !used.contains(&id) {
            return id;
        }
    }
}

/// Hashes each `--add-scram` credential. The hashing happens here, on the
/// formatter's side, with `hash_scram_password_with_salt` from
/// `krabka-security`, so the record on disk carries the stretched keys and
/// never the plain password.
fn scram_records(args: &FormatArgs) -> Result<Vec<MetadataRecord>, Failure> {
    let min = u32::try_from(MIN_SCRAM_ITERATIONS).expect("SCRAM minimum is positive");
    let mut records = Vec::with_capacity(args.add_scram.len());
    for spec in &args.add_scram {
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
    /// Writes every target directory, printing Kafka's progress lines.
    fn execute(self, fault: &Fault) -> Result<(), Failure> {
        for dir in &self.errors {
            println!(
                "I/O error trying to read log directory {}. Ignoring...",
                dir.display()
            );
        }
        for dir in &self.skipped {
            println!(
                "krabka format: {} is already formatted; leaving it alone",
                dir.display()
            );
        }
        if self.targets.is_empty() {
            println!("All of the log directories are already formatted.");
            return Ok(());
        }
        for target in &self.targets {
            println!(
                "Formatting {} {} with metadata.version {}.",
                target.kind.description(),
                target.dir.display(),
                self.metadata_version,
            );
            self.write(target, fault)
                .map_err(|e| krabka(EXIT_BOOTSTRAP_FAIL, e))?;
        }
        println!(
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
    /// `meta.properties.json` is written last and published by a rename. Its
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
        std::fs::create_dir_all(dir)
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
        write_bootstrap_files(dir, self.cluster_id, &self.records, fault)
            .map_err(|e| format!("bootstrap failed: {e}"))?;
        write_meta_properties(dir, self.cluster_id, target.directory_id, fault)
    }
}
