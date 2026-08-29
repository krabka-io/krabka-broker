//! `krabka format` subcommand.
//!
//! Writes bootstrap metadata for a fresh broker:
//! - a randomly-generated (or operator-supplied) cluster id
//! - any seed SCRAM credentials supplied via `--add-scram`
//!
//! ## Bootstrap output format
//!
//! Non-Raft metadata is written as a bootstrap stream for the broker to
//! pre-load. Dynamic KIP-853 modes additionally write the authoritative
//! offset-zero metadata checkpoint. The output is:
//!
//! - `<log_dir>/bootstrap.json` — a human-readable manifest with the
//!   cluster id and a base64'd `serde_wincode` blob per metadata record.
//! - `<log_dir>/bootstrap.records.bin` — the same records concatenated
//!   as length-prefixed `serde_wincode<SerdeCompat<MetadataRecord>>`
//!   payloads, so the broker can stream them without touching JSON.
//! - `<log_dir>/__cluster_metadata/@metadata-0/00000000000000000000-0000000000.checkpoint`
//!   — the KIP-630/KIP-853 bootstrap snapshot for dynamic membership.

use krabka_metadata::{KRaftVersionRecord, MetadataRecord, ScramCredentialRecord, VotersRecord};
use krabka_security::scram::{MIN_SCRAM_ITERATIONS, hash_scram_password_with_salt};
use ring::rand::{SecureRandom, SystemRandom};
use uuid::Uuid;

mod acl;
mod args;
mod features;
mod output;
mod quorum;
mod scram;
#[cfg(test)]
mod tests;

pub use self::args::{FormatArgs, ScramSpec};
use self::{
    features::resolve_format_features,
    output::{write_bootstrap_files, write_dynamic_checkpoint, write_meta_properties},
    quorum::{build_initial_voters, is_dynamic_format},
};
use crate::ids::{ClusterId, DirectoryId};

/// Exit codes:
/// - 0: success
/// - 2: iterations < 4096
/// - 3: `log_dir` non-empty
/// - 4: bootstrap write failure
const EXIT_OK: i32 = 0;
const EXIT_LOW_ITERATIONS: i32 = 2;
const EXIT_DIRTY_LOG_DIR: i32 = 3;
const EXIT_BOOTSTRAP_FAIL: i32 = 4;
const EXIT_INVALID_FEATURE: i32 = 5;

/// Formats `args.log_dir`, returning the process exit code.
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
/// Formats `args.log_dir` with `extra` seeded alongside the records the flags
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
        log_dir = %args.log_dir.display(),
        standalone = args.standalone,
        extra_records = extra.len(),
    )
)]
pub async fn run_with_records(args: FormatArgs, extra: Vec<MetadataRecord>) -> i32 {
    let dynamic_format = match is_dynamic_format(&args) {
        Ok(dynamic) => dynamic,
        Err(e) => {
            eprintln!("krabka format: {e}");
            return EXIT_INVALID_FEATURE;
        }
    };

    // Refuse to overwrite a non-empty directory. We treat "exists with
    // any entry" as non-empty; an empty dir or missing path is OK.
    if args.log_dir.exists() {
        match std::fs::read_dir(&args.log_dir) {
            Ok(mut it) => {
                if it.next().is_some() {
                    eprintln!(
                        "krabka format: refusing to overwrite non-empty log_dir {}",
                        args.log_dir.display(),
                    );
                    return EXIT_DIRTY_LOG_DIR;
                }
            }
            Err(e) => {
                eprintln!(
                    "krabka format: cannot read log_dir {}: {e}",
                    args.log_dir.display(),
                );
                return EXIT_BOOTSTRAP_FAIL;
            }
        }
    }

    if let Err(e) = std::fs::create_dir_all(&args.log_dir) {
        eprintln!(
            "krabka format: cannot create log_dir {}: {e}",
            args.log_dir.display(),
        );
        return EXIT_BOOTSTRAP_FAIL;
    }

    let cluster_id = ClusterId(args.cluster_id.unwrap_or_else(Uuid::new_v4));

    // KIP-853: generate + persist this replica's stable directory id. The
    // broker reads it back from `meta.properties.json` on every boot; it is
    // the identity component of every `Voter` this node ever appears as.
    let generated_directory_id = args
        .directory_id
        .unwrap_or_else(|| DirectoryId(Uuid::new_v4()));
    let initial_voters = match build_initial_voters(&args, generated_directory_id) {
        Ok(voters) => voters,
        Err(e) => {
            eprintln!("krabka format: {e}");
            return EXIT_BOOTSTRAP_FAIL;
        }
    };
    let directory_id = if args.initial_controllers.is_empty() {
        generated_directory_id
    } else {
        DirectoryId(
            initial_voters
                .get(args.node_id.expect("validated initial controller node id"))
                .expect("validated local initial controller")
                .directory_id,
        )
    };
    if args.directory_id.is_some() && directory_id != generated_directory_id {
        eprintln!("krabka format: --directory-id must match the local --initial-controllers entry");
        return EXIT_BOOTSTRAP_FAIL;
    }
    if let Err(e) = write_meta_properties(&args.log_dir, cluster_id, directory_id) {
        eprintln!("krabka format: {e}");
        return EXIT_BOOTSTRAP_FAIL;
    }

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

    let mut records: Vec<MetadataRecord> = Vec::new();

    // KIP-584 / KIP-778 / KIP-1022 bootstrap: finalize each registered feature
    // at its `--feature` override, else its per-release default for the
    // resolved bootstrap metadata.version (`--feature metadata.version` >
    // `--release-version` > latest stable). A 4.0 format thus seeds
    // metadata.version, group.version, etc. at their 4.0 defaults so a fresh
    // cluster engages each feature with no manual step; a level-0 feature is
    // omitted (absent = disabled), matching `kafka-storage format`.
    let (bootstrap_mv, feature_overrides) =
        match resolve_format_features(args.release_version.as_deref(), &args.feature) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("krabka format: {e}");
                return EXIT_INVALID_FEATURE;
            }
        };
    records.extend(krabka_metadata::bootstrap_feature_records_with_overrides(
        bootstrap_mv,
        &feature_overrides,
    ));

    // Caller-seeded records (a restore's recovered topics) follow the feature
    // levels that decide how they are read, and precede the credential and ACL
    // records so a seeded ACL names a topic the image already holds.
    records.extend(extra);

    // Build the seed records. Each `--add-scram` is hashed *here* (CLI
    // side) using `hash_scram_password_with_salt` from `krabka-security`
    // so the on-disk record carries the stretched keys, never the plain
    // password.
    for spec in &args.add_scram {
        if spec.iterations < u32::try_from(MIN_SCRAM_ITERATIONS).expect("SCRAM minimum is positive")
        {
            eprintln!(
                "krabka format: iterations must be >= {MIN_SCRAM_ITERATIONS}, got {} for user {}",
                spec.iterations, spec.name,
            );
            return EXIT_LOW_ITERATIONS;
        }
        let mut salt = vec![0u8; 16];
        if let Err(e) = SystemRandom::new().fill(&mut salt) {
            eprintln!("krabka format: rng failure: {e}");
            return EXIT_BOOTSTRAP_FAIL;
        }
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

    for acl in args.add_acl {
        records.push(MetadataRecord::V1AccessControlEntry(acl));
    }

    if dynamic_format
        && let Err(e) =
            write_dynamic_checkpoint(&args.log_dir, cluster_id, &raft_control_records, &records)
    {
        eprintln!("krabka format: checkpoint failed: {e}");
        return EXIT_BOOTSTRAP_FAIL;
    }

    if let Err(e) = write_bootstrap_files(&args.log_dir, cluster_id, &records) {
        eprintln!("krabka format: bootstrap failed: {e}");
        return EXIT_BOOTSTRAP_FAIL;
    }

    println!(
        "Formatted {} with cluster-id {} ({} seed record(s))",
        args.log_dir.display(),
        cluster_id,
        records.len(),
    );
    EXIT_OK
}
