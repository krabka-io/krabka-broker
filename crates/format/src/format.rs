//! `crabka format` subcommand.
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

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use clap::Args;
use crabka_metadata::{
    AclEntry, KRaftVersionRange, KRaftVersionRecord, MetadataRecord, ScramCredentialRecord, Voter,
    VoterEndpoint, VoterSet, VotersRecord, metadata_version::KRAFT_VERSION_FEATURE,
};
use crabka_security::{
    SaslMechanism,
    scram::{MIN_SCRAM_ITERATIONS, hash_scram_password_with_salt},
};
use ring::rand::{SecureRandom, SystemRandom};
use serde::Serialize;
use serde_wincode::SerdeCompat;
use uuid::Uuid;
use wincode::Serialize as _;

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
const ZERO_CHECKPOINT_NAME: &str = "00000000000000000000-0000000000.checkpoint";

#[derive(Args, Debug)]
pub struct FormatArgs {
    /// Directory to format. Must be empty or non-existent.
    #[arg(long)]
    log_dir: PathBuf,
    /// Cluster id. Generated if not provided.
    #[arg(long)]
    cluster_id: Option<Uuid>,
    /// Bootstrap `metadata.version` (KIP-778), e.g. `4.0` or `4.0-IV3`.
    /// Defaults to the broker's maximum supported level when omitted.
    #[arg(long)]
    release_version: Option<String>,
    /// Set an individual feature's finalized level at format time (KIP-1022),
    /// e.g. `--feature transaction.version=2`. May be repeated. Combines with
    /// `--release-version` (which sets the base release) for every feature
    /// except `metadata.version`, where the two conflict.
    #[arg(long = "feature", value_parser = parse_feature_spec)]
    feature: Vec<(String, i16)>,
    /// Seed a SCRAM credential. May be repeated.
    /// Format: `SCRAM-SHA-256=[name=<u>,password=<p>,iterations=<n>]`
    /// or `SCRAM-SHA-512=[name=<u>,password=<p>,iterations=<n>]`
    /// (iterations defaults to 4096 when omitted)
    #[arg(long, value_parser = parse_scram_spec)]
    add_scram: Vec<ScramSpec>,
    /// Seed an ACL entry. May be repeated.
    /// Format: `principal=User:<name>,host=<ip|*>,operation=<Op>,permission=<Allow|Deny>,resource=<Type>:<Name>[:<Pattern>]`
    /// Pattern defaults to `Literal`.
    #[arg(long, value_parser = parse_acl_spec)]
    add_acl: Vec<AclEntry>,
    /// This node's raft id. Required with `--standalone` and
    /// `--initial-controllers` so the local directory id can be persisted.
    #[arg(long, value_parser = parse_node_id)]
    node_id: Option<crabka_metadata::NodeId>,
    /// Stable directory identity. Intended for orchestrators that must verify
    /// the exact node incarnation before declaring it ready.
    #[arg(long, value_parser = parse_directory_id)]
    directory_id: Option<DirectoryId>,
    /// Format this node as the sole initial controller voter.
    #[arg(
        long,
        conflicts_with_all = ["initial_controllers", "no_initial_controllers"]
    )]
    standalone: bool,
    /// Explicit initial controllers: `id@host:port:directory-id`, comma-separated.
    #[arg(
        long,
        value_delimiter = ',',
        conflicts_with_all = ["standalone", "no_initial_controllers"]
    )]
    initial_controllers: Vec<String>,
    /// Format a dynamic controller that will join an existing quorum.
    #[arg(
        long,
        conflicts_with_all = ["standalone", "initial_controllers"]
    )]
    no_initial_controllers: bool,
    /// This node's controller listener (`host:port`) — written into the
    /// `VotersRecord` when `--standalone`.
    #[arg(long)]
    controller_listener: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramSpec {
    mechanism: SaslMechanism,
    name: String,
    password: String,
    iterations: u32,
}

/// Map a release string to a supported `metadata.version` feature level,
/// erroring if it is unknown or outside `[MIN, MAX]`.
fn resolve_release_level(s: &str) -> Result<i16, String> {
    let mv = crabka_metadata::metadata_version::from_version_string(s)
        .ok_or_else(|| format!("unknown metadata.version {s:?}"))?;
    let level = mv.feature_level();
    if !crabka_metadata::metadata_version::is_supported_level(level) {
        return Err(format!(
            "metadata.version {s:?} (level {level}) is outside the supported range"
        ));
    }
    Ok(level)
}

/// Parse a node id: a bare `u64` wrapped in the `NodeId` newtype.
fn parse_node_id(s: &str) -> Result<crabka_metadata::NodeId, String> {
    let id: u64 = s.trim().parse().map_err(|e| format!("node id: {e}"))?;
    Ok(crabka_metadata::NodeId(id))
}

fn parse_directory_id(s: &str) -> Result<DirectoryId, String> {
    Uuid::parse_str(s)
        .map(DirectoryId)
        .map_err(|error| format!("directory id: {error}"))
}

/// Parse one `--feature NAME=LEVEL` spec into `(name, level)`.
fn parse_feature_spec(s: &str) -> Result<(String, i16), String> {
    let (name, level) = s
        .split_once('=')
        .ok_or("--feature must be NAME=LEVEL, e.g. transaction.version=2")?;
    let name = name.trim();
    if name.is_empty() {
        return Err("feature name must not be empty".into());
    }
    let level: i16 = level
        .trim()
        .parse()
        .map_err(|e| format!("feature level: {e}"))?;
    Ok((name.to_string(), level))
}

/// Resolve `crabka format`'s KIP-1022 feature flags into the bootstrap
/// `metadata.version` level and the per-feature override map, applying the
/// validation `kafka-storage format` performs:
///
/// - every `--feature` names a registered feature, finalized in its supported
///   range (else reject);
/// - `--feature metadata.version=X` conflicts with `--release-version`;
/// - `bootstrap_mv` = `--feature metadata.version` if set, else
///   `--release-version`, else the newest supported level (latest stable);
/// - the fully-resolved feature set satisfies every KIP-1022 dependency.
fn resolve_format_features(
    release_version: Option<&str>,
    features: &[(String, i16)],
) -> Result<(i16, BTreeMap<String, i16>), String> {
    use crabka_metadata::metadata_version::{METADATA_VERSION_FEATURE, METADATA_VERSION_MAX};

    let mut overrides: BTreeMap<String, i16> = BTreeMap::new();
    let mut feature_mv: Option<i16> = None;

    for (name, level) in features {
        // KIP-853 persists kraft.version as a raft control record, never as a
        // FeatureLevelRecord. Its mode-specific validation happens separately.
        if name == KRAFT_VERSION_FEATURE {
            continue;
        }
        let Some(feat) = crabka_metadata::feature(name) else {
            let mut known: Vec<&str> = crabka_metadata::feature_registry()
                .iter()
                .map(|f| f.name())
                .collect();
            known.sort_unstable();
            return Err(format!(
                "Unsupported feature: {name}. Supported features are: {}",
                known.join(", ")
            ));
        };
        let (min, max) = feat.supported_range();
        if *level < min || *level > max {
            return Err(format!(
                "feature {name}={level} is outside the supported range {min}..={max}"
            ));
        }
        if name == METADATA_VERSION_FEATURE {
            if release_version.is_some() {
                return Err(
                    "Use --release-version instead of --feature metadata.version=X to avoid ambiguity.".into(),
                );
            }
            feature_mv = Some(*level);
        }
        if overrides.insert(name.clone(), *level).is_some() {
            return Err(format!("feature {name} specified more than once"));
        }
    }

    let bootstrap_mv = if let Some(mv) = feature_mv {
        mv
    } else if let Some(rv) = release_version {
        resolve_release_level(rv)?
    } else {
        METADATA_VERSION_MAX
    };

    // KIP-1022 dependency validation over the fully-resolved feature set
    // (every registered feature at its override-or-default level).
    let resolved: BTreeMap<String, i16> = crabka_metadata::feature_registry()
        .iter()
        .map(|f| {
            let level = overrides
                .get(f.name())
                .copied()
                .unwrap_or_else(|| f.default_level(bootstrap_mv));
            (f.name().to_string(), level)
        })
        .collect();
    crabka_metadata::validate_feature_dependencies(&resolved)?;

    Ok((bootstrap_mv, overrides))
}

/// Resolve the KIP-853 format mode and validate its kraft.version selection.
///
/// The three explicit quorum flags select dynamic membership and therefore
/// imply level 1. Omitting all three retains the static level-0 path.
fn is_dynamic_format(args: &FormatArgs) -> Result<bool, String> {
    let dynamic =
        args.standalone || !args.initial_controllers.is_empty() || args.no_initial_controllers;
    let mut requested = None;
    for (name, level) in &args.feature {
        if name != KRAFT_VERSION_FEATURE {
            continue;
        }
        if requested.replace(*level).is_some() {
            return Err("feature kraft.version specified more than once".into());
        }
        if !(0..=1).contains(level) {
            return Err(format!(
                "feature kraft.version={level} is outside the supported range 0..=1"
            ));
        }
    }

    match (dynamic, requested) {
        (true, None | Some(1)) => Ok(true),
        (true, Some(0)) => Err(
            "--standalone, --initial-controllers, and --no-initial-controllers require kraft.version=1"
                .into(),
        ),
        (false, None | Some(0)) => Ok(false),
        (false, Some(1)) => Err(
            "kraft.version=1 requires --standalone, --initial-controllers, or --no-initial-controllers"
                .into(),
        ),
        _ => unreachable!("kraft.version range was validated above"),
    }
}

fn parse_scram_spec(s: &str) -> Result<ScramSpec, String> {
    let s = s.trim();
    let (mechanism, body) = if let Some(rest) = s.strip_prefix("SCRAM-SHA-512=[") {
        (SaslMechanism::ScramSha512, rest)
    } else if let Some(rest) = s.strip_prefix("SCRAM-SHA-256=[") {
        (SaslMechanism::ScramSha256, rest)
    } else {
        return Err("must start with SCRAM-SHA-256=[ or SCRAM-SHA-512=[".into());
    };
    let body = body.strip_suffix(']').ok_or("must end with ]")?;
    let mut name = None;
    let mut password = None;
    let mut iterations = u32::try_from(MIN_SCRAM_ITERATIONS).expect("SCRAM minimum is positive");
    for attr in body.split(',') {
        let (k, v) = attr
            .split_once('=')
            .ok_or_else(|| format!("malformed attr: {attr}"))?;
        match k.trim() {
            "name" => name = Some(v.trim().to_string()),
            "password" => password = Some(v.trim().to_string()),
            "iterations" => {
                iterations = v.trim().parse().map_err(|e| format!("iterations: {e}"))?;
            }
            other => return Err(format!("unknown attr: {other}")),
        }
    }
    Ok(ScramSpec {
        mechanism,
        name: name.ok_or("missing name")?,
        password: password.ok_or("missing password")?,
        iterations,
    })
}

fn parse_acl_spec(spec: &str) -> Result<AclEntry, String> {
    use crabka_metadata::{AclOperation, PatternType, PermissionType, ResourceType};

    let mut principal = None;
    let mut host = None;
    let mut operation = None;
    let mut permission = None;
    let mut resource_type = None;
    let mut resource_name = None;
    let mut pattern_type = PatternType::Literal;

    for kv in spec.split(',') {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("malformed pair: {kv}"))?;
        match k {
            "principal" => principal = Some(v.to_string()),
            "host" => host = Some(v.to_string()),
            "operation" => {
                operation = Some(match v {
                    "All" => AclOperation::All,
                    "Read" => AclOperation::Read,
                    "Write" => AclOperation::Write,
                    "Create" => AclOperation::Create,
                    "Delete" => AclOperation::Delete,
                    "Alter" => AclOperation::Alter,
                    "Describe" => AclOperation::Describe,
                    "ClusterAction" => AclOperation::ClusterAction,
                    "DescribeConfigs" => AclOperation::DescribeConfigs,
                    "AlterConfigs" => AclOperation::AlterConfigs,
                    "IdempotentWrite" => AclOperation::IdempotentWrite,
                    "TwoPhaseCommit" => AclOperation::TwoPhaseCommit,
                    other => return Err(format!("unknown operation: {other}")),
                });
            }
            "permission" => {
                permission = Some(match v {
                    "Allow" => PermissionType::Allow,
                    "Deny" => PermissionType::Deny,
                    other => return Err(format!("unknown permission: {other}")),
                });
            }
            "resource" => {
                let mut parts = v.splitn(3, ':');
                let rt = parts.next().ok_or("missing resource type")?;
                let rn = parts.next().ok_or("missing resource name")?;
                if let Some(pt) = parts.next() {
                    pattern_type = match pt {
                        "Literal" => PatternType::Literal,
                        "Prefixed" => PatternType::Prefixed,
                        other => return Err(format!("unknown pattern: {other}")),
                    };
                }
                resource_type = Some(match rt {
                    "Topic" => ResourceType::Topic,
                    "Group" => ResourceType::Group,
                    "Cluster" => ResourceType::Cluster,
                    "TransactionalId" => ResourceType::TransactionalId,
                    other => return Err(format!("unknown resource type: {other}")),
                });
                resource_name = Some(rn.to_string());
            }
            other => return Err(format!("unknown key: {other}")),
        }
    }

    Ok(AclEntry {
        resource_type: resource_type.ok_or("resource required")?,
        resource_name: resource_name.ok_or("resource_name required")?,
        pattern_type,
        principal: principal.ok_or("principal required")?,
        host: host.ok_or("host required")?,
        operation: operation.ok_or("operation required")?,
        permission_type: permission.ok_or("permission required")?,
    })
}

/// Parse one `--initial-controllers` entry: `id@host:port:directory-id`.
///
/// The directory uuid is the trailing colon-delimited field, so we split
/// it off the right first, then peel `host:port` off the remainder.
fn parse_initial_controller(spec: &str) -> Result<Voter, String> {
    let (id_part, rest) = spec.split_once('@').ok_or("missing '@'")?;
    let id = crabka_metadata::NodeId(id_part.parse::<u64>().map_err(|_| "bad id")?);
    let (host_port, dir_part) = rest.rsplit_once(':').ok_or("missing directory uuid")?;
    let dir: Uuid = dir_part.parse().map_err(|_| "bad directory uuid")?;
    if dir.is_nil() {
        return Err("directory uuid must not be nil".into());
    }
    let (host, port) = host_port.rsplit_once(':').ok_or("missing host:port")?;
    if host.is_empty() {
        return Err("host must not be empty".into());
    }
    let port: u16 = port.parse().map_err(|_| "bad port")?;
    if port == 0 {
        return Err("port must not be zero".into());
    }
    Ok(Voter {
        id,
        directory_id: dir,
        endpoints: vec![VoterEndpoint {
            name: "CONTROLLER".into(),
            host: host.to_string(),
            port,
        }],
        kraft_version: KRaftVersionRange::default(),
    })
}

/// Derive the initial controller voter set from the format args.
///
/// - `--standalone`: a singleton set holding just this node (requires
///   `--node-id` + `--controller-listener`).
/// - `--initial-controllers`: the explicitly-listed voters.
/// - `--no-initial-controllers` or static mode: an empty set.
fn build_initial_voters(args: &FormatArgs, directory_id: DirectoryId) -> Result<VoterSet, String> {
    if args.standalone {
        let id = args.node_id.ok_or("--standalone requires --node-id")?;
        let listener = args
            .controller_listener
            .as_deref()
            .ok_or("--standalone requires --controller-listener")?;
        let (host, port) = listener
            .rsplit_once(':')
            .ok_or("--controller-listener must be host:port")?;
        if host.is_empty() {
            return Err("--controller-listener host must not be empty".into());
        }
        let port: u16 = port.parse().map_err(|_| "bad --controller-listener port")?;
        if port == 0 {
            return Err("--controller-listener port must not be zero".into());
        }
        Ok(VoterSet::from_voters([Voter {
            id,
            // `Voter.directory_id` is a raw `Uuid` (owned by `crabka_voters`);
            // unwrap the newtype at this crate boundary.
            directory_id: directory_id.into(),
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: host.to_string(),
                port,
            }],
            kraft_version: KRaftVersionRange::default(),
        }]))
    } else if !args.initial_controllers.is_empty() {
        let voters: Vec<_> = args
            .initial_controllers
            .iter()
            .map(|s| parse_initial_controller(s))
            .collect::<Result<_, _>>()?;
        let mut node_ids = BTreeSet::new();
        let mut directory_ids = BTreeSet::new();
        for voter in &voters {
            if !node_ids.insert(voter.id) {
                return Err(format!("duplicate initial controller id {}", voter.id));
            }
            if !directory_ids.insert(voter.directory_id) {
                return Err(format!(
                    "duplicate initial controller directory id {}",
                    voter.directory_id
                ));
            }
        }
        let voters = VoterSet::from_voters(voters);
        let node_id = args
            .node_id
            .ok_or("--initial-controllers requires --node-id")?;
        if !voters.contains(node_id) {
            return Err(format!(
                "--initial-controllers does not contain local --node-id {node_id}"
            ));
        }
        Ok(voters)
    } else {
        Ok(VoterSet::default())
    }
}

/// Persist `meta.properties.json` — the broker recovers `directory_id`
/// from it on every boot (KIP-853 voter identity).
#[tracing::instrument(
    level = "debug",
    name = "cli.write_meta_properties",
    skip_all,
    fields(log_dir = %log_dir.display(), %cluster_id, %directory_id),
    err
)]
fn write_meta_properties(
    log_dir: &std::path::Path,
    cluster_id: ClusterId,
    directory_id: DirectoryId,
) -> Result<(), String> {
    let meta = serde_json::json!({
        "cluster_id": cluster_id.to_string(),
        "directory_id": directory_id.to_string(),
        "version": 1,
    });
    let bytes = serde_json::to_vec_pretty(&meta)
        .map_err(|e| format!("serialize meta.properties.json: {e}"))?;
    std::fs::write(log_dir.join("meta.properties.json"), bytes)
        .map_err(|e| format!("write meta.properties.json: {e}"))
}

/// Human-readable manifest written to `<log_dir>/bootstrap.json`.
#[derive(Debug, Serialize)]
struct BootstrapManifest {
    /// Schema version of this bootstrap manifest. Bumped if the layout
    /// changes; the broker's future consumer will reject unknown values.
    schema: u32,
    // `ClusterId` is `#[serde(transparent)]`, so this serializes as the bare
    // UUID string exactly as the previous `Uuid` field did.
    cluster_id: ClusterId,
    record_count: usize,
    /// Base64-encoded `SerdeCompat<MetadataRecord>` payloads, one per
    /// seed record. Mirrors the contents of `bootstrap.records.bin` so
    /// operators can inspect the file without a hex editor.
    records_b64: Vec<String>,
}

// `async` matches the entry point in `main.rs`; the body is sync today
// (purely fs + crypto) but a real raft-log bootstrap would await tokio I/O.
// The body yields an `i32` (not a future), so `#[instrument]` is safe here
// w.r.t. `clippy::async_yields_async`.
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
#[tracing::instrument(
    level = "info",
    name = "cli.format",
    skip_all,
    fields(log_dir = %args.log_dir.display(), standalone = args.standalone)
)]
pub async fn run(args: FormatArgs) -> i32 {
    let dynamic_format = match is_dynamic_format(&args) {
        Ok(dynamic) => dynamic,
        Err(e) => {
            eprintln!("crabka format: {e}");
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
                        "crabka format: refusing to overwrite non-empty log_dir {}",
                        args.log_dir.display(),
                    );
                    return EXIT_DIRTY_LOG_DIR;
                }
            }
            Err(e) => {
                eprintln!(
                    "crabka format: cannot read log_dir {}: {e}",
                    args.log_dir.display(),
                );
                return EXIT_BOOTSTRAP_FAIL;
            }
        }
    }

    if let Err(e) = std::fs::create_dir_all(&args.log_dir) {
        eprintln!(
            "crabka format: cannot create log_dir {}: {e}",
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
            eprintln!("crabka format: {e}");
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
        eprintln!("crabka format: --directory-id must match the local --initial-controllers entry");
        return EXIT_BOOTSTRAP_FAIL;
    }
    if let Err(e) = write_meta_properties(&args.log_dir, cluster_id, directory_id) {
        eprintln!("crabka format: {e}");
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
                eprintln!("crabka format: {e}");
                return EXIT_INVALID_FEATURE;
            }
        };
    records.extend(crabka_metadata::bootstrap_feature_records_with_overrides(
        bootstrap_mv,
        &feature_overrides,
    ));

    // Build the seed records. Each `--add-scram` is hashed *here* (CLI
    // side) using `hash_scram_password_with_salt` from `crabka-security`
    // so the on-disk record carries the stretched keys, never the plain
    // password.
    for spec in &args.add_scram {
        if spec.iterations < u32::try_from(MIN_SCRAM_ITERATIONS).expect("SCRAM minimum is positive")
        {
            eprintln!(
                "crabka format: iterations must be >= {MIN_SCRAM_ITERATIONS}, got {} for user {}",
                spec.iterations, spec.name,
            );
            return EXIT_LOW_ITERATIONS;
        }
        let mut salt = vec![0u8; 16];
        if let Err(e) = SystemRandom::new().fill(&mut salt) {
            eprintln!("crabka format: rng failure: {e}");
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
        eprintln!("crabka format: checkpoint failed: {e}");
        return EXIT_BOOTSTRAP_FAIL;
    }

    if let Err(e) = write_bootstrap_files(&args.log_dir, cluster_id, &records) {
        eprintln!("crabka format: bootstrap failed: {e}");
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

/// Write the authoritative KIP-630/KIP-853 offset-zero checkpoint for a
/// dynamically formatted controller.
fn write_dynamic_checkpoint(
    log_dir: &std::path::Path,
    cluster_id: ClusterId,
    control_records: &[MetadataRecord],
    metadata_records: &[MetadataRecord],
) -> Result<(), String> {
    let mut image = crabka_metadata::MetadataImage::new(cluster_id.into());
    for record in control_records.iter().chain(metadata_records) {
        image.apply(record);
    }
    let bytes = crabka_raft::serialize_metadata_snapshot(&image, 0)
        .map_err(|e| format!("serialize offset-zero checkpoint: {e}"))?;
    let checkpoint_dir = crabka_raft::kraft::checkpoint_dir(&log_dir.join("__cluster_metadata"));
    std::fs::create_dir_all(&checkpoint_dir)
        .map_err(|e| format!("create checkpoint directory: {e}"))?;
    std::fs::write(checkpoint_dir.join(ZERO_CHECKPOINT_NAME), bytes)
        .map_err(|e| format!("write offset-zero checkpoint: {e}"))
}

/// Serialize the manifest + records to disk under `log_dir`. Returns the
/// first I/O or encoding error encountered.
#[tracing::instrument(
    level = "debug",
    name = "cli.write_bootstrap_files",
    skip_all,
    fields(log_dir = %log_dir.display(), record_count = records.len()),
    err
)]
fn write_bootstrap_files(
    log_dir: &std::path::Path,
    cluster_id: ClusterId,
    records: &[MetadataRecord],
) -> Result<(), String> {
    // 1. Per-record `SerdeCompat<MetadataRecord>` payloads.
    let mut record_blobs: Vec<Vec<u8>> = Vec::with_capacity(records.len());
    for rec in records {
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(rec)
            .map_err(|e| format!("serialize record: {e}"))?;
        record_blobs.push(bytes);
    }

    // 2. Binary stream: length-prefixed (u32 LE) blobs, concatenated.
    let mut bin = Vec::new();
    for blob in &record_blobs {
        let len: u32 = u32::try_from(blob.len())
            .map_err(|_| format!("record too large: {} bytes", blob.len()))?;
        bin.extend_from_slice(&len.to_le_bytes());
        bin.extend_from_slice(blob);
    }
    std::fs::write(log_dir.join("bootstrap.records.bin"), &bin)
        .map_err(|e| format!("write bootstrap.records.bin: {e}"))?;

    // 3. Manifest JSON (cluster id + base64 mirrors of each blob).
    let records_b64: Vec<String> = record_blobs.iter().map(|b| base64_encode(b)).collect();
    let manifest = BootstrapManifest {
        schema: 1,
        cluster_id,
        record_count: records.len(),
        records_b64,
    };
    let json =
        serde_json::to_string_pretty(&manifest).map_err(|e| format!("serialize manifest: {e}"))?;
    std::fs::write(log_dir.join("bootstrap.json"), json)
        .map_err(|e| format!("write bootstrap.json: {e}"))?;

    Ok(())
}

/// Tiny self-contained base64 encoder (standard alphabet, padded). We
/// don't pull in the `base64` crate just for the manifest mirror — the
/// records are only base64'd for human readability; the authoritative
/// copy lives in `bootstrap.records.bin`.
fn base64_encode(input: &[u8]) -> String {
    const ALPHA: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    let mut iter = input.chunks_exact(3);
    for chunk in iter.by_ref() {
        let n = (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2]);
        out.push(ALPHA[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHA[((n >> 12) & 0x3f) as usize] as char);
        out.push(ALPHA[((n >> 6) & 0x3f) as usize] as char);
        out.push(ALPHA[(n & 0x3f) as usize] as char);
    }
    let rem = iter.remainder();
    match rem.len() {
        0 => {}
        1 => {
            let n = u32::from(rem[0]) << 16;
            out.push(ALPHA[((n >> 18) & 0x3f) as usize] as char);
            out.push(ALPHA[((n >> 12) & 0x3f) as usize] as char);
            out.push('=');
            out.push('=');
        }
        2 => {
            let n = (u32::from(rem[0]) << 16) | (u32::from(rem[1]) << 8);
            out.push(ALPHA[((n >> 18) & 0x3f) as usize] as char);
            out.push(ALPHA[((n >> 12) & 0x3f) as usize] as char);
            out.push(ALPHA[((n >> 6) & 0x3f) as usize] as char);
            out.push('=');
        }
        _ => unreachable!("chunks_exact(3) remainder is 0..3"),
    }
    out
}

#[cfg(test)]
mod tests {

    use assert2::check;

    use super::*;

    /// The exit code `run` returns for each argv it can be given.
    ///
    /// Neither `run` nor `run_from_args` had a unit test, so a mutant making
    /// either return a constant survived -- and with them the whole of
    /// `is_dynamic_format` and `build_initial_voters`, whose only visible
    /// effect is which of these codes comes back.
    #[tokio::test]
    async fn exit_code_for_each_argv() {
        const STANDALONE: &[&str] = &[
            "--standalone",
            "--node-id",
            "1",
            "--controller-listener",
            "controller-1:9093",
        ];
        // (what it is, extra argv, expected exit)
        let cases: &[(&str, &[&str], i32)] = &[
            ("static, no flags at all", &[], EXIT_OK),
            ("standalone", STANDALONE, EXIT_OK),
            (
                "no-initial-controllers",
                &["--no-initial-controllers"],
                EXIT_OK,
            ),
            // is_dynamic_format: the kraft.version rules.
            (
                "kraft.version=1 with no quorum flag",
                &["--feature", "kraft.version=1"],
                EXIT_INVALID_FEATURE,
            ),
            (
                "standalone with kraft.version=0",
                &[
                    "--standalone",
                    "--node-id",
                    "1",
                    "--controller-listener",
                    "c:9093",
                    "--feature",
                    "kraft.version=0",
                ],
                EXIT_INVALID_FEATURE,
            ),
            (
                "kraft.version given twice",
                &[
                    "--no-initial-controllers",
                    "--feature",
                    "kraft.version=1",
                    "--feature",
                    "kraft.version=1",
                ],
                EXIT_INVALID_FEATURE,
            ),
            (
                "kraft.version above its range",
                &["--feature", "kraft.version=2"],
                EXIT_INVALID_FEATURE,
            ),
            // build_initial_voters: every way the standalone voter can be wrong.
            (
                "standalone without --node-id",
                &["--standalone", "--controller-listener", "c:9093"],
                EXIT_BOOTSTRAP_FAIL,
            ),
            (
                "standalone without --controller-listener",
                &["--standalone", "--node-id", "1"],
                EXIT_BOOTSTRAP_FAIL,
            ),
            (
                "listener with no port",
                &[
                    "--standalone",
                    "--node-id",
                    "1",
                    "--controller-listener",
                    "hostonly",
                ],
                EXIT_BOOTSTRAP_FAIL,
            ),
            (
                "listener with an empty host",
                &[
                    "--standalone",
                    "--node-id",
                    "1",
                    "--controller-listener",
                    ":9093",
                ],
                EXIT_BOOTSTRAP_FAIL,
            ),
            (
                "listener on port zero",
                &[
                    "--standalone",
                    "--node-id",
                    "1",
                    "--controller-listener",
                    "c:0",
                ],
                EXIT_BOOTSTRAP_FAIL,
            ),
        ];

        for (what, extra, want) in cases {
            let tmp = tempfile::tempdir().expect("tempdir");
            let log_dir = tmp.path().join("data");
            let mut argv = vec![
                "crabka-format".to_owned(),
                "--log-dir".to_owned(),
                log_dir.display().to_string(),
            ];
            argv.extend(extra.iter().map(|a| (*a).to_owned()));
            let got = crate::run_from_args(argv).await;
            check!(got == *want, "{what}: exit {got}, want {want}");
        }
    }

    /// Formatting into a fresh directory, returning its path for inspection.
    async fn format_into(tmp: &std::path::Path, extra: &[&str]) -> (i32, std::path::PathBuf) {
        let log_dir = tmp.join("data");
        let mut argv = vec![
            "crabka-format".to_owned(),
            "--log-dir".to_owned(),
            log_dir.display().to_string(),
        ];
        argv.extend(extra.iter().map(|a| (*a).to_owned()));
        (crate::run_from_args(argv).await, log_dir)
    }

    fn checkpoint_len(log_dir: &std::path::Path) -> u64 {
        let path = crabka_raft::kraft::checkpoint_dir(&log_dir.join("__cluster_metadata"))
            .join(ZERO_CHECKPOINT_NAME);
        std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
    }

    /// Any one of the three quorum flags selects a dynamic format, and their
    /// absence selects the static one. The offset-zero checkpoint is written
    /// only for a dynamic format, so its presence is the observable.
    #[tokio::test]
    async fn each_quorum_flag_on_its_own_selects_a_dynamic_format() {
        const STANDALONE: &[&str] = &[
            "--standalone",
            "--node-id",
            "1",
            "--controller-listener",
            "c:9093",
        ];
        const EXPLICIT: &[&str] = &[
            "--node-id",
            "3",
            "--initial-controllers",
            "3@host:9093:00000000-0000-0000-0000-000000000003",
        ];
        // (what it is, argv, dynamic?)
        let cases: &[(&str, &[&str], bool)] = &[
            ("no quorum flag", &[], false),
            ("--standalone", STANDALONE, true),
            ("--initial-controllers", EXPLICIT, true),
            (
                "--no-initial-controllers",
                &["--no-initial-controllers"],
                true,
            ),
        ];
        for (what, argv, dynamic) in cases {
            let tmp = tempfile::tempdir().expect("tempdir");
            let (code, log_dir) = format_into(tmp.path(), argv).await;
            check!(code == EXIT_OK, "{what}: exit {code}");
            check!(
                (checkpoint_len(&log_dir) > 0) == *dynamic,
                "{what}: checkpoint present should be {dynamic}"
            );
        }
    }

    /// The voter set rides in the checkpoint only when there is one. Both of
    /// these formats are dynamic, so both write a checkpoint -- what separates
    /// them is whether it also carries voters, and the one that does is bigger.
    #[tokio::test]
    async fn the_checkpoint_carries_voters_only_when_the_quorum_has_them() {
        let tmp_with = tempfile::tempdir().expect("tempdir");
        let (code, with_voters) = format_into(
            tmp_with.path(),
            &[
                "--standalone",
                "--node-id",
                "1",
                "--controller-listener",
                "c:9093",
            ],
        )
        .await;
        check!(code == EXIT_OK);

        let tmp_without = tempfile::tempdir().expect("tempdir");
        let (code, without_voters) =
            format_into(tmp_without.path(), &["--no-initial-controllers"]).await;
        check!(code == EXIT_OK);

        let (a, b) = (
            checkpoint_len(&with_voters),
            checkpoint_len(&without_voters),
        );
        check!(
            a > b,
            "checkpoint with voters ({a}) should exceed one without ({b})"
        );
    }

    /// The explicit quorum must name each controller once and must include
    /// this node.
    #[tokio::test]
    async fn an_explicit_quorum_is_checked_for_duplicates_and_for_this_node() {
        const A: &str = "1@host-a:9093:00000000-0000-0000-0000-000000000001";
        const B: &str = "2@host-b:9093:00000000-0000-0000-0000-000000000002";
        // Same id as A on a different host, and same directory id as A on a
        // different node: each is rejected by its own check.
        const DUP_ID: &str = "1@host-c:9093:00000000-0000-0000-0000-00000000000c";
        const DUP_DIR: &str = "3@host-d:9093:00000000-0000-0000-0000-000000000001";

        let cases: &[(&str, &str, &str, i32)] = &[
            ("a well-formed pair", "1", &joined(A, B), EXIT_OK),
            (
                "a repeated node id",
                "1",
                &joined(A, DUP_ID),
                EXIT_BOOTSTRAP_FAIL,
            ),
            (
                "a repeated directory id",
                "1",
                &joined(A, DUP_DIR),
                EXIT_BOOTSTRAP_FAIL,
            ),
            (
                "a quorum without this node",
                "9",
                &joined(A, B),
                EXIT_BOOTSTRAP_FAIL,
            ),
        ];
        for (what, node_id, controllers, want) in cases {
            let tmp = tempfile::tempdir().expect("tempdir");
            let (code, _) = format_into(
                tmp.path(),
                &["--node-id", node_id, "--initial-controllers", controllers],
            )
            .await;
            check!(code == *want, "{what}: exit {code}, want {want}");
        }
    }

    /// `--initial-controllers` takes one comma-separated value.
    fn joined(a: &str, b: &str) -> String {
        format!("{a},{b}")
    }

    /// `--directory-id` is only checked against the quorum entry when it was
    /// given, and only rejected when the two disagree.
    #[tokio::test]
    async fn an_explicit_directory_id_must_match_this_node_s_quorum_entry() {
        const CONTROLLER: &str = "1@host-a:9093:00000000-0000-0000-0000-000000000001";
        // (what it is, --directory-id, expected exit)
        let cases: &[(&str, &str, i32)] = &[
            (
                "matching the quorum entry",
                "00000000-0000-0000-0000-000000000001",
                EXIT_OK,
            ),
            (
                "disagreeing with the quorum entry",
                "00000000-0000-0000-0000-0000000000ff",
                EXIT_BOOTSTRAP_FAIL,
            ),
        ];
        for (what, directory_id, want) in cases {
            let tmp = tempfile::tempdir().expect("tempdir");
            let (code, _) = format_into(
                tmp.path(),
                &[
                    "--node-id",
                    "1",
                    "--initial-controllers",
                    CONTROLLER,
                    "--directory-id",
                    directory_id,
                ],
            )
            .await;
            check!(code == *want, "{what}: exit {code}, want {want}");
        }
    }

    /// The SCRAM iteration floor is inclusive: the minimum itself is allowed
    /// and one below it is not.
    #[tokio::test]
    async fn scram_iterations_are_checked_against_an_inclusive_minimum() {
        let min = u32::try_from(MIN_SCRAM_ITERATIONS).expect("SCRAM minimum is positive");
        for (iterations, want) in [(min, EXIT_OK), (min - 1, EXIT_LOW_ITERATIONS)] {
            let tmp = tempfile::tempdir().expect("tempdir");
            let spec =
                format!("SCRAM-SHA-256=[name=alice,password=hunter2,iterations={iterations}]");
            let (code, _) = format_into(tmp.path(), &["--add-scram", &spec]).await;
            check!(
                code == want,
                "iterations={iterations}: exit {code}, want {want}"
            );
        }
    }

    /// A directory holding anything at all is refused rather than overwritten.
    #[tokio::test]
    async fn a_non_empty_log_dir_is_refused() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let log_dir = tmp.path().join("data");
        std::fs::create_dir_all(&log_dir).expect("mkdir");
        std::fs::write(log_dir.join("someone-elses.txt"), b"x").expect("write");

        let code =
            crate::run_from_args(["crabka-format", "--log-dir", &log_dir.display().to_string()])
                .await;
        check!(code == EXIT_DIRTY_LOG_DIR);
    }

    /// The writers are only observable through the files they leave, so a
    /// mutant emptying one out to `Ok(())` survives until something reads the
    /// directory. A boot needs all of these.
    #[tokio::test]
    async fn a_standalone_format_writes_what_a_boot_reads() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let log_dir = tmp.path().join("data");

        let code = crate::run_from_args([
            "crabka-format",
            "--log-dir",
            &log_dir.display().to_string(),
            "--standalone",
            "--node-id",
            "1",
            "--controller-listener",
            "controller-1:9093",
        ])
        .await;
        check!(code == EXIT_OK);

        for name in [
            "meta.properties.json",
            "bootstrap.records.bin",
            "bootstrap.json",
        ] {
            let path = log_dir.join(name);
            let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            check!(len > 0, "{name} should exist and carry bytes, got {len}");
        }

        // KIP-853 dynamic quorum: the voter set lives in the offset-zero
        // checkpoint, not in the bootstrap record stream.
        let checkpoint = crabka_raft::kraft::checkpoint_dir(&log_dir.join("__cluster_metadata"))
            .join(ZERO_CHECKPOINT_NAME);
        let len = std::fs::metadata(&checkpoint).map(|m| m.len()).unwrap_or(0);
        check!(len > 0, "offset-zero checkpoint should carry the voter set");
    }

    /// A node id is a bare `u64`, and everything else is an error rather than
    /// a silent zero.
    #[test]
    fn parse_node_id_takes_an_integer_and_nothing_else() {
        check!(parse_node_id("7").map(|n| n.0) == Ok(7));
        check!(
            parse_node_id("  7  ").map(|n| n.0) == Ok(7),
            "surrounding space is trimmed"
        );
        for bad in ["", "-1", "1.0", "one", "0x7"] {
            check!(parse_node_id(bad).is_err(), "{bad:?} should not parse");
        }
    }

    /// A level equal to the supported minimum is in range. Every other case
    /// sits strictly inside the range or well outside it, so relaxing the
    /// guard from `<` to `<=` -- which rejects the minimum itself -- changed
    /// nothing any test looked at.
    #[test]
    fn resolve_features_accepts_a_level_at_the_supported_minimum() {
        // group.version supports 0..=1; metadata.version 7..=25.
        check!(resolve_format_features(None, &[("group.version".into(), 0)]).is_ok());
        check!(resolve_format_features(None, &[("metadata.version".into(), 7)]).is_ok());
    }

    #[test]
    fn release_version_maps_to_feature_level() {
        for (input, want) in [
            ("4.0", Some(25)),
            ("3.7-IV4", Some(19)),
            ("2.8", None),     // below MIN / unknown
            ("9.9-IV0", None), // unknown
        ] {
            assert2::assert!(resolve_release_level(input).ok() == want);
        }
    }

    #[test]
    fn bootstrap_seeds_every_nonzero_feature_at_release_default() {
        let bootstrap_mv = crabka_metadata::metadata_version::from_version_string("4.0")
            .unwrap()
            .feature_level();
        // Exercises the exact helper `run()` uses, so it tracks the registry
        // as features are added in later tasks. Features whose release default
        // is 0 are omitted (KIP-1022: level 0 = absent = disabled), matching
        // `kafka-storage format`.
        let records = crabka_metadata::bootstrap_feature_records(bootstrap_mv);
        for feat in crabka_metadata::feature_registry() {
            let found = records.iter().find_map(|r| match r {
                MetadataRecord::V1FeatureLevel(f) if f.name == feat.name() => Some(f.level),
                _ => None,
            });
            let expected = feat.default_level(bootstrap_mv);
            if expected > 0 {
                assert2::assert!(found == Some(expected));
            } else {
                assert2::assert!(found.is_none());
            }
        }
    }

    // The no-flag default path (`Ok(None) => METADATA_VERSION_MAX` in `run()`)
    // is covered end-to-end by `format_smoke.rs`, which formats without
    // `--release-version` and asserts the FeatureLevel record is present.
    #[test]
    fn max_version_string_resolves_to_max() {
        assert2::assert!(
            resolve_release_level("4.0").unwrap()
                == crabka_metadata::metadata_version::METADATA_VERSION_MAX
        );
    }

    #[test]
    fn parse_feature_spec_happy_path() {
        assert2::assert!(
            parse_feature_spec("group.version=1").unwrap() == ("group.version".to_string(), 1)
        );
        assert2::assert!(
            parse_feature_spec("metadata.version=20").unwrap()
                == ("metadata.version".to_string(), 20)
        );
    }

    #[test]
    fn parse_feature_spec_error_branches() {
        for bad in [
            "noequals",          // missing '='
            "group.version=abc", // non-integer level
            "group.version=",    // empty level
            "=1",                // empty name
        ] {
            assert2::assert!(parse_feature_spec(bad).is_err());
        }
    }

    #[test]
    fn resolve_features_defaults_bootstrap_mv_to_max() {
        // No --release-version, no metadata.version override → bootstrap at MAX;
        // an explicit non-metadata feature becomes an override.
        let (mv, ov) =
            resolve_format_features(None, &[("group.version".into(), 1)]).expect("resolve");
        assert2::assert!(mv == crabka_metadata::metadata_version::METADATA_VERSION_MAX);
        assert2::assert!(ov.get("group.version") == Some(&1));
    }

    #[test]
    fn resolve_features_metadata_version_feature_sets_bootstrap_mv() {
        let (mv, ov) =
            resolve_format_features(None, &[("metadata.version".into(), 20)]).expect("resolve");
        assert2::assert!(mv == 20);
        assert2::assert!(ov.get("metadata.version") == Some(&20));
    }

    #[test]
    fn resolve_features_release_version_sets_bootstrap_mv() {
        let (mv, ov) = resolve_format_features(Some("4.0-IV0"), &[]).expect("resolve");
        assert2::assert!(mv == 22);
        assert2::assert!(ov.is_empty());
    }

    #[test]
    fn resolve_features_release_and_feature_combine() {
        // --release-version sets the base; a non-metadata --feature overrides it.
        let (mv, ov) =
            resolve_format_features(Some("4.0-IV0"), &[("transaction.version".into(), 2)])
                .expect("resolve");
        assert2::assert!(mv == 22);
        assert2::assert!(ov.get("transaction.version") == Some(&2));
    }

    #[test]
    fn resolve_features_rejects_release_plus_metadata_version_feature() {
        // Ambiguity: both --release-version and --feature metadata.version set MV.
        let err = resolve_format_features(Some("4.0-IV0"), &[("metadata.version".into(), 24)])
            .unwrap_err();
        assert2::assert!(err.contains("metadata.version"));
    }

    #[test]
    fn resolve_features_rejects_unknown_feature() {
        let err = resolve_format_features(None, &[("bogus.version".into(), 1)]).unwrap_err();
        assert2::assert!(err.contains("Unsupported feature"));
        assert2::assert!(err.contains("bogus.version"));
    }

    #[test]
    fn resolve_features_rejects_out_of_range_level() {
        for (name, level) in [
            ("group.version", 5),     // group.version supports 0..=1
            ("metadata.version", 99), // metadata.version supports 7..=25
            ("metadata.version", 1),
        ] {
            assert2::assert!(resolve_format_features(None, &[(name.into(), level)]).is_err());
        }
    }

    #[test]
    fn resolve_features_rejects_bad_release_string() {
        assert2::assert!(resolve_format_features(Some("2.8"), &[]).is_err());
    }

    #[test]
    fn parse_scram_spec_happy_path() {
        let spec = parse_scram_spec("SCRAM-SHA-512=[name=alice,password=hunter2,iterations=8192]")
            .unwrap();
        assert2::assert!(
            spec == ScramSpec {
                mechanism: SaslMechanism::ScramSha512,
                name: "alice".to_string(),
                password: "hunter2".to_string(),
                iterations: 8192,
            }
        );
    }

    #[test]
    fn parse_scram_spec_iterations_default() {
        let spec = parse_scram_spec("SCRAM-SHA-512=[name=bob,password=p]").unwrap();
        assert2::assert!(spec.iterations == 4096);
    }

    #[test]
    fn parse_scram_spec_sha256_prefix() {
        let spec = parse_scram_spec("SCRAM-SHA-256=[name=alice,password=hunter2,iterations=8192]")
            .unwrap();
        assert2::assert!(spec.name.as_str() == "alice");
        assert2::assert!(spec.mechanism == SaslMechanism::ScramSha256);
    }

    #[test]
    fn parse_scram_spec_rejects_missing_prefix() {
        assert2::assert!(parse_scram_spec("PLAIN=[name=a,password=b]").is_err());
    }

    #[test]
    fn parse_scram_spec_rejects_missing_name() {
        assert2::assert!(parse_scram_spec("SCRAM-SHA-512=[password=p,iterations=4096]").is_err());
    }

    #[test]
    fn parse_scram_spec_rejects_unknown_attr() {
        assert2::assert!(parse_scram_spec("SCRAM-SHA-512=[name=a,password=b,foo=bar]").is_err());
    }

    #[test]
    fn parse_acl_spec_minimal() {
        let s = "principal=User:admin,host=*,operation=All,permission=Allow,resource=Cluster:kafka-cluster";
        let entry = parse_acl_spec(s).unwrap();
        assert2::assert!(
            entry
                == AclEntry {
                    resource_type: crabka_metadata::ResourceType::Cluster,
                    resource_name: "kafka-cluster".to_string(),
                    pattern_type: crabka_metadata::PatternType::Literal,
                    principal: "User:admin".to_string(),
                    host: "*".to_string(),
                    operation: crabka_metadata::AclOperation::All,
                    permission_type: crabka_metadata::PermissionType::Allow,
                }
        );
    }

    #[test]
    fn parse_acl_spec_with_prefixed_pattern() {
        let s = "principal=User:alice,host=*,operation=Read,permission=Allow,resource=Topic:team-:Prefixed";
        let entry = parse_acl_spec(s).unwrap();
        assert2::assert!(entry.pattern_type == crabka_metadata::PatternType::Prefixed);
        assert2::assert!(entry.resource_name.as_str() == "team-");
    }

    #[test]
    fn parse_acl_spec_unknown_key_errors() {
        let s = "principal=User:admin,host=*,bogus=x";
        assert2::assert!(parse_acl_spec(s).is_err());
    }

    #[test]
    fn parses_initial_controller_spec() {
        let v =
            parse_initial_controller("3@host:9093:00000000-0000-0000-0000-000000000003").unwrap();
        assert2::assert!(
            v == Voter {
                id: crabka_metadata::NodeId(3),
                directory_id: Uuid::from_u128(3),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".to_string(),
                    host: "host".to_string(),
                    port: 9093,
                }],
                kraft_version: KRaftVersionRange { min: 0, max: 1 },
            }
        );
    }

    #[test]
    fn rejects_initial_controller_without_at() {
        assert2::assert!(parse_initial_controller("3:host:9093:uuid").is_err());
    }

    #[test]
    fn rejects_initial_controller_bad_uuid() {
        assert2::assert!(parse_initial_controller("3@host:9093:not-a-uuid").is_err());
    }

    #[test]
    fn parse_acl_spec_all_operations() {
        use crabka_metadata::AclOperation;
        for (s, op) in [
            ("All", AclOperation::All),
            ("Read", AclOperation::Read),
            ("Write", AclOperation::Write),
            ("Create", AclOperation::Create),
            ("Delete", AclOperation::Delete),
            ("Alter", AclOperation::Alter),
            ("Describe", AclOperation::Describe),
            ("ClusterAction", AclOperation::ClusterAction),
            ("DescribeConfigs", AclOperation::DescribeConfigs),
            ("AlterConfigs", AclOperation::AlterConfigs),
            ("IdempotentWrite", AclOperation::IdempotentWrite),
            ("TwoPhaseCommit", AclOperation::TwoPhaseCommit),
        ] {
            let spec =
                format!("principal=User:u,host=*,operation={s},permission=Allow,resource=Topic:t");
            assert2::assert!(parse_acl_spec(&spec).unwrap().operation == op);
        }
    }

    #[test]
    fn parse_acl_spec_all_resource_types_and_deny() {
        use crabka_metadata::{PermissionType, ResourceType};
        for (s, rt) in [
            ("Topic", ResourceType::Topic),
            ("Group", ResourceType::Group),
            ("Cluster", ResourceType::Cluster),
            ("TransactionalId", ResourceType::TransactionalId),
        ] {
            let spec =
                format!("principal=User:u,host=*,operation=All,permission=Deny,resource={s}:n");
            let entry = parse_acl_spec(&spec).unwrap();
            assert2::assert!(entry.resource_type == rt);
            assert2::assert!(entry.permission_type == PermissionType::Deny);
        }
    }

    #[test]
    fn parse_acl_spec_error_branches() {
        for bad in [
            "principal=User:u,host=*,operation=Bogus,permission=Allow,resource=Topic:t",
            "principal=User:u,host=*,operation=All,permission=Maybe,resource=Topic:t",
            "principal=User:u,host=*,operation=All,permission=Allow,resource=Topic:t:Weird",
            "principal=User:u,host=*,operation=All,permission=Allow,resource=Bogus:t",
            "principal=User:u,host=*,operation=All,permission=Allow,resource=Topic",
            "malformedpair",
            "host=*,operation=All,permission=Allow,resource=Topic:t",
            "principal=User:u,operation=All,permission=Allow,resource=Topic:t",
            "principal=User:u,host=*,permission=Allow,resource=Topic:t",
            "principal=User:u,host=*,operation=All,resource=Topic:t",
            "principal=User:u,host=*,operation=All,permission=Allow",
        ] {
            assert2::assert!(parse_acl_spec(bad).is_err());
        }
    }

    #[test]
    fn parse_scram_spec_error_branches() {
        for bad in [
            "SCRAM-SHA-512=[name=a,password=b", // missing closing ]
            "SCRAM-SHA-512=[name=a,password=b,iterations=xx]", // bad iterations
            "SCRAM-SHA-512=[name=a,badattr]",   // malformed attr (no '=')
            "SCRAM-SHA-512=[name=a,iterations=4096]", // missing password
        ] {
            assert2::assert!(parse_scram_spec(bad).is_err());
        }
    }

    #[test]
    fn parse_initial_controller_error_branches() {
        for bad in [
            "notanum@host:9093:00000000-0000-0000-0000-000000000003", // bad id
            "3@host9093",                                             // missing directory uuid
            "3@host:notaport:00000000-0000-0000-0000-000000000003",   // bad port
            "3@hostonly:00000000-0000-0000-0000-000000000003",        // missing host:port
        ] {
            assert2::assert!(parse_initial_controller(bad).is_err());
        }
    }

    #[test]
    fn base64_encode_known_vectors() {
        // RFC 4648 §10
        for (input, want) in [
            (b"".as_slice(), ""),
            (b"f".as_slice(), "Zg=="),
            (b"fo".as_slice(), "Zm8="),
            (b"foo".as_slice(), "Zm9v"),
            (b"foob".as_slice(), "Zm9vYg=="),
            (b"fooba".as_slice(), "Zm9vYmE="),
            (b"foobar".as_slice(), "Zm9vYmFy"),
        ] {
            assert2::assert!(base64_encode(input) == want);
        }
    }
}
