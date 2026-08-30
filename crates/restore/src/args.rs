//! The `krabka restore` command line and the parsers for its compound values.
//!
//! Every flag is long-form. The target-side flags carry the same names as
//! `krabka format`, because a restore formats the cluster it writes into and an
//! operator must not have to learn two spellings for one concept.
//!
//! The flags are grouped into three flattened structs, one per stage of the
//! command: where the archive is, what the target cluster is, and what the
//! restore keeps. `#[command(flatten)]` keeps every flag top-level, so the
//! grouping shows up in `--help` as headings and nowhere else.
//!
//! This file holds the clap definition itself. The values those flags take, and
//! the parsers clap calls to build them, live in `value` and `timestamp`, and
//! the cross-flag checks clap cannot express live in `validate`.

use std::path::PathBuf;

use clap::{ArgGroup, Args};
use krabka_ids::ProducerId;
use krabka_metadata::NodeId;
use regex::Regex;
use uuid::Uuid;

use self::{
    timestamp::parse_timestamp,
    value::{
        parse_header_pattern, parse_node_id, parse_offset_bound, parse_offset_range,
        parse_producer_id, parse_regex, parse_topic_name,
    },
};
use crate::report::ReportFormat;

mod timestamp;
mod validate;
mod value;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub use self::value::{HeaderPattern, OffsetBound, OffsetRange, PartitionRef};

/// Where the archive is.
///
/// Exactly one backend is selected. The sub-flags of a backend are checked by
/// [`RestoreArgs::validate`], not by clap: a mutually exclusive `ArgGroup`
/// makes clap's `requires` unenforceable, because clap treats a required
/// argument as acceptably absent when it conflicts with one that is present.
#[derive(Args, Debug)]
#[command(next_help_heading = "Archive source")]
#[command(group(
    ArgGroup::new("archive_source")
        .required(true)
        .args(["local", "s3_bucket", "gcs_bucket"]),
))]
pub struct ArchiveArgs {
    /// Read the archive from a local directory tree.
    #[arg(long = "archive-local", value_name = "DIR")]
    pub local: Option<PathBuf>,

    /// Read the archive from this S3 or S3-compatible bucket.
    #[arg(long = "archive-s3-bucket", value_name = "BUCKET")]
    pub s3_bucket: Option<String>,

    /// S3 region. Defaults to `us-east-1`, which `MinIO` and R2 accept as a
    /// placeholder.
    #[arg(long = "archive-s3-region", value_name = "REGION")]
    pub s3_region: Option<String>,

    /// S3 endpoint URL, for a non-AWS S3-compatible store.
    #[arg(long = "archive-s3-endpoint", value_name = "URL")]
    pub s3_endpoint: Option<String>,

    /// S3 access key id. Without it the AWS credential chain applies.
    #[arg(long = "archive-s3-access-key-id", value_name = "ID")]
    pub s3_access_key_id: Option<String>,

    /// S3 secret access key. Without it the AWS credential chain applies.
    #[arg(long = "archive-s3-secret-access-key", value_name = "SECRET")]
    pub s3_secret_access_key: Option<String>,

    /// Allow plaintext HTTP to the S3 endpoint.
    #[arg(long = "archive-s3-allow-http")]
    pub s3_allow_http: bool,

    /// Read the archive from this Google Cloud Storage bucket.
    #[arg(long = "archive-gcs-bucket", value_name = "BUCKET")]
    pub gcs_bucket: Option<String>,

    /// Path to a GCS service-account JSON key. Without it Workload Identity or
    /// application default credentials apply.
    #[arg(long = "archive-gcs-service-account-path", value_name = "PATH")]
    pub gcs_service_account_path: Option<String>,

    /// GCS API base URL, for an emulator.
    #[arg(long = "archive-gcs-endpoint", value_name = "URL")]
    pub gcs_endpoint: Option<String>,

    /// Allow plaintext HTTP to the GCS endpoint.
    #[arg(long = "archive-gcs-allow-http")]
    pub gcs_allow_http: bool,

    /// Key prefix inside the archive, for a bucket that holds more than the
    /// tiered-storage tree. It applies to every backend.
    #[arg(long = "archive-prefix", value_name = "PREFIX")]
    pub prefix: Option<String>,

    /// A broker's `<log.dir>/remote-log-metadata/snapshot`.
    ///
    /// The snapshot is authoritative about segment lifecycle state. Without it
    /// a segment the old cluster had marked for deletion is indistinguishable
    /// from a live one.
    #[arg(long, value_name = "PATH")]
    pub rlmm_snapshot: Option<PathBuf>,

    /// A controller `<offset>-<epoch>.checkpoint` metadata snapshot.
    ///
    /// Topic configuration, ACLs, client quotas, SCRAM credentials, and
    /// finalized feature levels are recovered from it.
    #[arg(long, value_name = "PATH")]
    pub metadata_snapshot: Option<PathBuf>,
}

/// The cluster the restore writes.
///
/// Every flag here carries the name `krabka format` gives it, and is forwarded
/// to the formatter unchanged.
#[derive(Args, Debug)]
#[command(next_help_heading = "Target cluster")]
pub struct TargetArgs {
    /// Directory to restore into. Must be empty or absent.
    #[arg(long, value_name = "DIR")]
    pub log_dir: PathBuf,

    /// Cluster id of the restored cluster. Generated if not provided.
    #[arg(long)]
    pub cluster_id: Option<Uuid>,

    /// This node's raft id. Required with `--standalone` and
    /// `--initial-controllers`.
    #[arg(long, value_parser = parse_node_id)]
    pub node_id: Option<NodeId>,

    /// Format the restored node as the sole initial controller voter.
    #[arg(long, conflicts_with_all = ["initial_controllers", "no_initial_controllers"])]
    pub standalone: bool,

    /// Explicit initial controllers: `id@host:port:directory-id`,
    /// comma-separated.
    #[arg(
        long,
        value_delimiter = ',',
        conflicts_with_all = ["standalone", "no_initial_controllers"]
    )]
    pub initial_controllers: Vec<String>,

    /// Format a dynamic controller that will join an existing quorum.
    #[arg(long, conflicts_with_all = ["standalone", "initial_controllers"])]
    pub no_initial_controllers: bool,

    /// This node's controller listener, as `host:port`.
    #[arg(long, value_name = "HOST:PORT")]
    pub controller_listener: Option<String>,
}

/// Arguments of an offline point-in-time restore.
///
/// The fields are public so a test or an embedding tool can build the struct
/// directly. A `clap::Args` struct with private fields is reachable only
/// through an argv, which forces every caller through string formatting.
#[derive(Args, Debug)]
pub struct RestoreArgs {
    /// Where the archive is.
    #[command(flatten)]
    pub archive: ArchiveArgs,

    /// The cluster the restore writes.
    #[command(flatten)]
    pub target: TargetArgs,

    /// Restore this topic. May be repeated. Every topic the archive holds is
    /// restored when the flag is absent.
    #[arg(
        long = "topic",
        value_name = "NAME",
        value_parser = parse_topic_name,
        help_heading = HEADING_BOUNDS
    )]
    pub topic: Vec<String>,

    /// Keep offsets at or below `N` in one partition: `topic:partition=N`.
    /// May be repeated.
    #[arg(
        long,
        value_name = "TOPIC:PARTITION=N",
        value_parser = parse_offset_bound,
        help_heading = HEADING_BOUNDS
    )]
    pub to_offset: Vec<OffsetBound>,

    /// Keep records whose timestamp is below this instant. Accepts RFC 3339
    /// with an explicit zone, or bare epoch milliseconds.
    #[arg(
        long,
        value_name = "RFC3339|EPOCH_MS",
        value_parser = parse_timestamp,
        help_heading = HEADING_BOUNDS
    )]
    pub to_timestamp: Option<i64>,

    /// Drop records whose key matches this pattern. May be repeated.
    #[arg(
        long,
        value_name = "REGEX",
        value_parser = parse_regex,
        help_heading = HEADING_BOUNDS
    )]
    pub exclude_key: Vec<Regex>,

    /// Drop records that carry a header matching `NAME=REGEX`. May be
    /// repeated.
    #[arg(
        long,
        value_name = "NAME=REGEX",
        value_parser = parse_header_pattern,
        help_heading = HEADING_BOUNDS
    )]
    pub exclude_header: Vec<HeaderPattern>,

    /// Drop records written by this producer id. May be repeated.
    #[arg(
        long,
        value_name = "ID",
        value_parser = parse_producer_id,
        help_heading = HEADING_BOUNDS
    )]
    pub exclude_producer_id: Vec<ProducerId>,

    /// Drop an offset range in one partition: `topic:partition=A..B`, with `B`
    /// exclusive. Write `A..=B` to include `B`. May be repeated.
    #[arg(
        long,
        value_name = "TOPIC:PARTITION=A..B",
        value_parser = parse_offset_range,
        help_heading = HEADING_BOUNDS
    )]
    pub exclude_offset: Vec<OffsetRange>,

    /// Verify, format cluster metadata, and report without writing partition data.
    #[arg(long, help_heading = HEADING_BEHAVIOUR)]
    pub dry_run: bool,

    /// Report format.
    #[arg(long, value_enum, default_value = "text", help_heading = HEADING_BEHAVIOUR)]
    pub report: ReportFormat,

    /// Skip a segment that fails verification instead of stopping. The report
    /// names every segment that was skipped.
    #[arg(long, help_heading = HEADING_BEHAVIOUR)]
    pub continue_on_corrupt: bool,
}

/// Help headings for the flags that are not in a flattened group.
const HEADING_BOUNDS: &str = "Selection and bounds";
const HEADING_BEHAVIOUR: &str = "Behaviour";

impl RestoreArgs {
    /// Whether `topic` is in the restore set.
    ///
    /// An empty `--topic` list selects every topic the archive holds.
    #[must_use]
    pub fn selects_topic(&self, topic: &str) -> bool {
        self.topic.is_empty() || self.topic.iter().any(|selected| selected == topic)
    }
}
