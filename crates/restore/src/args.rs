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

use std::{fmt, path::PathBuf};

use clap::{ArgGroup, Args};
use krabka_ids::{Offset, ProducerId};
use krabka_metadata::NodeId;
use regex::Regex;
use uuid::Uuid;

use crate::{error::RestoreError, report::ReportFormat};

/// Kafka's limit on a topic name, which a bound may not exceed either.
const MAX_TOPIC_NAME_LEN: usize = 249;

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

    /// Reject a sub-flag of a backend that was not selected.
    fn validate_backend_flags(&self) -> Result<(), RestoreError> {
        let archive = &self.archive;
        let s3 = [
            ("--archive-s3-region", archive.s3_region.is_some()),
            ("--archive-s3-endpoint", archive.s3_endpoint.is_some()),
            (
                "--archive-s3-access-key-id",
                archive.s3_access_key_id.is_some(),
            ),
            (
                "--archive-s3-secret-access-key",
                archive.s3_secret_access_key.is_some(),
            ),
            ("--archive-s3-allow-http", archive.s3_allow_http),
        ];
        let gcs = [
            (
                "--archive-gcs-service-account-path",
                archive.gcs_service_account_path.is_some(),
            ),
            ("--archive-gcs-endpoint", archive.gcs_endpoint.is_some()),
            ("--archive-gcs-allow-http", archive.gcs_allow_http),
        ];
        for (flags, selected, needs) in [
            (&s3[..], archive.s3_bucket.is_some(), "--archive-s3-bucket"),
            (
                &gcs[..],
                archive.gcs_bucket.is_some(),
                "--archive-gcs-bucket",
            ),
        ] {
            if selected {
                continue;
            }
            if let Some((flag, _)) = flags.iter().find(|(_, given)| *given) {
                return Err(RestoreError::InvalidArgument(format!(
                    "{flag} needs {needs}"
                )));
            }
        }
        Ok(())
    }

    /// Check the flag combinations clap cannot express.
    ///
    /// # Errors
    ///
    /// Returns [`RestoreError::InvalidArgument`] when a backend sub-flag names
    /// a backend that `--archive-*` did not select, when one partition carries
    /// two `--to-offset` bounds, or when a bound names a topic that `--topic`
    /// excludes. Each means the operator wrote a flag that can never apply.
    pub fn validate(&self) -> Result<(), RestoreError> {
        self.validate_backend_flags()?;

        let mut bounded: Vec<&PartitionRef> = Vec::with_capacity(self.to_offset.len());
        for bound in &self.to_offset {
            if bounded.contains(&&bound.partition) {
                return Err(RestoreError::InvalidArgument(format!(
                    "--to-offset names {} more than once",
                    bound.partition
                )));
            }
            bounded.push(&bound.partition);
        }

        for partition in self
            .to_offset
            .iter()
            .map(|bound| &bound.partition)
            .chain(self.exclude_offset.iter().map(|range| &range.partition))
        {
            if !self.selects_topic(&partition.topic) {
                return Err(RestoreError::InvalidArgument(format!(
                    "a bound names {partition}, which --topic does not select"
                )));
            }
        }
        Ok(())
    }
}

/// A topic partition an operator names in a bound.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionRef {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
}

impl fmt::Display for PartitionRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

/// One `--to-offset` bound: the last offset the restore keeps in a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetBound {
    /// The partition the bound applies to.
    pub partition: PartitionRef,
    /// The highest offset the restore keeps. It is inclusive.
    pub last_offset: Offset,
}

/// One `--exclude-offset` range, normalized to a half-open interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetRange {
    /// The partition the range applies to.
    pub partition: PartitionRef,
    /// First excluded offset.
    pub start: Offset,
    /// First offset past the range. It is not excluded.
    pub end_exclusive: Offset,
}

/// One `--exclude-header` pattern.
#[derive(Debug, Clone)]
pub struct HeaderPattern {
    /// The header name, matched byte for byte.
    pub name: String,
    /// The pattern the header value must match for the record to be dropped.
    pub pattern: Regex,
}

impl PartialEq for HeaderPattern {
    /// Two patterns are equal when they name the same header and hold the same
    /// source pattern. `Regex` has no equality of its own, and the compiled
    /// program is a function of the source.
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.pattern.as_str() == other.pattern.as_str()
    }
}

impl Eq for HeaderPattern {}

/// Parse a node id: a bare `u64` in the [`NodeId`] newtype.
fn parse_node_id(s: &str) -> Result<NodeId, String> {
    let id: u64 = s
        .trim()
        .parse()
        .map_err(|error| format!("node id: {error}"))?;
    Ok(NodeId(id))
}

/// Parse a producer id. A negative value is the "no producer" sentinel and
/// never identifies a writer, so it cannot be excluded.
fn parse_producer_id(s: &str) -> Result<ProducerId, String> {
    let id: i64 = s
        .trim()
        .parse()
        .map_err(|error| format!("producer id: {error}"))?;
    if id < 0 {
        return Err(format!("producer id must not be negative, got {id}"));
    }
    Ok(ProducerId(id))
}

/// Compile one operator-supplied pattern.
fn parse_regex(s: &str) -> Result<Regex, String> {
    Regex::new(s).map_err(|error| format!("pattern {s:?}: {error}"))
}

/// Check a Kafka topic name: it is not empty, it is at most 249 characters, it
/// is neither `.` nor `..`, and it holds only `[a-zA-Z0-9._-]`.
fn parse_topic_name(s: &str) -> Result<String, String> {
    if s.is_empty() {
        return Err("topic name must not be empty".into());
    }
    if s.len() > MAX_TOPIC_NAME_LEN {
        return Err(format!(
            "topic name must be at most {MAX_TOPIC_NAME_LEN} characters, got {}",
            s.len()
        ));
    }
    if s == "." || s == ".." {
        return Err(format!("topic name must not be {s:?}"));
    }
    if let Some(bad) = s
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        return Err(format!(
            "topic name may hold only [a-zA-Z0-9._-], found {bad:?}"
        ));
    }
    Ok(s.to_owned())
}

/// Parse the `topic:partition` half of a bound.
fn parse_partition_ref(s: &str) -> Result<PartitionRef, String> {
    let (topic, partition) = s
        .split_once(':')
        .ok_or_else(|| format!("expected topic:partition, got {s:?}"))?;
    let topic = parse_topic_name(topic)?;
    let partition: i32 = partition
        .parse()
        .map_err(|error| format!("partition index: {error}"))?;
    if partition < 0 {
        return Err(format!(
            "partition index must not be negative, got {partition}"
        ));
    }
    Ok(PartitionRef { topic, partition })
}

/// Parse one `--to-offset topic:partition=N`.
fn parse_offset_bound(s: &str) -> Result<OffsetBound, String> {
    let (partition, last) = s
        .split_once('=')
        .ok_or_else(|| format!("expected topic:partition=N, got {s:?}"))?;
    let partition = parse_partition_ref(partition)?;
    let last_offset: i64 = last
        .parse()
        .map_err(|error| format!("offset bound: {error}"))?;
    if last_offset < 0 {
        return Err(format!(
            "offset bound must not be negative, got {last_offset}"
        ));
    }
    Ok(OffsetBound {
        partition,
        last_offset: Offset(last_offset),
    })
}

/// Parse one `--exclude-offset topic:partition=A..B` or `topic:partition=A..=B`.
fn parse_offset_range(s: &str) -> Result<OffsetRange, String> {
    let (partition, range) = s
        .split_once('=')
        .ok_or_else(|| format!("expected topic:partition=A..B, got {s:?}"))?;
    let partition = parse_partition_ref(partition)?;
    let (start, end, end_included) = if let Some((start, end)) = range.split_once("..=") {
        (start, end, true)
    } else {
        let (start, end) = range
            .split_once("..")
            .ok_or_else(|| format!("expected an offset range A..B, got {range:?}"))?;
        (start, end, false)
    };
    let start: i64 = start
        .parse()
        .map_err(|error| format!("range start: {error}"))?;
    let end: i64 = end.parse().map_err(|error| format!("range end: {error}"))?;
    if start < 0 || end < 0 {
        return Err(format!(
            "offset range must not be negative, got {start}..{end}"
        ));
    }
    let end_exclusive = if end_included {
        end.checked_add(1)
            .ok_or_else(|| format!("range end {end} overflows when made exclusive"))?
    } else {
        end
    };
    if start >= end_exclusive {
        return Err(format!(
            "offset range must exclude at least one offset, got {range:?}"
        ));
    }
    Ok(OffsetRange {
        partition,
        start: Offset(start),
        end_exclusive: Offset(end_exclusive),
    })
}

/// Parse one `--exclude-header NAME=REGEX`.
///
/// The split is on the first `=`, so a pattern may hold `=` and a header name
/// may not.
fn parse_header_pattern(s: &str) -> Result<HeaderPattern, String> {
    let (name, pattern) = s
        .split_once('=')
        .ok_or_else(|| format!("expected NAME=REGEX, got {s:?}"))?;
    if name.is_empty() {
        return Err("header name must not be empty".into());
    }
    Ok(HeaderPattern {
        name: name.to_owned(),
        pattern: parse_regex(pattern)?,
    })
}

/// Parse `--to-timestamp` into epoch milliseconds.
///
/// A value of only digits, with an optional sign, is epoch milliseconds. Every
/// other value is RFC 3339.
fn parse_timestamp(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("timestamp must not be empty".into());
    }
    if is_signed_integer(s) {
        return s
            .parse()
            .map_err(|error| format!("epoch milliseconds: {error}"));
    }
    parse_rfc3339_millis(s)
}

fn is_signed_integer(s: &str) -> bool {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// Convert an RFC 3339 instant to epoch milliseconds, truncating any precision
/// below a millisecond.
///
/// The zone designator is required, as RFC 3339 requires it. A restore bound
/// with an implied zone is a bound nobody can check.
fn parse_rfc3339_millis(s: &str) -> Result<i64, String> {
    let (date, rest) = s
        .split_at_checked(10)
        .ok_or_else(|| format!("expected an RFC 3339 timestamp, got {s:?}"))?;
    let (separator, rest) = rest
        .split_at_checked(1)
        .ok_or_else(|| format!("expected a time after the date in {s:?}"))?;
    if !matches!(separator, "T" | "t" | " ") {
        return Err(format!(
            "expected T between the date and the time, got {separator:?}"
        ));
    }
    let (year, month, day) = parse_date(date)?;
    let (time, offset_seconds) = split_zone(rest)?;
    let (hour, minute, second, millis) = parse_time(time)?;

    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offset_seconds;
    Ok(seconds * 1_000 + millis)
}

fn parse_date(date: &str) -> Result<(i64, i64, i64), String> {
    let bytes = date.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(format!("expected a YYYY-MM-DD date, got {date:?}"));
    }
    let year = parse_digits(&date[0..4], "year")?;
    let month = parse_digits(&date[5..7], "month")?;
    let day = parse_digits(&date[8..10], "day")?;
    if year == 0 {
        return Err("year must be 0001 or later".into());
    }
    if !(1..=12).contains(&month) {
        return Err(format!("month must be 01..12, got {month:02}"));
    }
    let last = days_in_month(year, month);
    if !(1..=last).contains(&day) {
        return Err(format!(
            "day must be 01..{last:02} for {year:04}-{month:02}, got {day:02}"
        ));
    }
    Ok((year, month, day))
}

/// Split the zone designator off the end and return the offset in seconds.
fn split_zone(rest: &str) -> Result<(&str, i64), String> {
    if let Some(time) = rest.strip_suffix(['Z', 'z']) {
        return Ok((time, 0));
    }
    let split = rest
        .len()
        .checked_sub(6)
        .and_then(|at| rest.split_at_checked(at));
    let (time, zone) = split.ok_or_else(|| {
        format!("expected a zone designator, Z or +HH:MM or -HH:MM, at the end of {rest:?}")
    })?;
    let bytes = zone.as_bytes();
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => {
            return Err(format!(
                "expected a zone designator, Z or +HH:MM or -HH:MM, got {zone:?}"
            ));
        }
    };
    if bytes[3] != b':' {
        return Err(format!("expected a zone offset of +HH:MM, got {zone:?}"));
    }
    let hours = parse_digits(&zone[1..3], "zone hours")?;
    let minutes = parse_digits(&zone[4..6], "zone minutes")?;
    if hours > 23 || minutes > 59 {
        return Err(format!("zone offset out of range: {zone:?}"));
    }
    Ok((time, sign * (hours * 3_600 + minutes * 60)))
}

fn parse_time(time: &str) -> Result<(i64, i64, i64, i64), String> {
    let (hms, fraction) = match time.split_once('.') {
        Some((hms, fraction)) => (hms, Some(fraction)),
        None => (time, None),
    };
    let bytes = hms.as_bytes();
    if bytes.len() != 8 || bytes[2] != b':' || bytes[5] != b':' {
        return Err(format!("expected a HH:MM:SS time, got {hms:?}"));
    }
    let hour = parse_digits(&hms[0..2], "hour")?;
    let minute = parse_digits(&hms[3..5], "minute")?;
    let second = parse_digits(&hms[6..8], "second")?;
    if hour > 23 {
        return Err(format!("hour must be 00..23, got {hour:02}"));
    }
    if minute > 59 {
        return Err(format!("minute must be 00..59, got {minute:02}"));
    }
    if second > 59 {
        return Err(format!("second must be 00..59, got {second:02}"));
    }
    let millis = match fraction {
        None => 0,
        Some(fraction) => {
            if fraction.is_empty() || fraction.len() > 9 {
                return Err(format!(
                    "fractional second must be 1 to 9 digits, got {fraction:?}"
                ));
            }
            let mut padded = String::from(fraction);
            padded.push_str("000");
            parse_digits(&padded[0..3], "fractional second")?
        }
    };
    Ok((hour, minute, second, millis))
}

fn parse_digits(s: &str, field: &str) -> Result<i64, String> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("{field} must be digits, got {s:?}"));
    }
    s.parse().map_err(|error| format!("{field}: {error}"))
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        _ => 28,
    }
}

/// Days between 1970-01-01 and the given civil date, by Howard Hinnant's
/// `days_from_civil`. The era arithmetic holds for every year this parser
/// accepts, so no calendar table is needed.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let shifted = if month <= 2 { year - 1 } else { year };
    let era = if shifted >= 0 { shifted } else { shifted - 399 } / 400;
    let year_of_era = shifted - era * 400;
    let month_from_march = (month + 9) % 12;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests;
