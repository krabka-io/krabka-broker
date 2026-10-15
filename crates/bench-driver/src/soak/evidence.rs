//! What a soak run records, in the form the verdict is computed from.
//!
//! The harness writes one `evidence.json` at the end of a run, and appends
//! each fault to `faults.jsonl` as it is injected and cleared, so a run that
//! dies mid-way still leaves its fault timeline behind. Every timestamp is
//! wall-clock Unix milliseconds, the coordinate system the broker's own logs
//! and metrics use, so the fault record lines up with them.
//!
//! Records are identified by `(producer, seq)`: each producer numbers what it
//! sends from zero and writes the number into the record value, so a reader can
//! say which send a record came from without trusting offsets that a failover
//! may have reassigned.

use {
    super::{
        contract::{Isolation, RecoveryCheck, Semantics},
        ledger::SeqSet,
    },
    krabka_units::{prelude::*, serde_units},
    serde::{Deserialize, Serialize},
    std::collections::BTreeMap,
};

/// Everything one run recorded.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Evidence {
    /// The contract's `name`, so evidence cannot be judged against another.
    pub contract: String,
    /// [`Contract::digest`](super::contract::Contract::digest) of the contract
    /// the run executed, so evidence cannot be judged against an edited copy.
    pub contract_digest: String,
    pub seed: u64,
    /// The broker image the run exercised, by digest where the runner has one.
    pub broker_image: String,
    pub started_unix_ms: i64,
    pub ended_unix_ms: i64,
    /// What the run measured its host as: CPUs, memory, free disk, the Docker
    /// version. Recorded beside the contract's declared host so a reader can
    /// tell a run on a smaller machine from one on the declared machine.
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    pub producers: Vec<ProducerLedger>,
    pub readers: Vec<ReaderLedger>,
    pub commits: Vec<CommitCheck>,
    pub faults: Vec<FaultRecord>,
    pub recoveries: Vec<RecoveryRecord>,
    pub paths: Vec<PathMeasurement>,
    /// Records a self-check deleted from the cluster on purpose. Empty in a soak.
    #[serde(default)]
    pub injected: Vec<InjectedLoss>,
}

/// One producer's account of what it sent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProducerLedger {
    pub path: String,
    /// Unique within the run; written into every record value.
    pub producer: String,
    pub semantics: Semantics,
    /// Sequence numbers `0..attempted` were each sent at least once.
    pub attempted: u64,
    /// Sends the broker acknowledged.
    pub acked: SeqSet,
    /// Sends that returned an error at least once. Such a record may or may not
    /// have been written.
    pub failed: SeqSet,
    /// Records the application sent more than once, after a failed attempt.
    pub retried: SeqSet,
    /// The transactions of a transactional producer, in the order begun.
    #[serde(default)]
    pub transactions: Vec<TxnLedger>,
}

/// One transaction and how it ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxnLedger {
    pub id: u64,
    pub seqs: SeqSet,
    pub outcome: TxnOutcome,
}

/// How a transaction ended, as its producer saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxnOutcome {
    /// `EndTxn(commit)` was acknowledged.
    Committed,
    /// `EndTxn(abort)` was acknowledged, or the transaction was abandoned
    /// before its commit was sent.
    Aborted,
    /// The commit was sent and its answer never arrived. The transaction may
    /// have committed or aborted, but it must have done one of them whole.
    Unknown,
}

/// What one reader saw of one producer's records, reading from the earliest
/// offset after the load stopped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReaderLedger {
    pub path: String,
    pub isolation: Isolation,
    pub producer: String,
    pub seen: SeqSet,
    /// Records seen at more than one offset.
    #[serde(default)]
    pub duplicates: Vec<DuplicateRead>,
    /// For a compacted path: the highest sequence number read for each key.
    #[serde(default)]
    pub latest_by_key: BTreeMap<u64, u64>,
}

/// One record read at more than one position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuplicateRead {
    pub seq: u64,
    /// Every `(partition, offset)` the record was read at.
    pub positions: Vec<(i32, i64)>,
}

/// The `fault` of the committed-offset check taken after the load stops.
pub const FINAL_COMMIT_CHECK: &str = "final";

/// A committed offset checked across a fault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitCheck {
    pub group: String,
    pub topic: String,
    pub partition: i32,
    /// The fault this check straddles, or `final` for the end of the run.
    pub fault: String,
    /// The last offset the coordinator acknowledged committing before the fault.
    pub acknowledged: i64,
    /// What `OffsetFetch` returned once the fault had recovered. `None` when it
    /// returned nothing at all.
    pub fetched: Option<i64>,
    /// The partition's log end offset when `fetched` was read.
    pub log_end: i64,
}

/// A fault as it was executed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultRecord {
    /// The scheduled occurrence's id, `<plan>#<n>`.
    pub id: String,
    pub kind: String,
    /// The broker it landed on.
    pub target: String,
    /// What that broker was when the fault started: `controller`, `leader`,
    /// `follower`, or a plain `broker`.
    pub role: String,
    pub injected_unix_ms: i64,
    /// When cleanup completed. `None` when it never did.
    pub cleared_unix_ms: Option<i64>,
    /// Why cleanup failed, when it did.
    #[serde(default)]
    pub cleanup_error: Option<String>,
    /// What the fault did, for example the bytes a disk fill wrote.
    #[serde(default)]
    pub detail: String,
}

/// One recovery, measured after a fault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRecord {
    pub fault: String,
    pub check: RecoveryCheck,
    /// The instant the deadline runs from: the injection for a failover, the
    /// cleanup for a rejoin or a catch-up.
    pub from_unix_ms: i64,
    /// When the recovery was observed. `None` when the harness gave up.
    pub recovered_unix_ms: Option<i64>,
}

impl RecoveryRecord {
    /// How long recovery took, when it was observed.
    ///
    /// A recovery stamped before the instant it is measured from was not
    /// observed: a wall-clock step or a broken ledger must not read as an
    /// instant recovery that meets every deadline.
    #[must_use]
    pub fn took(&self) -> Option<Time> {
        let recovered = self.recovered_unix_ms?;
        (recovered >= self.from_unix_ms)
            .then(|| Time::from_millis(recovered.saturating_sub(self.from_unix_ms)))
    }
}

/// Latency percentiles over one window kind.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LatencySummary {
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub p50: Time,
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub p99: Time,
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub p999: Time,
    pub count: u64,
}

/// One path's measured latency and throughput.
///
/// "Steady" is everything outside the fault windows and their recovery
/// deadlines; "disturbed" is everything inside them. The budgets apply to the
/// steady windows. The disturbed figures are retained as evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathMeasurement {
    pub path: String,
    pub produce_steady: LatencySummary,
    pub produce_disturbed: LatencySummary,
    pub consume_steady: LatencySummary,
    pub consume_disturbed: LatencySummary,
    /// Acknowledged records per second over the steady windows.
    #[serde(with = "serde_units::human::frequency")]
    pub steady_rate: Frequency,
}

/// One record a self-check deleted from the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InjectedLoss {
    pub path: String,
    pub producer: String,
    pub seq: u64,
    pub partition: i32,
    pub offset: i64,
}

/// The record value prefix that carries a record's identity:
/// `<producer>/<seq>;`. The rest of the value is padding.
#[must_use]
pub fn record_tag(producer: &str, seq: u64) -> String { format!("{producer}/{seq};") }

/// The `(producer, seq)` a record value carries, or `None` for a value that
/// no soak producer wrote.
#[must_use]
pub fn parse_record_tag(value: &[u8]) -> Option<(&str, u64)> {
    let end = value.iter().position(|byte| *byte == b';')?;
    let tag = std::str::from_utf8(&value[..end]).ok()?;
    let (producer, seq) = tag.rsplit_once('/')?;
    Some((producer, seq.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use {
        super::{RecoveryRecord, parse_record_tag, record_tag},
        crate::soak::contract::RecoveryCheck,
        assert2::check,
        krabka_units::prelude::*,
    };

    #[yare::parameterized(
        plain = { b"p-0/17;xxxx", Some(("p-0", 17)) },
        slash_in_name = { b"path/p-1/3;", Some(("path/p-1", 3)) },
        no_terminator = { b"p-0/17xxxx", None },
        no_number = { b"p-0/;", None },
        foreign = { b"hello world", None },
    )]
    fn a_record_value_names_its_send(value: &[u8], expected: Option<(&str, u64)>) {
        check!(parse_record_tag(value) == expected);
    }

    #[test]
    fn a_tag_round_trips() {
        let mut value = record_tag("classic-idempotent-0", 42).into_bytes();
        value.extend_from_slice(b"padding");
        check!(parse_record_tag(&value) == Some(("classic-idempotent-0", 42)));
    }

    #[yare::parameterized(
        observed = { Some(2_500), Some(millis(1_500)) },
        never = { None, None },
        clock_step_back = { Some(900), None },
        same_instant = { Some(1_000), Some(Time::ZERO) },
    )]
    fn a_recovery_reports_how_long_it_took(recovered: Option<i64>, took: Option<Time>) {
        let record = RecoveryRecord {
            fault: "f#0".into(),
            check: RecoveryCheck::IsrRejoin,
            from_unix_ms: 1_000,
            recovered_unix_ms: recovered,
        };
        check!(record.took() == took);
    }
}
