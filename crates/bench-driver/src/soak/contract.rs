//! The sustained-load contract: what a soak declares before it runs.
//!
//! A contract is a YAML file under `bench/soak/`. It fixes every number the
//! verdict is judged against -- host resources, disk capacity, seed, client
//! settings, throughput targets, latency budgets, recovery deadlines and the
//! fault plan -- so that a run cannot be judged against limits chosen after
//! its results were seen. [`Contract::validate`] refuses a contract that could
//! not be run safely or judged honestly, and lists every reason at once.
//!
//! Every size, extent and rate carries its unit, as a benchmark scenario does:
//! `64KiB`, `15s`, `2000/s`.

use {
    super::lifecycle::{Lifecycle, Side},
    crate::scenario::{Acks, Compression, bounded},
    anyhow::{Context as _, bail},
    krabka_units::{fmt::Human as _, prelude::*, serde_units},
    serde::{Deserialize, Serialize},
    sha2::{Digest as _, Sha256},
    std::{collections::BTreeSet, fmt, path::Path},
};

/// Whether a contract is a soak or the short self-check of the soak machinery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    /// A run whose verdict is evidence about the broker. It must cover every
    /// load path the milestone names, and it may inject nothing into its own
    /// evidence.
    Soak,
    /// A run of minutes whose verdict is evidence about the harness. It injects
    /// one missing record and one missed deadline and passes only when both,
    /// and nothing else, are detected.
    SelfCheck,
    /// A run whose verdict is evidence about upgrading, reconfiguring and
    /// rolling back release images under load. Its plan holds lifecycle
    /// transitions, and each one must reconcile at its checkpoint as well as at
    /// the end. See [`super::lifecycle`].
    Lifecycle,
    /// A disaster-recovery drill: the load runs through a capture and into the
    /// loss of the whole source cluster, and the verdict reconciles what a
    /// fresh cluster restored from the capture serves. Every record
    /// acknowledged before the capture started must come back; the records
    /// acknowledged after it that do not are the recovery point, judged
    /// against the declared [`RecoveryObjectives`].
    DisasterRecovery,
}

/// A sustained-load and failure contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    pub name: String,
    pub profile: Profile,
    /// Seeds the fault-schedule jitter and the choice of an `any` target, so
    /// two runs of one contract inject the same faults at the same offsets.
    pub seed: u64,
    /// Length of the load, from the first record to the start of reconciliation.
    #[serde(with = "bounded::positive_time")]
    pub duration: Time,
    /// The fault-free tail at the end of the load. No fault may start or still
    /// be recovering inside it, so reconciliation reads a settled cluster.
    #[serde(with = "bounded::nonnegative_time")]
    pub settle: Time,
    pub host: HostResources,
    pub cluster: ClusterShape,
    pub clients: ClientSettings,
    pub paths: Vec<LoadPath>,
    pub deadlines: RecoveryDeadlines,
    #[serde(default)]
    pub faults: Vec<FaultPlan>,
    #[serde(default)]
    pub self_check: Option<SelfCheck>,
    /// The release a `lifecycle` contract starts on. Left out of the encoding
    /// when absent, so the digest of a contract without one is unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Lifecycle>,
    /// The recovery objectives of a disaster-recovery drill. Omitted from the
    /// encoding when absent, so a soak's digest does not change with it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disaster_recovery: Option<RecoveryObjectives>,
}

/// What a disaster-recovery drill must achieve, declared before it runs.
///
/// The capture boundary is the instant the capture starts. A record the
/// cluster acknowledged before it is inside the boundary and must be restored.
/// A record acknowledged after it may be lost: the drill counts those, and the
/// recovery point objective bounds how far back the oldest lost one may be.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryObjectives {
    /// How long the load keeps running after the capture starts, before the
    /// source cluster is lost. Records acknowledged in it are the ones a
    /// restore from that capture may lose.
    #[serde(with = "bounded::positive_time")]
    pub post_capture_load: Time,
    /// The recovery point objective: the most time, back from the loss, whose
    /// acknowledged records may be missing from the restored cluster.
    #[serde(with = "bounded::positive_time")]
    pub rpo: Time,
    /// The recovery time objective: from the loss until a fresh cluster,
    /// restored from the capture alone, serves authenticated produce and
    /// consume of the captured data.
    #[serde(with = "bounded::positive_time")]
    pub rto: Time,
}

/// The machine a run is entitled to. Recorded so that a result names the
/// resources it was measured on, and so a disk fill is bounded by them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostResources {
    pub cpus: u32,
    #[serde(with = "bounded::positive_size")]
    pub memory: ByteSize,
    /// Each broker's data volume. A disk-pressure fault fills to a fraction of
    /// this, never of whatever the host happens to have free.
    #[serde(with = "bounded::positive_size")]
    pub disk_capacity: ByteSize,
    /// Free space a disk fill must leave on the host file system, whatever the
    /// watermark asks for. The fill is refused rather than shrunk below it.
    #[serde(with = "bounded::positive_size")]
    pub disk_reserve: ByteSize,
}

/// The cluster under test.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterShape {
    pub brokers: u32,
    pub replication_factor: i16,
    pub min_insync_replicas: i16,
    /// The controller's check-quorum window: Kafka's
    /// `controller.quorum.fetch.timeout.ms`. A pause shorter than this must not
    /// move the controller, so a pause that does not intend a failover has to
    /// stay below it.
    #[serde(with = "bounded::positive_time")]
    pub check_quorum_window: Time,
    /// Kafka's `broker.session.timeout.ms`. Omitted contracts keep the
    /// executor's default and their original evidence digest. The executor's
    /// three-second heartbeat cadence must fit inside this session.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_units::human::option_time"
    )]
    pub broker_session_timeout: Option<Time>,
    /// Kafka's `replica.lag.time.max.ms`, recorded because it decides whether a
    /// paused or slowed follower leaves the ISR at all.
    #[serde(with = "bounded::positive_time")]
    pub replica_lag_time_max: Time,
}

/// Client settings, shared by every path of their kind.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientSettings {
    pub producer: ProducerSettings,
    pub transactions: TransactionSettings,
    pub consumer: ConsumerSettings,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerSettings {
    pub acks: Acks,
    #[serde(default)]
    pub compression: Compression,
    #[serde(with = "bounded::nonnegative_time")]
    pub linger: Time,
    #[serde(with = "bounded::positive_size")]
    pub batch_size: ByteSize,
    #[serde(with = "bounded::positive_time")]
    pub request_timeout: Time,
    #[serde(with = "bounded::positive_time")]
    pub delivery_timeout: Time,
    /// How many times the application sends one record before it gives up.
    /// A record sent more than once may legitimately appear more than once.
    pub max_send_attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionSettings {
    #[serde(with = "bounded::positive_time")]
    pub timeout: Time,
    pub records_per_transaction: u32,
    /// Every `abort_every`-th transaction is aborted on purpose, so aborted
    /// data exists for a `read_committed` reader to wrongly see.
    pub abort_every: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerSettings {
    #[serde(with = "bounded::positive_time")]
    pub session_timeout: Time,
    /// How often a committing consumer commits what it has polled.
    #[serde(with = "bounded::positive_time")]
    pub commit_interval: Time,
    #[serde(with = "bounded::positive_time")]
    pub poll_timeout: Time,
}

/// What a path's producer promises about its records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Semantics {
    /// An idempotent producer: a record the application sent once appears at
    /// most once, and an acknowledged one appears exactly once.
    Idempotent,
    /// A transactional producer: committed transactions appear whole under
    /// `read_committed`, aborted ones never do.
    Transactional,
    /// An idempotent producer over a compacted topic: the latest acknowledged
    /// value of every key survives compaction.
    Compacted,
}

/// Where a path's topic keeps its data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Storage {
    /// Local segments only.
    Classic,
    /// KIP-405 tiered storage, with a local retention short enough that the
    /// read-back from the earliest offset is served from the remote tier.
    Tiered,
    /// The diskless data path: the quorum WAL and the object store.
    Diskless,
}

/// A consumer's isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Isolation {
    ReadCommitted,
    ReadUncommitted,
}

impl fmt::Display for Isolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Isolation::ReadCommitted => "read_committed",
            Isolation::ReadUncommitted => "read_uncommitted",
        })
    }
}

/// Latency percentiles a path must stay under outside its fault windows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyBudget {
    #[serde(with = "bounded::positive_time")]
    pub p50: Time,
    #[serde(with = "bounded::positive_time")]
    pub p99: Time,
    #[serde(with = "bounded::positive_time")]
    pub p999: Time,
}

/// One load path: a producer, its topic, and the consumers that read it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadPath {
    pub name: String,
    pub semantics: Semantics,
    pub storage: Storage,
    pub partitions: i32,
    /// The paced produce rate.
    #[serde(with = "bounded::positive_rate")]
    pub rate: Frequency,
    /// The lowest achieved produce rate, outside fault windows, that passes.
    #[serde(with = "bounded::positive_rate")]
    pub throughput_floor: Frequency,
    #[serde(with = "bounded::positive_size")]
    pub payload: ByteSize,
    /// Distinct keys a compacted path cycles through.
    #[serde(default)]
    pub key_space: Option<u64>,
    /// The isolation levels this path is consumed and read back at.
    pub consumers: Vec<Isolation>,
    /// Whether the consumers commit offsets, so committed offsets are checked
    /// across every fault.
    #[serde(default)]
    pub commit_offsets: bool,
    /// Acknowledgement latency of the producer.
    pub produce_latency: LatencyBudget,
    /// Create-time to poll latency of the consumers.
    pub consume_latency: LatencyBudget,
}

/// How long the cluster has to recover from a fault.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryDeadlines {
    /// From the fault until every partition the target led has a live leader
    /// elsewhere.
    #[serde(with = "bounded::positive_time")]
    pub leader_failover: Time,
    /// From the fault's end until every partition's ISR is its full replica set
    /// again.
    #[serde(with = "bounded::positive_time")]
    pub isr_rejoin: Time,
    /// From the fault until another node is the active controller.
    #[serde(with = "bounded::positive_time")]
    pub controller_failover: Time,
    /// From `SIGCONT` until every partition's ISR is its full replica set again.
    #[serde(with = "bounded::positive_time")]
    pub catch_up_after_pause: Time,
    /// From a certificate rotation until the rotated broker serves the new
    /// certificate. Required when the plan rotates one.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_units::human::option_time"
    )]
    pub certificate_rotation: Option<Time>,
    /// From a reassignment until it is complete: no partition of the path is
    /// still reassigning, each has the planned replicas, and its ISR is full.
    /// Required when the plan reassigns.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_units::human::option_time"
    )]
    pub reassignment: Option<Time>,
}

/// A recovery the harness measures after a fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryCheck {
    LeaderFailover,
    IsrRejoin,
    ControllerFailover,
    CatchUpAfterPause,
    CertificateServed,
    ReassignmentComplete,
}

impl RecoveryCheck {
    /// The deadline the contract gives this check. A lifecycle check whose
    /// deadline the contract left out has none to meet: [`Contract::validate`]
    /// refuses a plan that needs it.
    #[must_use]
    pub fn deadline(self, deadlines: &RecoveryDeadlines) -> Time {
        match self {
            RecoveryCheck::LeaderFailover => deadlines.leader_failover,
            RecoveryCheck::IsrRejoin => deadlines.isr_rejoin,
            RecoveryCheck::ControllerFailover => deadlines.controller_failover,
            RecoveryCheck::CatchUpAfterPause => deadlines.catch_up_after_pause,
            RecoveryCheck::CertificateServed => {
                deadlines.certificate_rotation.unwrap_or(Time::ZERO)
            }
            RecoveryCheck::ReassignmentComplete => deadlines.reassignment.unwrap_or(Time::ZERO),
        }
    }
}

impl fmt::Display for RecoveryCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RecoveryCheck::LeaderFailover => "leader_failover",
            RecoveryCheck::IsrRejoin => "isr_rejoin",
            RecoveryCheck::ControllerFailover => "controller_failover",
            RecoveryCheck::CatchUpAfterPause => "catch_up_after_pause",
            RecoveryCheck::CertificateServed => "certificate_served",
            RecoveryCheck::ReassignmentComplete => "reassignment_complete",
        })
    }
}

/// Which broker a fault lands on. All but `broker` are resolved when the
/// fault starts, against the cluster as it is then, and the resolved broker is
/// recorded with the fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Target {
    /// The active controller.
    Controller,
    /// The leader of the first path's partition 0.
    Leader,
    /// A broker that is neither the controller nor that leader.
    Follower,
    /// A broker chosen from the seed when the schedule is expanded.
    Any,
    /// The broker at this index, counting from zero.
    Broker(u32),
}

/// How a slow follower is slowed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum Slowdown {
    /// `tc qdisc ... netem delay` in the broker's network namespace, run from
    /// `image`, which must carry `tc`.
    Netem {
        #[serde(with = "bounded::positive_time")]
        delay: Time,
        #[serde(with = "bounded::nonnegative_time")]
        jitter: Time,
        image: String,
    },
    /// A CPU quota on the broker's container, which slows its fetches and its
    /// disk writes alike.
    CpuThrottle { cpus: f64 },
}

/// What a fault does.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FaultKind {
    /// Fill the target's data volume to `watermark` of `host.disk_capacity`.
    DiskPressure {
        #[serde(with = "serde_units::human::ratio")]
        watermark: Ratio,
    },
    /// Slow the target's replication.
    SlowFollower { slowdown: Slowdown },
    /// `SIGSTOP` the target for the fault's duration, then `SIGCONT` it.
    Pause {
        /// Whether the pause is meant to outlast the check-quorum window and
        /// move leadership. A pause that is not must stay below the window.
        #[serde(default)]
        intends_failover: bool,
    },
    /// Stop the target and start it again after the fault's duration.
    Restart {
        /// `SIGTERM` and a controlled shutdown when true, `SIGKILL` when false.
        graceful: bool,
    },
    /// A lifecycle transition: stop the target with a controlled shutdown,
    /// and after the fault's duration start it again, on the same data, from
    /// the image of the `to` release.
    Roll { to: Side },
    /// A lifecycle transition: give the target's TLS listener a certificate
    /// from a new CA, with the same DN and SANs, and a truststore that holds
    /// both CAs. The files are rewritten in place and the listener's
    /// keystore and truststore are altered to them, which the broker reloads
    /// without a restart.
    CertificateRotation,
    /// A lifecycle transition: drain the target of `path`. Every replica of
    /// the path that the target holds moves, with
    /// `AlterPartitionReassignments`, to a broker that holds no replica of that
    /// partition, so the cluster needs more brokers than the replication
    /// factor.
    Reassignment { path: String },
}

impl FaultKind {
    /// The snake-case name the YAML spells this kind with.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            FaultKind::DiskPressure { .. } => "disk_pressure",
            FaultKind::SlowFollower { .. } => "slow_follower",
            FaultKind::Pause { .. } => "pause",
            FaultKind::Restart { .. } => "restart",
            FaultKind::Roll { .. } => "roll",
            FaultKind::CertificateRotation => "certificate_rotation",
            FaultKind::Reassignment { .. } => "reassignment",
        }
    }

    /// Whether the fault takes its target out of the quorum long enough to
    /// move leadership, and so a controller failover when the target is the
    /// active controller.
    #[must_use]
    pub fn moves_leadership(&self) -> bool {
        matches!(
            self,
            FaultKind::Restart { .. }
                | FaultKind::Roll { .. }
                | FaultKind::Pause {
                    intends_failover: true
                }
        )
    }

    /// The recoveries the harness measures after a fault of this kind on
    /// `target`.
    #[must_use]
    pub fn checks(&self, target: Target) -> Vec<RecoveryCheck> {
        let mut checks = match self {
            FaultKind::Pause {
                intends_failover: false,
            } => vec![RecoveryCheck::CatchUpAfterPause],
            FaultKind::Pause {
                intends_failover: true,
            } => vec![
                RecoveryCheck::LeaderFailover,
                RecoveryCheck::CatchUpAfterPause,
            ],
            FaultKind::Restart { .. } | FaultKind::Roll { .. } => {
                vec![RecoveryCheck::LeaderFailover, RecoveryCheck::IsrRejoin]
            }
            FaultKind::SlowFollower { .. } | FaultKind::DiskPressure { .. } => {
                vec![RecoveryCheck::IsrRejoin]
            }
            FaultKind::CertificateRotation => vec![RecoveryCheck::CertificateServed],
            FaultKind::Reassignment { .. } => vec![RecoveryCheck::ReassignmentComplete],
        };
        if self.moves_leadership() && target == Target::Controller {
            checks.push(RecoveryCheck::ControllerFailover);
        }
        checks
    }
}

/// A fault, once or on a period.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultPlan {
    pub name: String,
    /// What the fault does, as `{ kind: pause }` or `{ kind: restart, graceful: false }`.
    pub fault: FaultKind,
    pub target: Target,
    /// When the first occurrence starts, from the start of the load.
    #[serde(with = "bounded::nonnegative_time")]
    pub first_at: Time,
    /// The period of later occurrences. Absent means once.
    #[serde(default, with = "serde_units::human::option_time")]
    pub every: Option<Time>,
    /// Each occurrence starts up to this much later than its slot, by a draw
    /// from the seed.
    #[serde(default = "zero_time", with = "bounded::nonnegative_time")]
    pub jitter: Time,
    /// How long the fault holds before its cleanup runs.
    #[serde(with = "bounded::positive_time")]
    pub duration: Time,
}

fn zero_time() -> Time { Time::ZERO }

/// What a self-check injects, so that it can prove each is detected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelfCheck {
    /// The path whose earliest acknowledged record is deleted from the cluster
    /// once the load stops. The reconciler must report exactly that record lost.
    pub missing_record_path: String,
    pub missed_deadline: MissedDeadline,
}

/// A recovery deadline the self-check sets below anything a cluster can meet,
/// so the deadline checker must report it missed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissedDeadline {
    /// The fault plan whose first occurrence carries the impossible deadline.
    pub fault: String,
    pub check: RecoveryCheck,
    #[serde(with = "bounded::positive_time")]
    pub deadline: Time,
}

/// One reason a contract cannot be run or judged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractError(pub String);

impl fmt::Display for ContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.0) }
}

/// The highest fraction of a data volume a disk-pressure fault may fill.
pub const MAX_DISK_WATERMARK: f64 = 0.95;

impl Contract {
    /// Parse a contract from YAML. This checks the shape only; call
    /// [`Self::validate`] before running or judging it.
    ///
    /// # Errors
    ///
    /// Returns the parser's error for a document that is not a contract.
    pub fn from_yaml(text: &str) -> Result<Self, serde_saphyr::Error> {
        serde_saphyr::from_str(text)
    }

    /// Read, parse and validate the contract at `path`.
    ///
    /// # Errors
    ///
    /// Returns why the file cannot be read or parsed, or every reason
    /// [`Self::validate`] refuses it, one per line.
    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let contract =
            Self::from_yaml(&text).with_context(|| format!("parse contract {}", path.display()))?;
        let errors = contract.validate();
        if !errors.is_empty() {
            bail!(
                "{} is not a usable contract ({} problem(s)):\n{}",
                path.display(),
                errors.len(),
                errors
                    .iter()
                    .map(|error| format!("contract: {error}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }
        Ok(contract)
    }

    /// The SHA-256 of the contract's canonical JSON encoding, as lowercase hex.
    ///
    /// Evidence records this, and the judge refuses evidence whose digest
    /// differs. A name and a seed do not change when a budget or a deadline is
    /// edited after a run; this does.
    #[must_use]
    pub fn digest(&self) -> String {
        let canonical = serde_json::to_vec(self).unwrap_or_default();
        hex::encode(Sha256::digest(canonical))
    }

    /// The path named `name`.
    #[must_use]
    pub fn path(&self, name: &str) -> Option<&LoadPath> {
        self.paths.iter().find(|path| path.name == name)
    }

    /// The load paths, storage kinds and isolation levels the milestone names
    /// that this contract does not cover.
    #[must_use]
    pub fn missing_coverage(&self) -> Vec<String> {
        let semantics: BTreeSet<Semantics> = self.paths.iter().map(|path| path.semantics).collect();
        let storage: BTreeSet<Storage> = self.paths.iter().map(|path| path.storage).collect();
        let isolation: BTreeSet<Isolation> = self
            .paths
            .iter()
            .flat_map(|path| path.consumers.iter().copied())
            .collect();
        let mut missing = Vec::new();
        for (wanted, label) in [
            (Semantics::Idempotent, "an idempotent produce path"),
            (Semantics::Transactional, "a transactional produce path"),
            (Semantics::Compacted, "a compacted path"),
        ] {
            if !semantics.contains(&wanted) {
                missing.push(label.to_owned());
            }
        }
        for (wanted, label) in [
            (Storage::Classic, "a classic topic"),
            (Storage::Tiered, "a tiered topic"),
            (Storage::Diskless, "a diskless topic"),
        ] {
            if !storage.contains(&wanted) {
                missing.push(label.to_owned());
            }
        }
        for wanted in [Isolation::ReadCommitted, Isolation::ReadUncommitted] {
            if !isolation.contains(&wanted) {
                missing.push(format!("a {wanted} consumer"));
            }
        }
        if !self.paths.iter().any(|path| path.commit_offsets) {
            missing.push("a path that commits offsets".to_owned());
        }
        missing
    }

    /// Every reason this contract cannot be run safely or judged honestly.
    ///
    /// An empty list is a contract the harness will run. The fault schedule is
    /// expanded and checked here too, so a plan whose occurrences would overlap
    /// or run into the settle window is refused before the first record.
    #[must_use]
    pub fn validate(&self) -> Vec<ContractError> {
        let mut errors = Vec::new();
        let mut fail = |message: String| errors.push(ContractError(message));

        if self.settle >= self.duration {
            fail(format!(
                "settle ({}) must be shorter than the run ({})",
                human(self.settle),
                human(self.duration)
            ));
        }
        self.validate_cluster(&mut fail);
        self.validate_clients(&mut fail);
        self.validate_paths(&mut fail);
        self.validate_faults(&mut fail);
        self.validate_transitions(&mut fail);
        self.validate_profile(&mut fail);
        if let Err(schedule_errors) = super::schedule::expand(self) {
            errors.extend(schedule_errors);
        }
        errors
    }

    fn validate_cluster(&self, fail: &mut impl FnMut(String)) {
        let cluster = &self.cluster;
        if cluster
            .broker_session_timeout
            .is_some_and(|timeout| timeout <= secs(3))
        {
            fail(
                "broker_session_timeout must exceed the executor's 3s heartbeat interval"
                    .to_owned(),
            );
        }
        if cluster.replication_factor != 3 {
            fail(format!(
                "replication_factor is {}; the sustained-load contract is RF=3",
                cluster.replication_factor
            ));
        }
        if u32::try_from(cluster.replication_factor).unwrap_or(u32::MAX) > cluster.brokers {
            fail(format!(
                "replication_factor {} needs at least that many brokers, not {}",
                cluster.replication_factor, cluster.brokers
            ));
        }
        if cluster.min_insync_replicas < 2
            || cluster.min_insync_replicas >= cluster.replication_factor
        {
            fail(format!(
                "min_insync_replicas is {}; it must be at least 2 and below the replication factor, \
                 or one fault either loses acknowledged data or stops every write",
                cluster.min_insync_replicas
            ));
        }
        if self.host.cpus == 0 {
            fail("host.cpus must be at least 1".to_owned());
        }
    }

    fn validate_clients(&self, fail: &mut impl FnMut(String)) {
        let producer = &self.clients.producer;
        if producer.acks != Acks::All {
            fail("clients.producer.acks must be `all`: anything less acknowledges records the contract cannot hold the cluster to".to_owned());
        }
        if producer.max_send_attempts == 0 {
            fail("clients.producer.max_send_attempts must be at least 1".to_owned());
        }
        if producer.delivery_timeout < producer.request_timeout + producer.linger {
            fail("clients.producer.delivery_timeout must cover request_timeout plus linger, as Kafka requires".to_owned());
        }
        let txn = &self.clients.transactions;
        if txn.records_per_transaction == 0 {
            fail("clients.transactions.records_per_transaction must be at least 1".to_owned());
        }
        if txn.abort_every < 2 {
            fail(
                "clients.transactions.abort_every must be at least 2, so some transactions commit"
                    .to_owned(),
            );
        }
    }

    fn validate_paths(&self, fail: &mut impl FnMut(String)) {
        if self.paths.is_empty() {
            fail("a contract needs at least one load path".to_owned());
        }
        let mut names = BTreeSet::new();
        for path in &self.paths {
            let name = &path.name;
            if !names.insert(name.as_str()) {
                fail(format!("path `{name}` is declared twice"));
            }
            if path.partitions < 1 {
                fail(format!("path `{name}` needs at least one partition"));
            }
            if path.throughput_floor > path.rate {
                fail(format!(
                    "path `{name}` sets a throughput floor above its paced rate, which no run can meet"
                ));
            }
            for (label, budget) in [
                ("produce", &path.produce_latency),
                ("consume", &path.consume_latency),
            ] {
                if budget.p50 > budget.p99 || budget.p99 > budget.p999 {
                    fail(format!(
                        "path `{name}`: the {label} latency budget must satisfy p50 <= p99 <= p999"
                    ));
                }
            }
            if path.consumers.is_empty() {
                fail(format!(
                    "path `{name}` has no consumers, so nothing would reconcile what it acknowledged"
                ));
            }
            match (path.semantics, path.key_space) {
                (Semantics::Compacted, None | Some(0)) => {
                    fail(format!(
                        "compacted path `{name}` needs a positive key_space"
                    ));
                }
                (Semantics::Idempotent | Semantics::Transactional, Some(_)) => {
                    fail(format!("path `{name}` sets key_space but is not compacted"));
                }
                (Semantics::Compacted, Some(_)) | (_, None) => {}
            }
            if path.semantics == Semantics::Transactional
                && !path.consumers.contains(&Isolation::ReadCommitted)
            {
                fail(format!(
                    "transactional path `{name}` needs a read_committed consumer, or aborted data could \
                     never be caught"
                ));
            }
            if path.semantics == Semantics::Compacted && path.storage == Storage::Diskless {
                fail(format!(
                    "compacted path `{name}` cannot be diskless: compaction runs over classic segments"
                ));
            }
        }
    }

    fn validate_faults(&self, fail: &mut impl FnMut(String)) {
        let mut names = BTreeSet::new();
        for plan in &self.faults {
            let name = &plan.name;
            if !names.insert(name.as_str()) {
                fail(format!("fault `{name}` is declared twice"));
            }
            if let Target::Broker(index) = plan.target
                && index >= self.cluster.brokers
            {
                fail(format!(
                    "fault `{name}` targets broker {index} of a {}-broker cluster",
                    self.cluster.brokers
                ));
            }
            if let Some(every) = plan.every
                && plan.jitter >= every
            {
                fail(format!(
                    "fault `{name}`: jitter must be shorter than its period"
                ));
            }
            match &plan.fault {
                FaultKind::Pause {
                    intends_failover: true,
                } if self.cluster.broker_session_timeout.is_some_and(|session| {
                    plan.duration <= session + self.cluster.check_quorum_window
                }) =>
                {
                    fail(format!(
                        "fault `{name}` must outlast controller election plus the declared broker session timeout"
                    ));
                }
                FaultKind::Pause {
                    intends_failover: false,
                } if plan.duration >= self.cluster.check_quorum_window => fail(format!(
                    "fault `{name}` pauses for {} but the check-quorum window is {}: a pause that does \
                     not intend a failover must stay below it",
                    human(plan.duration),
                    human(self.cluster.check_quorum_window)
                )),
                FaultKind::Pause {
                    intends_failover: true,
                } if plan.duration < self.cluster.check_quorum_window => fail(format!(
                    "fault `{name}` intends a failover but pauses for {}, inside the {} check-quorum window",
                    human(plan.duration),
                    human(self.cluster.check_quorum_window)
                )),
                FaultKind::DiskPressure { watermark } => {
                    let fraction = watermark.as_f64();
                    if !(fraction > 0.0 && fraction <= MAX_DISK_WATERMARK) {
                        fail(format!(
                            "fault `{name}`: the disk watermark must be above 0% and at most {:.0}%",
                            MAX_DISK_WATERMARK * 100.0
                        ));
                    }
                }
                FaultKind::SlowFollower {
                    slowdown: Slowdown::CpuThrottle { cpus },
                } if !(*cpus > 0.0 && *cpus < f64::from(self.host.cpus)) => fail(format!(
                    "fault `{name}`: a CPU throttle must be above zero and below host.cpus"
                )),
                FaultKind::SlowFollower {
                    slowdown: Slowdown::Netem { image, .. },
                } if !image.contains("@sha256:") => fail(format!(
                    "fault `{name}`: the netem image must be pinned by digest"
                )),
                _ => {}
            }
        }
    }

    /// The rules for lifecycle transitions: a `roll` needs a declared source
    /// and a fixed broker, a rotation and a reassignment need their deadlines,
    /// and a reassignment needs a path and a broker that holds none of it.
    fn validate_transitions(&self, fail: &mut impl FnMut(String)) {
        for plan in &self.faults {
            let name = &plan.name;
            let is_transition = matches!(
                plan.fault,
                FaultKind::Roll { .. }
                    | FaultKind::CertificateRotation
                    | FaultKind::Reassignment { .. }
            );
            if is_transition && self.profile != Profile::Lifecycle {
                fail(format!(
                    "fault `{name}` is a lifecycle transition, which only a lifecycle contract runs"
                ));
            }
            if is_transition && plan.every.is_some() {
                fail(format!(
                    "transition `{name}` repeats; a lifecycle transition happens once"
                ));
            }
            if is_transition && !matches!(plan.target, Target::Broker(_)) {
                fail(format!(
                    "transition `{name}` must target a broker by index, so the plan says which \
                     broker moves and in what order"
                ));
            }
            match &plan.fault {
                FaultKind::CertificateRotation if self.deadlines.certificate_rotation.is_none() => {
                    fail(format!(
                        "transition `{name}` rotates a certificate, but deadlines.certificate_rotation \
                         is not set"
                    ));
                }
                FaultKind::Reassignment { path } => {
                    if self.deadlines.reassignment.is_none() {
                        fail(format!(
                            "transition `{name}` reassigns, but deadlines.reassignment is not set"
                        ));
                    }
                    if self.path(path).is_none() {
                        fail(format!(
                            "transition `{name}` reassigns `{path}`, which is no path"
                        ));
                    }
                    if u32::try_from(self.cluster.replication_factor).unwrap_or(u32::MAX)
                        >= self.cluster.brokers
                    {
                        fail(format!(
                            "transition `{name}` reassigns, but every one of the {} brokers already \
                             holds a replica at replication_factor {}",
                            self.cluster.brokers, self.cluster.replication_factor
                        ));
                    }
                }
                _ => {}
            }
        }
    }

    fn validate_profile(&self, fail: &mut impl FnMut(String)) {
        if self.profile != Profile::DisasterRecovery && self.disaster_recovery.is_some() {
            fail(
                "only a disaster_recovery contract declares recovery objectives; drop disaster_recovery"
                    .to_owned(),
            );
        }
        if self.profile == Profile::Lifecycle {
            self.validate_lifecycle(fail);
            return;
        }
        if self.lifecycle.is_some() {
            fail("only a lifecycle contract declares a lifecycle section".to_owned());
        }
        match (self.profile, &self.self_check) {
            (Profile::DisasterRecovery, _) => self.validate_drill(fail),
            (Profile::Soak, Some(_)) => {
                fail(
                    "a soak may not inject failures into its own evidence; drop self_check"
                        .to_owned(),
                );
            }
            (Profile::Soak, None) => {
                if self.faults.is_empty() {
                    fail(
                        "a soak needs at least one fault plan, or it proves nothing about recovery"
                            .to_owned(),
                    );
                }
                for missing in self.missing_coverage() {
                    fail(format!("a soak must cover {missing}"));
                }
            }
            (Profile::SelfCheck, None) => {
                fail("a self-check contract needs a self_check section".to_owned());
            }
            (Profile::SelfCheck, Some(check)) => {
                match self.path(&check.missing_record_path) {
                    None => fail(format!(
                        "self_check.missing_record_path names no path: `{}`",
                        check.missing_record_path
                    )),
                    Some(path) if path.semantics != Semantics::Idempotent => fail(format!(
                        "self_check.missing_record_path `{}` must be an idempotent path, where one \
                         deleted record is exactly one lost record",
                        path.name
                    )),
                    Some(_) => {}
                }
                let missed = &check.missed_deadline;
                match self.faults.iter().find(|plan| plan.name == missed.fault) {
                    None => fail(format!(
                        "self_check.missed_deadline names no fault: `{}`",
                        missed.fault
                    )),
                    Some(plan) if !plan.fault.checks(plan.target).contains(&missed.check) => {
                        fail(format!(
                            "self_check.missed_deadline: fault `{}` is not measured for {}",
                            missed.fault, missed.check
                        ));
                    }
                    Some(_) => {}
                }
                if missed.deadline >= missed.check.deadline(&self.deadlines) {
                    fail("self_check.missed_deadline must be shorter than the contract's own deadline".to_owned());
                }
            }
            // Validated above, by `validate_lifecycle`.
            (Profile::Lifecycle, _) => {}
        }
    }

    /// A lifecycle contract declares its source release by digest, injects
    /// nothing into its own evidence, and upgrades at least one broker.
    fn validate_lifecycle(&self, fail: &mut impl FnMut(String)) {
        if self.self_check.is_some() {
            fail(
                "a lifecycle run may not inject failures into its own evidence; drop self_check"
                    .to_owned(),
            );
        }
        match &self.lifecycle {
            None => fail("a lifecycle contract needs a lifecycle section".to_owned()),
            Some(lifecycle) => {
                let source = &lifecycle.source;
                if !source.image.contains("@sha256:") {
                    fail(format!(
                        "lifecycle.source.image must be pinned by digest, not `{}`",
                        source.image
                    ));
                }
                if !source.supports.contains_key("metadata.version") {
                    fail(
                        "lifecycle.source.supports must name metadata.version, which every release \
                         supports"
                            .to_owned(),
                    );
                }
                for (feature, range) in &source.supports {
                    if range.min > range.max {
                        fail(format!(
                            "lifecycle.source.supports.{feature} is inverted: {range}"
                        ));
                    }
                }
            }
        }
        let upgrades = self
            .faults
            .iter()
            .any(|plan| plan.fault == FaultKind::Roll { to: Side::Target });
        if !upgrades {
            fail("a lifecycle contract must roll at least one broker to the target".to_owned());
        }
    }

    /// A drill restores what a capture holds and nothing else, so each of its
    /// paths must be one the capture can hold, and its only fault is the loss.
    fn validate_drill(&self, fail: &mut impl FnMut(String)) {
        if self.self_check.is_some() {
            fail("a disaster-recovery drill may not inject failures; drop self_check".to_owned());
        }
        if !self.faults.is_empty() {
            fail(
                "a disaster-recovery drill's only fault is the loss of the source cluster; drop faults"
                    .to_owned(),
            );
        }
        let Some(objectives) = &self.disaster_recovery else {
            fail("a disaster-recovery contract needs a disaster_recovery section".to_owned());
            return;
        };
        if objectives.rpo < objectives.post_capture_load {
            fail(format!(
                "disaster_recovery.rpo ({}) is shorter than post_capture_load ({}): a restore of a \
                 diskless topic loses everything after the capture, so no run could meet it",
                human(objectives.rpo),
                human(objectives.post_capture_load)
            ));
        }
        if objectives.post_capture_load >= self.duration {
            fail(format!(
                "disaster_recovery.post_capture_load ({}) must be shorter than the run ({})",
                human(objectives.post_capture_load),
                human(self.duration)
            ));
        }
        for path in &self.paths {
            if path.storage == Storage::Classic {
                fail(format!(
                    "path `{}` is classic: a classic topic's segments are never archived, so it is \
                     outside the backup boundary",
                    path.name
                ));
            }
            if path.semantics != Semantics::Idempotent {
                fail(format!(
                    "path `{}` must be idempotent: transaction state is never captured, and a \
                     compacted topic is never tiered",
                    path.name
                ));
            }
        }
        let storage: BTreeSet<Storage> = self.paths.iter().map(|path| path.storage).collect();
        for (wanted, label) in [
            (Storage::Tiered, "a tiered topic"),
            (Storage::Diskless, "a diskless topic"),
        ] {
            if !storage.contains(&wanted) {
                fail(format!("a disaster-recovery drill must cover {label}"));
            }
        }
        if !self.paths.iter().any(|path| path.commit_offsets) {
            fail("a disaster-recovery drill must cover a path that commits offsets".to_owned());
        }
    }
}

/// A time in its human form, for a message.
pub(crate) fn human(time: Time) -> String { time.human().to_string() }

#[cfg(test)]
pub(crate) mod fixture {
    use {
        super::{
            Contract, FaultKind, FaultPlan, Isolation, LoadPath, Profile, RecoveryObjectives,
            Storage, Target,
        },
        crate::soak::lifecycle::{LevelRange, Lifecycle, Release, Side},
        krabka_units::prelude::*,
        std::collections::BTreeMap,
    };

    /// A valid self-check contract, small enough to read in a test.
    pub(crate) const SELF_CHECK: &str = r"
name: fixture-self-check
profile: self_check
seed: 7
duration: 4min
settle: 30s
host: { cpus: 4, memory: 8GiB, disk_capacity: 4GiB, disk_reserve: 1GiB }
cluster:
  brokers: 3
  replication_factor: 3
  min_insync_replicas: 2
  check_quorum_window: 2s
  replica_lag_time_max: 30s
clients:
  producer:
    acks: all
    linger: 5ms
    batch_size: 16KiB
    request_timeout: 10s
    delivery_timeout: 30s
    max_send_attempts: 3
  transactions: { timeout: 30s, records_per_transaction: 5, abort_every: 4 }
  consumer: { session_timeout: 10s, commit_interval: 2s, poll_timeout: 200ms }
paths:
  - name: classic-idempotent
    semantics: idempotent
    storage: classic
    partitions: 3
    rate: 50/s
    throughput_floor: 40/s
    payload: 256B
    consumers: [read_uncommitted]
    commit_offsets: true
    produce_latency: { p50: 50ms, p99: 500ms, p999: 2s }
    consume_latency: { p50: 100ms, p99: 1s, p999: 3s }
  - name: classic-transactional
    semantics: transactional
    storage: classic
    partitions: 3
    rate: 50/s
    throughput_floor: 40/s
    payload: 256B
    consumers: [read_committed, read_uncommitted]
    produce_latency: { p50: 50ms, p99: 500ms, p999: 2s }
    consume_latency: { p50: 100ms, p99: 1s, p999: 3s }
deadlines:
  leader_failover: 30s
  isr_rejoin: 60s
  controller_failover: 30s
  catch_up_after_pause: 30s
faults:
  - name: short-pause
    fault: { kind: pause }
    target: follower
    first_at: 30s
    duration: 1s
  - name: leader-restart
    fault: { kind: restart, graceful: false }
    target: leader
    first_at: 90s
    duration: 5s
self_check:
  missing_record_path: classic-idempotent
  missed_deadline: { fault: leader-restart, check: isr_rejoin, deadline: 1ms }
";

    pub(crate) fn self_check() -> Contract {
        let contract = Contract::from_yaml(SELF_CHECK).expect("the fixture parses");
        assert2::assert!(contract.validate().is_empty(), "{:?}", contract.validate());
        contract
    }

    /// The self-check's cluster and load as a valid lifecycle contract: four
    /// brokers, one upgrade, one rotation and one reassignment.
    pub(crate) fn lifecycle() -> Contract {
        let mut contract = self_check();
        contract.name = "fixture-lifecycle".to_owned();
        contract.profile = Profile::Lifecycle;
        contract.self_check = None;
        contract.cluster.brokers = 4;
        contract.deadlines.certificate_rotation = Some(secs(30));
        contract.deadlines.reassignment = Some(secs(60));
        contract.lifecycle = Some(Lifecycle {
            source: Release {
                version: "v1.0.5".to_owned(),
                image: "ghcr.io/krabka-io/krabka-broker@sha256:2a81".to_owned(),
                supports: BTreeMap::from([("metadata.version".to_owned(), LevelRange {
                    min: 7,
                    max: 30,
                })]),
            },
        });
        let plan =
            |name: &str, fault: FaultKind, broker: u32, first_at: Time, duration: Time| FaultPlan {
                name: name.to_owned(),
                fault,
                target: Target::Broker(broker),
                first_at,
                every: None,
                jitter: Time::ZERO,
                duration,
            };
        contract.faults = vec![
            plan(
                "upgrade",
                FaultKind::Roll { to: Side::Target },
                0,
                secs(30),
                secs(5),
            ),
            plan(
                "rotate",
                FaultKind::CertificateRotation,
                1,
                secs(100),
                secs(1),
            ),
            plan(
                "reassign",
                FaultKind::Reassignment {
                    path: "classic-idempotent".to_owned(),
                },
                3,
                secs(135),
                secs(1),
            ),
            plan(
                "rollback",
                FaultKind::Roll { to: Side::Source },
                0,
                secs(200),
                secs(5),
            ),
        ];
        contract.duration = minutes(6);
        assert2::assert!(contract.validate().is_empty(), "{:?}", contract.validate());
        contract
    }

    /// A valid disaster-recovery drill: the self-check's shape, with a tiered
    /// and a diskless idempotent path, no fault and recovery objectives.
    pub(crate) fn drill() -> Contract {
        let mut contract = self_check();
        contract.name = "fixture-drill".to_owned();
        contract.profile = Profile::DisasterRecovery;
        contract.duration = minutes(20);
        contract.settle = Time::ZERO;
        contract.faults.clear();
        contract.self_check = None;
        let path = contract.paths[0].clone();
        contract.paths = [
            ("tiered-idempotent", Storage::Tiered),
            ("diskless-idempotent", Storage::Diskless),
        ]
        .into_iter()
        .map(|(name, storage)| LoadPath {
            name: name.to_owned(),
            storage,
            consumers: vec![Isolation::ReadUncommitted, Isolation::ReadCommitted],
            // Low, so the recovery tests' few acknowledgements after the
            // boundary meet it.
            throughput_floor: Frequency::from_per_sec(0.1),
            ..path.clone()
        })
        .collect();
        contract.disaster_recovery = Some(RecoveryObjectives {
            post_capture_load: secs(20),
            rpo: minutes(2),
            rto: minutes(10),
        });
        assert2::assert!(contract.validate().is_empty(), "{:?}", contract.validate());
        contract
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            Acks, Contract, FaultKind, FaultPlan, Isolation, Profile, RecoveryCheck, Semantics,
            Slowdown, Storage, Target, fixture,
        },
        crate::soak::lifecycle::Side,
        assert2::{assert, check},
        krabka_units::prelude::*,
    };

    fn messages(contract: &Contract) -> Vec<String> {
        contract
            .validate()
            .into_iter()
            .map(|error| error.0)
            .collect()
    }

    fn assert_refused(contract: &Contract, fragment: &str) {
        let messages = messages(contract);
        assert!(
            messages.iter().any(|message| message.contains(fragment)),
            "expected a refusal containing {fragment:?}, got {messages:?}"
        );
    }

    #[test]
    fn the_fixture_is_valid() {
        check!(messages(&fixture::self_check()).is_empty());
    }

    #[test]
    fn the_digest_changes_with_any_declared_limit() {
        let contract = fixture::self_check();
        check!(contract.digest() == fixture::self_check().digest());
        check!(contract.digest().len() == 64);
        let mut edited = fixture::self_check();
        edited.deadlines.isr_rejoin = secs(61);
        check!(edited.digest() != contract.digest());
    }

    #[test]
    fn a_bare_number_is_refused() {
        let yaml = fixture::SELF_CHECK.replace("duration: 4min", "duration: 240");
        check!(Contract::from_yaml(&yaml).is_err());
    }

    #[test]
    fn an_unknown_key_is_refused() {
        let yaml = fixture::SELF_CHECK.replace("seed: 7", "seed: 7\nsede: 8");
        check!(Contract::from_yaml(&yaml).is_err());
    }

    #[yare::parameterized(
        rf_two = { |c: &mut Contract| c.cluster.replication_factor = 2, "RF=3" },
        min_isr_one = { |c: &mut Contract| c.cluster.min_insync_replicas = 1, "min_insync_replicas" },
        min_isr_rf = { |c: &mut Contract| c.cluster.min_insync_replicas = 3, "min_insync_replicas" },
        acks_leader = { |c: &mut Contract| c.clients.producer.acks = Acks::Leader, "acks must be `all`" },
        settle_too_long = { |c: &mut Contract| c.settle = minutes(5), "settle" },
        floor_above_rate = { |c: &mut Contract| c.paths[0].throughput_floor = per_sec(100), "throughput floor" },
        inverted_budget = { |c: &mut Contract| c.paths[0].produce_latency.p50 = secs(5), "p50 <= p99 <= p999" },
        no_consumers = { |c: &mut Contract| c.paths[0].consumers.clear(), "no consumers" },
        txn_without_rc = { |c: &mut Contract| c.paths[1].consumers.retain(|i| *i != Isolation::ReadCommitted), "read_committed consumer" },
        compacted_without_keys = { |c: &mut Contract| c.paths[0].semantics = Semantics::Compacted, "positive key_space" },
        duplicate_path = { |c: &mut Contract| c.paths[1].name = c.paths[0].name.clone(), "declared twice" },
        long_quiet_pause = { |c: &mut Contract| c.faults[0].duration = secs(3), "must stay below it" },
        short_failover_pause = { |c: &mut Contract| c.faults[0].fault = FaultKind::Pause { intends_failover: true }, "inside the" },
        broker_out_of_range = { |c: &mut Contract| c.faults[0].target = Target::Broker(3), "targets broker 3" },
        unpinned_netem = { |c: &mut Contract| c.faults[0].fault = FaultKind::SlowFollower { slowdown: Slowdown::Netem { delay: millis(100), jitter: Time::ZERO, image: "netshoot:latest".into() } }, "pinned by digest" },
        overfull_disk = { |c: &mut Contract| c.faults[0].fault = FaultKind::DiskPressure { watermark: fraction(0.99) }, "disk watermark" },
        overlapping_faults = { |c: &mut Contract| c.faults[1].first_at = secs(31), "overlaps" },
        fault_in_settle = { |c: &mut Contract| c.faults[1].first_at = secs(225), "settle" },
        unknown_self_check_path = { |c: &mut Contract| c.self_check.as_mut().unwrap().missing_record_path = "nope".into(), "names no path" },
        self_check_on_txn_path = { |c: &mut Contract| c.self_check.as_mut().unwrap().missing_record_path = "classic-transactional".into(), "must be an idempotent path" },
        unmeasured_missed_check = { |c: &mut Contract| c.self_check.as_mut().unwrap().missed_deadline.check = RecoveryCheck::ControllerFailover, "is not measured for" },
        possible_missed_deadline = { |c: &mut Contract| c.self_check.as_mut().unwrap().missed_deadline.deadline = minutes(2), "shorter than the contract's own" },
        self_check_without_section = { |c: &mut Contract| c.self_check = None, "needs a self_check section" },
        soak_with_injection = { |c: &mut Contract| c.profile = Profile::Soak, "may not inject" },
    )]
    fn an_unsafe_or_unjudgeable_contract_is_refused(edit: fn(&mut Contract), fragment: &str) {
        let mut contract = fixture::self_check();
        edit(&mut contract);
        assert_refused(&contract, fragment);
    }

    #[test]
    fn the_lifecycle_fixture_is_valid() {
        check!(messages(&fixture::lifecycle()).is_empty());
    }

    #[yare::parameterized(
        transition_in_a_soak = { |c: &mut Contract| { c.profile = Profile::Soak; c.lifecycle = None; }, "only a lifecycle contract runs" },
        lifecycle_section_in_a_soak = { |c: &mut Contract| c.profile = Profile::Soak, "only a lifecycle contract declares" },
        no_lifecycle_section = { |c: &mut Contract| c.lifecycle = None, "needs a lifecycle section" },
        unpinned_source = { |c: &mut Contract| c.lifecycle.as_mut().unwrap().source.image = "ghcr.io/krabka-io/krabka-broker:v1.0.5".into(), "pinned by digest" },
        source_without_metadata_version = { |c: &mut Contract| c.lifecycle.as_mut().unwrap().source.supports.clear(), "must name metadata.version" },
        inverted_source_range = { |c: &mut Contract| c.lifecycle.as_mut().unwrap().source.supports.get_mut("metadata.version").unwrap().min = 31, "is inverted" },
        no_upgrade = { |c: &mut Contract| c.faults[0].fault = FaultKind::Roll { to: Side::Source }, "at least one broker to the target" },
        repeating_transition = { |c: &mut Contract| c.faults[0].every = Some(minutes(10)), "happens once" },
        transition_by_role = { |c: &mut Contract| c.faults[0].target = Target::Leader, "target a broker by index" },
        rotation_without_deadline = { |c: &mut Contract| c.deadlines.certificate_rotation = None, "deadlines.certificate_rotation" },
        reassignment_without_deadline = { |c: &mut Contract| c.deadlines.reassignment = None, "deadlines.reassignment" },
        reassignment_of_no_path = { |c: &mut Contract| c.faults[2].fault = FaultKind::Reassignment { path: "nope".into() }, "which is no path" },
        reassignment_without_a_spare_broker = { |c: &mut Contract| { c.cluster.brokers = 3; c.faults[2].target = Target::Broker(2); }, "already holds a replica" },
        lifecycle_with_injection = { |c: &mut Contract| c.self_check = fixture::self_check().self_check, "may not inject" },
    )]
    fn an_unrunnable_lifecycle_contract_is_refused(edit: fn(&mut Contract), fragment: &str) {
        let mut contract = fixture::lifecycle();
        edit(&mut contract);
        assert_refused(&contract, fragment);
    }

    #[test]
    fn a_contract_without_lifecycle_deadlines_keeps_its_digest() {
        let encoded = serde_json::to_string(&fixture::self_check()).unwrap();
        check!(!encoded.contains("lifecycle"));
        check!(!encoded.contains("certificate_rotation"));
        check!(!encoded.contains("reassignment"));
        check!(!encoded.contains("broker_session_timeout"));
    }

    #[test]
    fn a_failover_pause_outlasts_both_controller_and_broker_timeouts() {
        let mut contract = fixture::self_check();
        contract.cluster.broker_session_timeout = Some(secs(5));
        contract.faults[0].fault = FaultKind::Pause {
            intends_failover: true,
        };
        contract.faults[0].target = Target::Controller;
        for (duration, refused) in [(secs(7), true), (secs(8), false)] {
            contract.faults[0].duration = duration;
            check!(
                messages(&contract)
                    .iter()
                    .any(|message| message.contains("broker session timeout"))
                    == refused
            );
        }
        for timeout in [secs(0), secs(1), secs(3)] {
            contract.cluster.broker_session_timeout = Some(timeout);
            assert_refused(&contract, "broker_session_timeout must exceed");
        }
    }

    #[test]
    fn a_soak_must_cover_every_path_the_milestone_names() {
        let mut contract = fixture::self_check();
        contract.profile = Profile::Soak;
        contract.self_check = None;
        let messages = messages(&contract);
        for missing in ["a compacted path", "a tiered topic", "a diskless topic"] {
            check!(
                messages.iter().any(|message| message.ends_with(missing)),
                "{missing} not reported in {messages:?}"
            );
        }
        check!(
            !messages
                .iter()
                .any(|message| message.contains("a classic topic"))
        );
        check!(
            !messages
                .iter()
                .any(|message| message.contains("read_committed consumer"))
        );
        check!(
            contract
                .paths
                .iter()
                .all(|path| path.storage == Storage::Classic)
        );
    }

    #[test]
    fn a_soak_without_faults_is_refused() {
        let mut contract = fixture::self_check();
        contract.profile = Profile::Soak;
        contract.self_check = None;
        contract.faults.clear();
        assert_refused(&contract, "at least one fault plan");
    }

    #[yare::parameterized(
        quiet_pause = { FaultKind::Pause { intends_failover: false }, Target::Follower, &[RecoveryCheck::CatchUpAfterPause] },
        failover_pause = { FaultKind::Pause { intends_failover: true }, Target::Leader, &[RecoveryCheck::LeaderFailover, RecoveryCheck::CatchUpAfterPause] },
        controller_pause = { FaultKind::Pause { intends_failover: true }, Target::Controller, &[RecoveryCheck::LeaderFailover, RecoveryCheck::CatchUpAfterPause, RecoveryCheck::ControllerFailover] },
        restart = { FaultKind::Restart { graceful: true }, Target::Any, &[RecoveryCheck::LeaderFailover, RecoveryCheck::IsrRejoin] },
        controller_restart = { FaultKind::Restart { graceful: false }, Target::Controller, &[RecoveryCheck::LeaderFailover, RecoveryCheck::IsrRejoin, RecoveryCheck::ControllerFailover] },
        quiet_controller_pause = { FaultKind::Pause { intends_failover: false }, Target::Controller, &[RecoveryCheck::CatchUpAfterPause] },
        disk = { FaultKind::DiskPressure { watermark: fraction(0.5) }, Target::Any, &[RecoveryCheck::IsrRejoin] },
        roll = { FaultKind::Roll { to: Side::Target }, Target::Broker(0), &[RecoveryCheck::LeaderFailover, RecoveryCheck::IsrRejoin] },
        rotation = { FaultKind::CertificateRotation, Target::Broker(0), &[RecoveryCheck::CertificateServed] },
        reassignment = { FaultKind::Reassignment { path: "p".into() }, Target::Broker(3), &[RecoveryCheck::ReassignmentComplete] },
    )]
    fn each_fault_kind_names_the_recoveries_it_is_measured_for(
        kind: FaultKind,
        target: Target,
        expected: &[RecoveryCheck],
    ) {
        check!(kind.checks(target) == expected);
    }

    #[test]
    fn the_drill_fixture_is_valid() {
        check!(messages(&fixture::drill()).is_empty());
    }

    #[test]
    fn a_contract_without_objectives_encodes_none_so_its_digest_is_unchanged() {
        let encoded = serde_json::to_string(&fixture::self_check()).unwrap();
        check!(!encoded.contains("disaster_recovery"));
        let encoded = serde_json::to_string(&fixture::drill()).unwrap();
        check!(encoded.contains("disaster_recovery"));
    }

    fn classic(contract: &mut Contract) { contract.paths[0].storage = Storage::Classic; }

    fn transactional(contract: &mut Contract) {
        contract.paths[0].semantics = Semantics::Transactional;
    }

    fn short_rpo(contract: &mut Contract) {
        if let Some(objectives) = &mut contract.disaster_recovery {
            objectives.rpo = secs(5);
        }
    }

    fn long_post_capture_load(contract: &mut Contract) {
        if let Some(objectives) = &mut contract.disaster_recovery {
            objectives.post_capture_load = contract.duration;
            objectives.rpo = contract.duration;
        }
    }

    fn no_objectives(contract: &mut Contract) { contract.disaster_recovery = None; }

    fn a_fault(contract: &mut Contract) {
        contract.faults.push(FaultPlan {
            name: "pause".into(),
            fault: FaultKind::Pause {
                intends_failover: false,
            },
            target: Target::Follower,
            first_at: secs(30),
            every: None,
            jitter: Time::ZERO,
            duration: secs(1),
        });
    }

    fn no_diskless(contract: &mut Contract) { contract.paths[1].storage = Storage::Tiered; }

    fn no_commits(contract: &mut Contract) {
        for path in &mut contract.paths {
            path.commit_offsets = false;
        }
    }

    fn objectives_on_a_soak(contract: &mut Contract) {
        let objectives = contract.disaster_recovery.clone();
        *contract = fixture::self_check();
        contract.disaster_recovery = objectives;
    }

    #[yare::parameterized(
        classic_is_outside_the_boundary = { classic, "outside the backup boundary" },
        transactions_are_not_captured = { transactional, "must be idempotent" },
        rpo_below_the_post_capture_load = { short_rpo, "no run could meet it" },
        post_capture_load_past_the_run = { long_post_capture_load, "must be shorter than the run" },
        objectives_missing = { no_objectives, "needs a disaster_recovery section" },
        faults_declared = { a_fault, "only fault is the loss" },
        diskless_missing = { no_diskless, "must cover a diskless topic" },
        commits_missing = { no_commits, "a path that commits offsets" },
        objectives_on_another_profile = { objectives_on_a_soak, "only a disaster_recovery contract" },
    )]
    fn a_drill_is_refused_when_it_cannot_be_judged(edit: fn(&mut Contract), fragment: &str) {
        let mut contract = fixture::drill();
        edit(&mut contract);
        assert_refused(&contract, fragment);
    }
}
