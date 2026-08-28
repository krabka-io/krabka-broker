//! Broker-side Prometheus metrics.
//!
//! Mirrors the operator's `telemetry` / `health` pattern: a shared
//! `Registry` is wrapped in `Arc<Mutex<…>>` so hot-path counters can
//! be looked up without holding the registry lock. The
//! [`BrokerMetrics`] struct hands out cheap `Arc<Counter>` / `Arc<Gauge>`
//! handles that handlers and background tasks clone and increment
//! directly.
//!
//! Naming follows Prometheus convention: `krabka_broker_<subject>_<unit>`.
//! Where Kafka has a canonical JMX name, we keep the metric semantics
//! close to it (e.g. `BrokerTopicMetrics:BytesInPerSec` ↔
//! `krabka_broker_topic_bytes_in_total`), but the units convert from
//! per-second gauges to monotonic counters per Prometheus best practice
//! — operators compute rates with `rate()` at scrape time.

use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::{Arc, atomic::AtomicU64},
};

use crabka_metadata::BreakGlassAction as GatedAction;
use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder},
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

/// Latency buckets (seconds) for the per-API `request_duration_seconds`
/// histogram. Spans ~100µs (idempotent `ApiVersions`) to 10s (a slow
/// controller round-trip or a throttled admin RPC), tuned so the common
/// Produce/Fetch band (0.5ms–50ms) lands on distinct buckets.
const REQUEST_DURATION_BUCKETS: [f64; 12] = [
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 10.0,
];

/// Latency buckets (seconds) for `barrier_injection_duration_seconds`. One
/// injection appends a marker to every partition of a barrier group, and a
/// partition that another broker leads costs an inter-broker round trip. The
/// span runs from 5ms for a small single-broker group to 30s, which is the
/// default `barrier_injection_timeout`.
const BARRIER_INJECTION_DURATION_BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Latency buckets (seconds) for `delivery_activation_lateness_seconds`.
/// KFC-1 bounds activation lateness at twice the topic's declared
/// `delivery_clock_uncertainty` plus one scheduler tick, so the value an
/// operator sees is normally a few hundred milliseconds at most: the span opens
/// at 1ms and resolves the sub-second band finely. The tail runs to 30s so a
/// broker with real clock skew, or one whose scheduler is starved of CPU, still
/// lands in a bucket instead of in `+Inf`.
const DELIVERY_ACTIVATION_LATENESS_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0, 30.0,
];

/// Shared registry owning every metric the broker emits. Wrapped in
/// `Arc<Mutex<…>>` because `prometheus-client` requires `&mut Registry`
/// to register and we want lazy registration from multiple init paths.
pub type SharedRegistry = Arc<Mutex<Registry>>;

/// Sentinel label value that folds unbounded inputs (unrecognised
/// `api_key`s, `SaslAuthenticate` without a prior handshake) into one
/// series, keeping label cardinality bounded.
pub(crate) const UNKNOWN_LABEL: &str = "Unknown";

/// Per-topic label set. `EncodeLabelSet` is the prometheus-client
/// derive that produces the `topic="<name>"` label on emitted samples.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TopicLabel {
    pub topic: String,
}

/// Per-partition label set, paired with the `partition_*` and `delivery_*`
/// metric families. Consumed by the rebalancer's metric scraper. Cardinality is
/// bounded by the number of partitions this broker hosts, because both fields
/// come from the metadata image and never from client input.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PartitionLabel {
    pub topic: String,
    pub partition: i32,
}

/// Fleet-complete KIP-932 backlog for one share-group partition.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ShareGroupLabel {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
}

/// KIP-511 client software fingerprint, attached to the
/// `client_software_versions_total` counter on every accepted v3+
/// `ApiVersions` handshake.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ClientSoftwareLabel {
    pub software_name: String,
    pub software_version: String,
}

/// Per-API request fingerprint, paired with the
/// `api_requests` counter family. `api_key` is the
/// `ApiKey::IntoStaticStr`-derived variant name (e.g. `"Produce"`,
/// `"DescribeQuorum"`) so operators see human-readable api-name
/// labels. A krabka-private api key carries its own RPC name instead
/// (e.g. `"TriggerBarrier"`), because the generated enum does not
/// know that range. Cardinality is bounded by `ApiKey::ALL.len()`
/// (~80 entries) plus one label per krabka-private RPC; requests for
/// unknown api keys land under the `"Unknown"` sentinel label.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ApiKeyLabel {
    pub api_key: String,
}

/// Per-barrier-group label set, paired with the `barrier_*` families.
/// `group` is the barrier group name. Cardinality is bounded by
/// `BrokerConfig::barrier_max_groups`, because the coordinator rejects a new
/// group past that cap.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BarrierGroupLabel {
    pub group: String,
}

/// Controller directory identity attached to the current vote.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DirectoryLabel {
    pub directory_id: String,
}

/// SASL mechanism fingerprint, paired with the
/// `{successful,failed}_authentication_total` counter families.
/// `mechanism` is the canonical Kafka wire name from
/// [`krabka_security::SaslMechanism::wire_name`] (`"PLAIN"`,
/// `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`, `"OAUTHBEARER"`) when the
/// `SaslAuthenticate` frame arrived in a valid sequence; the
/// `"Unknown"` sentinel covers `ILLEGAL_SASL_STATE` rejects where
/// no prior `SaslHandshake` ran and the mechanism is unset.
/// Cardinality is bounded by `SaslMechanism::*` + 1 — a tight set
/// regardless of client population.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SaslMechanismLabel {
    pub mechanism: String,
}

/// KFC-7 rejection fingerprint, paired with the
/// `schema_validation_rejections` counter family. `reason` is one of the five
/// fixed values `unframed`, `unknown_id`, `wrong_subject`, `body_mismatch`,
/// and `registry_unavailable`, so a topic with schema validation on adds at
/// most five series. No schema id, subject, or client string reaches this
/// label set.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SchemaRejectionLabel {
    pub topic: String,
    pub reason: String,
}

/// KFC-9 state of a break-glass proposal.
///
/// The four states are the whole lifecycle of a proposal, so a closed enum
/// bounds the `break_glass_proposals` label set at four series.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum BreakGlassState {
    /// The proposal has fewer approvals than the broker needs.
    Pending,
    /// The proposal has every approval it needs and no transition used it yet.
    Approved,
    /// The proposal passed its expiry time and no transition used it.
    Expired,
    /// A privileged transition used the proposal.
    Consumed,
}

impl BreakGlassState {
    /// Every state, in lifecycle order.
    pub const ALL: [Self; 4] = [Self::Pending, Self::Approved, Self::Expired, Self::Consumed];

    /// The `state` label value this variant renders as.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Expired => "expired",
            Self::Consumed => "consumed",
        }
    }
}

impl EncodeLabelValue for BreakGlassState {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), fmt::Error> {
        EncodeLabelValue::encode(&self.as_str(), encoder)
    }
}

/// KFC-9 privileged transition that a break-glass proposal authorizes, as a
/// metric label.
///
/// [`crabka_metadata::BreakGlassAction`] is the one definition of the gated
/// set. This newtype is only what lets that definition *be* a label value:
/// both the enum and `EncodeLabelValue` are foreign to this crate, so the
/// orphan rule forbids implementing the trait on the enum directly.
///
/// The label set stays as closed as a broker-local enum made it. The wrapped
/// enum is itself closed, so the `break_glass_refusals` and
/// `break_glass_bypassed` families still carry one series per gated operation
/// and no caller can name an eighth action. What the newtype adds is that
/// there is now one enum rather than two: an action added to the metadata
/// definition cannot be forgotten here, and `break_glass::action_name` is the
/// one spelling that the configuration, the audit event and the metric label
/// all share.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakGlassAction(pub GatedAction);

impl BreakGlassAction {
    /// The `action` label value this transition renders as.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        crate::break_glass::action_name(self.0)
    }
}

/// Hashing the label text rather than the wrapped enum, which carries no
/// `Hash` of its own. Two actions are equal exactly when their names are, so
/// the hash agrees with `Eq`.
impl Hash for BreakGlassAction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl EncodeLabelValue for BreakGlassAction {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), fmt::Error> {
        EncodeLabelValue::encode(&self.as_str(), encoder)
    }
}

/// KFC-9 proposal-state label set, paired with the `break_glass_proposals`
/// gauge family. Cardinality is bounded at four, because the field is the
/// closed [`BreakGlassState`] enum and no caller can name a fifth state.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BreakGlassStateLabel {
    pub state: BreakGlassState,
}

/// KFC-9 privileged-transition label set, paired with the
/// `break_glass_refusals` and `break_glass_bypassed` counter families.
/// Cardinality is bounded at seven, because the field wraps the closed
/// [`crabka_metadata::BreakGlassAction`] enum and no caller can name an eighth
/// action.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BreakGlassActionLabel {
    pub action: BreakGlassAction,
}

/// Cheaply-clonable bundle of counter / gauge handles. Construct once
/// in `Broker::start`; hand out clones (each clone is a single
/// `Arc::clone`) to every subsystem that emits.
#[derive(Clone)]
pub struct BrokerMetrics {
    pub registry: SharedRegistry,
    pub topic_bytes_in: Family<TopicLabel, Counter>,
    pub topic_bytes_out: Family<TopicLabel, Counter>,
    /// Cumulative count of records received from producers,
    /// per topic. Sums `RecordBatch.records.len()` for every batch on
    /// the Produce path. Mirrors Kafka's
    /// `BrokerTopicMetrics.MessagesInPerSec`; pairs with
    /// `topic_bytes_in` to surface both volume and message rate.
    /// Legacy (v0/v1) producers don't contribute — `RecordsPayload`
    /// keeps their bytes opaque until the v2 conversion, so we
    /// count there. The accompanying `produce_message_conversions`
    /// counter still tracks how often legacy batches arrive, so
    /// operators can detect under-counting from a legacy fleet.
    pub topic_messages_in: Family<TopicLabel, Counter>,
    pub topic_produce_requests: Family<TopicLabel, Counter>,
    pub topic_fetch_requests: Family<TopicLabel, Counter>,
    /// Per-topic counter of Produce partition responses
    /// that carried a non-zero error code. Mirrors Kafka's
    /// `BrokerTopicMetrics.FailedProduceRequestsPerSec`. Incremented
    /// once per failed partition (matching the JVM's per-row mark),
    /// so a request whose two partitions both fail bumps the topic
    /// counter by 2. Topic-level authorization denials and
    /// unknown-topic responses count, mirroring JVM behavior.
    pub topic_failed_produce_requests: Family<TopicLabel, Counter>,
    /// Per-topic counter of Fetch partition responses
    /// that carried a non-zero error code. Mirrors Kafka's
    /// `BrokerTopicMetrics.FailedFetchRequestsPerSec`. Pairs with
    /// `topic_fetch_requests` to surface error rate.
    pub topic_failed_fetch_requests: Family<TopicLabel, Counter>,
    pub partition_bytes_in: Family<PartitionLabel, Counter>,
    pub partition_bytes_out: Family<PartitionLabel, Counter>,
    /// Cumulative bytes this broker accepted from a partition
    /// leader as a follower (`Fetch(replica_id >= 0)` round-trip). Mirrors
    /// Kafka's `BrokerTopicMetrics.replicationBytesInPerSec`. Operators
    /// graph `rate(replication_bytes_in_total[1m])` to spot ISR fall-behind
    /// caused by ingest, not by client read load.
    pub replication_bytes_in: Family<PartitionLabel, Counter>,
    /// Cumulative bytes this broker served *to* a follower
    /// (i.e. the leader-side outbound for inter-broker `Fetch`). Mirrors
    /// Kafka's `BrokerTopicMetrics.replicationBytesOutPerSec`. Operators
    /// graph the per-partition rate to attribute leader outbound to
    /// followers vs. consumers (the latter still rolls up to
    /// `partition_bytes_out`).
    pub replication_bytes_out: Family<PartitionLabel, Counter>,
    pub partition_disk_bytes: Family<PartitionLabel, Gauge>,
    /// Records waiting for acquisition in each share-group partition.
    pub share_group_backlog: Family<ShareGroupLabel, Gauge>,
    /// Cumulative handler-thread microseconds spent processing each
    /// (topic, partition). Exported as
    /// `krabka_broker_partition_cpu_micros_total`. Rebalancer takes
    /// `rate(...)` to get micros/sec; dividing by `1_000_000` yields the
    /// per-partition core occupancy. We track microseconds (integer
    /// counter) rather than seconds (float) because `prometheus-client`
    /// counters are `u64`.
    pub partition_cpu_micros: Family<PartitionLabel, Counter>,
    pub partitions_led: Gauge,
    /// Total number of partitions (leader + follower
    /// replicas) this broker hosts. Mirrors Kafka's
    /// `ReplicaManager.PartitionCount`. Sampled in the same per-second
    /// tick as `partitions_led`.
    pub partitions_total: Gauge,
    /// Count of partitions this broker leads whose ISR is
    /// smaller than the assigned replica set — Kafka's
    /// `ReplicaManager.UnderReplicatedPartitions`. Sampled by reading
    /// the current `MetadataImage` and matching partitions where this
    /// broker is the leader. Operators alert on
    /// `under_replicated_partitions > 0` to spot stuck followers
    /// before they fail an unclean election.
    pub under_replicated_partitions: Gauge,
    /// Count of partitions this broker leads whose ISR is
    /// strictly less than the topic's `min.insync.replicas`. Mirrors
    /// Kafka's `ReplicaManager.UnderMinIsrPartitionCount`. Operators
    /// alert on `under_min_isr_partition_count > 0`: partitions in
    /// this state reject `acks=all` produces with
    /// `NOT_ENOUGH_REPLICAS`, so the metric
    /// surfaces "writes are blocked" before clients start retrying.
    pub under_min_isr_partition_count: Gauge,
    /// Count of partitions this broker leads that
    /// currently have no live leader (leader broker dead with no
    /// eligible ISR replacement). Mirrors Kafka's
    /// `ReplicaManager.OfflinePartitionsCount`. Operators alert on
    /// `> 0`: such partitions are wholly unavailable until an ISR
    /// member returns or an unclean election runs.
    pub offline_partitions_count: Gauge,
    pub active_controller: Gauge,
    /// Configured static voters ignored after `kraft.version` reaches 1.
    pub ignored_static_voters: Gauge,
    /// 1 when this node carries the data-bearing witness role, 0 otherwise.
    /// The value comes from the `broker.witness` config in the metadata
    /// image, not from the local flag, so it confirms that the role reached
    /// the controller. An operator reads it to see that the role took effect
    /// on the node they meant to configure.
    pub witness_role: Gauge,
    /// Count of partitions this broker leads from a site other than the
    /// stretch cluster's preferred leader site. It stays at zero on a cluster
    /// that pins leadership to no site. Operators alert on
    /// `leader_site_drift_partitions > 0` to catch leadership that drifted,
    /// such as a failover that no rebalance has undone yet.
    pub leader_site_drift_partitions: Gauge,
    /// One-hot series for the directory identity voted for in this epoch.
    pub voted_directory: Family<DirectoryLabel, Gauge>,
    /// Cumulative count of distinct controller-leader
    /// transitions this broker has observed (any change in the raft
    /// leader, including this broker becoming or ceasing to be
    /// leader). Mirrors Kafka's
    /// `KafkaController.LeaderElectionRateAndTimeMs`. Operators alert
    /// on `rate(controller_leader_changes_total[5m]) > 0` for sustained
    /// periods to spot flapping raft leadership.
    pub controller_leader_changes_total: Counter,
    pub isr_shrinks_total: Counter,
    pub isr_expands_total: Counter,
    /// KIP-227: current count of live incremental-fetch sessions across the
    /// per-broker cache. Sampled periodically from `FetchSessionCache::len()`.
    pub incremental_fetch_sessions: Gauge,
    /// KIP-227: cumulative count of incremental-fetch sessions evicted to
    /// make room for a new allocation. Incremented inside the cache.
    pub incremental_fetch_session_evictions_total: Counter,
    /// KIP-227: sum of `session.partitions.len()` across every live session.
    /// Sampled periodically alongside `incremental_fetch_sessions`.
    pub incremental_fetch_partitions_cached: Gauge,
    /// KIP-511: per-(name, version) counter of accepted v3+ `ApiVersions`
    /// handshakes. Operators graph this to see which client libraries
    /// and versions are connecting.
    pub client_software_versions: Family<ClientSoftwareLabel, Counter>,
    /// Cumulative count of completed `SaslAuthenticate`
    /// frames per mechanism that ended in a successful auth state
    /// transition. Mirrors Kafka's
    /// `kafka.network:type=Selector,name=successful-authentication-total`.
    /// Paired with `failed_authentication` so operators compute the
    /// auth failure ratio per mechanism at scrape time.
    pub successful_authentication: Family<SaslMechanismLabel, Counter>,
    /// Cumulative count of `SaslAuthenticate` frames per
    /// mechanism that returned a non-zero error code. Mirrors
    /// Kafka's `failed-authentication-total`. The `"Unknown"`
    /// mechanism label covers `ILLEGAL_SASL_STATE` rejects where
    /// the connection sent `SaslAuthenticate` without first
    /// completing a `SaslHandshake`; per-mechanism failures land
    /// under the canonical wire name (`PLAIN`, `SCRAM-SHA-256`,
    /// `SCRAM-SHA-512`, `OAUTHBEARER`).
    pub failed_authentication: Family<SaslMechanismLabel, Counter>,
    /// Per-Kafka-API request counter. Bumped once per
    /// dispatched request from the network dispatcher, labelled by
    /// the `ApiKey` variant name (or `"Unknown"` for unrecognised
    /// keys). Mirrors Kafka's `RequestMetrics.RequestsPerSec`; rate(...)
    /// gives operators per-API request throughput across the
    /// broker without needing to slice the dashboard by handler.
    pub api_requests: Family<ApiKeyLabel, Counter>,
    /// Per-Kafka-API counter of requests the dispatcher
    /// answered with the synthetic `UNSUPPORTED_VERSION` response
    /// because no handler matched the `api_key` (or, for unknown
    /// `api_key`s, the dispatcher didn't recognise the key at all).
    /// Operators alert on `rate(unsupported_api_requests_total[5m]) > 0`
    /// to catch clients on `api_key`/version pairs the broker
    /// doesn't speak — frequently the smoking gun for upgrade-skew
    /// or misconfigured clients.
    pub unsupported_api_requests: Family<ApiKeyLabel, Counter>,
    /// Per-Kafka-API request-handling latency in seconds
    /// (`krabka_broker_request_duration_seconds{api}`). Observed in the
    /// dispatch path around the full handler round-trip (decode → handle →
    /// encode) for every dispatched frame, labelled by the `ApiKey`
    /// variant name. Operators graph
    /// `histogram_quantile(0.99, rate(request_duration_seconds_bucket[5m]))`
    /// per api to spot handler tail-latency regressions, and use `_count`
    /// as a request-rate stream that pairs with `api_requests`.
    pub request_duration_seconds: Family<ApiKeyLabel, Histogram>,
    /// Number of requests currently being handled by this
    /// broker (gauge). Incremented on dispatch entry, decremented on exit
    /// (including the error/close path). A sustained climb signals handler
    /// stalls or a wedged downstream (controller / replication).
    pub in_flight_requests: Gauge,
    /// Number of client connections currently open to this
    /// broker (gauge). Incremented when a connection is accepted and the
    /// per-connection serve loop starts, decremented when that loop exits
    /// (EOF, error, or SASL-session expiry). Mirrors Kafka's
    /// `kafka.network:type=Acceptor` connection-count intent.
    pub active_connections: Gauge,
    /// Per-Kafka-API counter of requests whose handler
    /// returned an error (the dispatcher closed the connection). Labelled
    /// by the `ApiKey` variant name; disjoint from
    /// `unsupported_api_requests` (which counts the synthetic
    /// `UNSUPPORTED_VERSION` arm). Operators alert on
    /// `rate(request_errors_total[5m]) > 0` to catch handler-level faults.
    pub request_errors: Family<ApiKeyLabel, Counter>,
    /// KIP-405: `1` when this broker has finished swapping in
    /// the topic-backed `RemoteLogMetadataManager` and is
    /// answering metadata queries from the durable
    /// `__remote_log_metadata` topic; `0` while still on the
    /// fail-closed `NotReadyRlmm` placeholder (the default until a
    /// configured `[remote_storage.kafka_metadata]` bootstrap completes).
    /// Operators alert on
    /// `min_over_time(tiered_storage_rlmm_topic_backed[5m]) == 0`
    /// against clusters that asked for `metadataManager: Topic` to catch
    /// a stuck bootstrap.
    pub tiered_storage_rlmm_topic_backed: Gauge,
    /// Number of topic-backed RLMM bootstrap attempts; climbs while stuck
    /// retrying, flat once `tiered_storage_rlmm_topic_backed` flips to 1.
    pub tiered_storage_rlmm_bootstrap_attempts: Counter,
    /// Per-topic counter of v0/v1 → v2 record-batch
    /// up-conversions on the Produce path. Mirrors Kafka's
    /// `BrokerTopicMetrics.ProduceMessageConversionsPerSec`. Bumped
    /// once per partition's slice of a Produce request whose
    /// `records` field arrived as a legacy `MessageSet`.
    pub produce_message_conversions: Family<TopicLabel, Counter>,
    /// Per-topic counter of v2 → v0/v1 record-batch
    /// down-conversions on the Fetch path. Mirrors Kafka's
    /// `BrokerTopicMetrics.FetchMessageConversionsPerSec`. Bumped
    /// once per partition's slice of a Fetch response whose response
    /// payload was down-converted to satisfy a legacy (`Fetch v < 4`)
    /// client.
    pub fetch_message_conversions: Family<TopicLabel, Counter>,
    /// KIP-841: cumulative count of unclean leader
    /// elections this broker, as controller leader, has driven —
    /// i.e. elections that picked an out-of-ISR replica as the new
    /// leader because the topic had
    /// `unclean.leader.election.enable=true` and the ISR was empty
    /// at failover time. Mirrors Kafka's
    /// `ControllerStats.UncleanLeaderElectionsPerSec`. An operator
    /// alert on `rate(unclean_leader_elections_total[5m]) > 0`
    /// flags the data-loss footgun.
    pub unclean_leader_elections_total: Counter,
    /// `FedRAMP` MLA: cumulative audit records successfully written to the
    /// audit topic. Incremented by the audit subsystem on each successful
    /// produce to `__krabka_audit`.
    pub audit_events_total: Counter,
    /// `FedRAMP` MLA: cumulative audit records that failed to write to the
    /// audit topic. Incremented on each produce error; operators alert on
    /// `rate(audit_write_failures_total[5m]) > 0`.
    pub audit_write_failures_total: Counter,
    /// Current count of audit records buffered in the durable spool (gauge).
    pub audit_spool_depth: Gauge,
    /// Current bytes buffered in the durable audit spool (gauge).
    pub audit_spool_bytes: Gauge,
    /// Cumulative audit records diverted to the spool on topic-write failure.
    pub audit_records_spooled_total: Counter,
    /// Cumulative audit records drained from the spool back to the topic.
    pub audit_records_replayed_total: Counter,
    /// Cumulative audit records lost (channel-full or spool-full).
    pub audit_records_dropped_total: Counter,
    /// KIP-714 client-metric batches dropped because the bounded OTLP queue
    /// was full or closed.
    pub client_metrics_otlp_dropped_total: Counter,
    /// KIP-714 client-metric export attempts rejected by the collector or
    /// failed at the transport layer.
    pub client_metrics_otlp_failed_total: Counter,
    /// Cumulative count of completed log-compaction sweeps run by this
    /// broker's cleaner — one increment per `tick_all` pass, whether or
    /// not any partition was eligible. Lets tests (and operators) observe
    /// that the compaction ticker has completed at least one full pass
    /// after a segment was sealed, replacing fixed `sleep`s with a poll on
    /// this counter. Mirrors the intent of Kafka's `LogCleaner` run
    /// accounting.
    pub log_cleaner_runs_total: Counter,
    /// Per-partition cumulative count of compaction passes
    /// (`Partition::compact_log`) this broker's cleaner completed
    /// successfully. Bumped once per eligible (leader &&
    /// `cleanup.policy=compact`) partition per sweep. Pairs with
    /// `log_cleaner_runs_total`: a test that seals a segment then waits for
    /// this counter to advance knows the sealed segment has been through a
    /// compaction pass without guessing a duration.
    pub log_compactions_total: Family<PartitionLabel, Counter>,
    /// Per-group count of barrier epochs the coordinator started. It
    /// increments when the coordinator writes the injection-start record that
    /// freezes the target set, before it appends the first marker.
    pub barrier_epochs_started_total: Family<BarrierGroupLabel, Counter>,
    /// Per-group count of barrier epochs that reached every partition of the
    /// group. The coordinator published a complete cut for each one.
    pub barrier_epochs_committed_total: Family<BarrierGroupLabel, Counter>,
    /// Per-group count of barrier epochs whose cut names at least one
    /// partition that got no marker. The coordinator publishes the partial cut
    /// and consumes the epoch, so
    /// `rate(barrier_epochs_published_partial_total[5m]) > 0` is the alert an
    /// operator sets on a group that does not reach all of its partitions.
    pub barrier_epochs_published_partial_total: Family<BarrierGroupLabel, Counter>,
    /// Per-group wall-clock seconds from the injection-start record to the
    /// published cut. Operators graph
    /// `histogram_quantile(0.99, rate(..._bucket[5m]))` against
    /// `barrier_injection_timeout` to see how much headroom a group has.
    pub barrier_injection_duration_seconds: Family<BarrierGroupLabel, Histogram>,
    /// Per-group epoch of the newest cut this coordinator published (gauge).
    /// A flat value beside a live `barrier_min_injection_interval` says that
    /// injection stopped.
    pub barrier_latest_epoch: Family<BarrierGroupLabel, Gauge>,
    /// Per-topic count of barrier markers this broker appended, across every
    /// group and every partition it leads. Markers survive compaction, so this
    /// counter also tracks the control batches that accumulate in a compacted
    /// topic.
    pub barrier_markers_written_total: Family<TopicLabel, Counter>,
    /// Number of barrier groups this broker coordinates (gauge). It is zero on
    /// a broker that leads no `__barrier_state` partition.
    pub barrier_groups_coordinated: Gauge,
    /// KFC-1 deliver-at-time watermark of each scheduled partition this broker
    /// leads (gauge): the first offset that is not visible to a consumer yet.
    /// Read against `partition_disk_bytes` or the log end offset to see how far
    /// visibility trails durability. Cardinality is bounded by the number of
    /// partitions this broker leads whose topic sets
    /// `delivery.mode=scheduled`; an ordinary partition never creates a series,
    /// because the scheduler drops it before it reports.
    pub delivery_watermark: Family<PartitionLabel, Gauge>,
    /// KFC-1 records of each scheduled partition that are durable but not
    /// visible yet (gauge): the log end offset minus
    /// `delivery_watermark`. A value that grows without falling is a schedule
    /// whose head-of-line record is far in the future. Cardinality is bounded
    /// exactly as `delivery_watermark` is.
    pub delivery_pending_records: Family<PartitionLabel, Gauge>,
    /// KFC-1 seconds from a batch's activation deadline to the moment the
    /// broker first made it visible.
    ///
    /// The deadline is the record timestamp plus the topic's declared
    /// `delivery_clock_uncertainty`, so this histogram measures the delay
    /// *beyond* the bound the operator declared, and a healthy broker reports
    /// values at zero. Add `delivery_clock_uncertainty` to read the delay from
    /// the record's own delivery time. A rising tail says the declared bound is
    /// not honest, or that the scheduler is starved of CPU. It carries no
    /// labels, so it is one series per broker.
    pub delivery_activation_lateness_seconds: Histogram,
    /// KFC-1 cumulative count of delivery-scheduler wakeups, whether a deadline
    /// came due, a produce re-armed the task, or its idle bound elapsed. Paired
    /// with the lateness histogram it separates "the scheduler never ran" from
    /// "the scheduler ran late". One series per broker.
    pub delivery_scheduler_wakeups_total: Counter,
    /// KFC-7 cumulative count of records the broker rejected because they
    /// failed schema validation, per topic and reason.
    ///
    /// The broker bumps it once per rejected record, so a Produce request with
    /// three bad records adds 3. An operator reads the split by reason during
    /// a rollout: a run of `unframed` is a producer that never used a
    /// serializer, and a run of `wrong_subject` is a producer that writes the
    /// right format to the wrong topic.
    pub schema_validation_rejections: Family<SchemaRejectionLabel, Counter>,
    /// KFC-7 cumulative count of schema lookups the broker answered from its
    /// local cache. It carries no labels, so it is one series per broker.
    pub schema_validation_cache_hits: Counter,
    /// KFC-7 cumulative count of schema lookups that cost a registry round
    /// trip on the produce path.
    ///
    /// Paired with `schema_validation_cache_hits` it gives the hit rate, and
    /// the hit rate is what says whether this feature costs anything at steady
    /// state. It carries no labels, so it is one series per broker.
    pub schema_validation_cache_misses: Counter,
    /// KFC-8 the clock bound this broker declares, in seconds.
    ///
    /// It is `delivery_clock_uncertainty`, the bound KFC-1 adds to a batch's
    /// timestamp before the batch activates. The value is a constant of the
    /// running process, and it is broker-wide: no topic config overrides it.
    ///
    /// The broker exports it so an alert can compare measured clock
    /// uncertainty against the bound the broker actually relies on. Without
    /// this series a rule has to carry a copy of the threshold, and the copy
    /// goes stale the moment an operator retunes the broker.
    pub delivery_clock_uncertainty_seconds: Gauge<f64, AtomicU64>,
    /// KFC-9 cumulative count of Produce partition rows the broker refused
    /// because a freeze covers the topic.
    ///
    /// The gate sits before the batch is parsed, so a refused row costs no CRC
    /// check and moves no log end offset. The broker bumps this counter once
    /// per refused row, so one request that names three partitions of a frozen
    /// topic adds 3.
    ///
    /// Cardinality is bounded by the number of topics a freeze covers, and
    /// that is at most the number of topics the cluster holds. This is the
    /// bound the other per-topic families here already accept. A client
    /// cannot invent a series, because the label comes from a topic name that
    /// resolved in the metadata image.
    pub topic_freeze_rejections: Family<TopicLabel, Counter>,
    /// KFC-9 live entries in the freeze registry (gauge).
    ///
    /// It counts registry entries and not frozen topics: one prefix entry
    /// covers a whole namespace. The value falls when a thaw removes an entry,
    /// and `freeze.max_entries` caps it. It carries no labels, so it is one
    /// series per broker.
    pub topic_freezes_active: Gauge,
    /// KFC-9 break-glass proposals by state (gauge).
    ///
    /// A proposal moves through the states of [`BreakGlassState`], so a rise
    /// in `pending` beside a flat `approved` is an incident where the second
    /// person has not answered yet.
    pub break_glass_proposals: Family<BreakGlassStateLabel, Gauge>,
    /// KFC-9 cumulative count of privileged transitions the broker refused
    /// because no approved break-glass proposal covers them, per action.
    ///
    /// A refusal is the expected answer when an operator runs the tool before
    /// the approval lands, so a steady rate here is normal.
    pub break_glass_refusals: Family<BreakGlassActionLabel, Counter>,
    /// KFC-9 cumulative count of privileged transitions that ran **without**
    /// an approved break-glass proposal, per action.
    ///
    /// This is the series to alert on. It counts data-losing transitions that
    /// no second person approved: the background unclean-recovery path has no
    /// caller to refuse, so `break_glass.background_unclean_recovery =
    /// "audit-only"` lets recovery run and bumps this counter instead. Any
    /// non-zero rate is an unclean recovery that took the cluster past the
    /// two-person rule, and an operator should read the audit log for the
    /// partition it names.
    pub break_glass_bypassed: Family<BreakGlassActionLabel, Counter>,
}

impl BrokerMetrics {
    fn unregistered(registry: SharedRegistry) -> Self {
        Self {
            registry,
            topic_bytes_in: Family::default(),
            topic_bytes_out: Family::default(),
            topic_messages_in: Family::default(),
            topic_produce_requests: Family::default(),
            topic_fetch_requests: Family::default(),
            topic_failed_produce_requests: Family::default(),
            topic_failed_fetch_requests: Family::default(),
            partition_bytes_in: Family::default(),
            partition_bytes_out: Family::default(),
            replication_bytes_in: Family::default(),
            replication_bytes_out: Family::default(),
            partition_disk_bytes: Family::default(),
            share_group_backlog: Family::default(),
            partition_cpu_micros: Family::default(),
            partitions_led: Gauge::default(),
            partitions_total: Gauge::default(),
            under_replicated_partitions: Gauge::default(),
            under_min_isr_partition_count: Gauge::default(),
            offline_partitions_count: Gauge::default(),
            active_controller: Gauge::default(),
            ignored_static_voters: Gauge::default(),
            witness_role: Gauge::default(),
            leader_site_drift_partitions: Gauge::default(),
            voted_directory: Family::default(),
            controller_leader_changes_total: Counter::default(),
            isr_shrinks_total: Counter::default(),
            isr_expands_total: Counter::default(),
            incremental_fetch_sessions: Gauge::default(),
            incremental_fetch_session_evictions_total: Counter::default(),
            incremental_fetch_partitions_cached: Gauge::default(),
            client_software_versions: Family::default(),
            successful_authentication: Family::default(),
            failed_authentication: Family::default(),
            api_requests: Family::default(),
            unsupported_api_requests: Family::default(),
            request_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(REQUEST_DURATION_BUCKETS)
            }),
            in_flight_requests: Gauge::default(),
            active_connections: Gauge::default(),
            request_errors: Family::default(),
            tiered_storage_rlmm_topic_backed: Gauge::default(),
            tiered_storage_rlmm_bootstrap_attempts: Counter::default(),
            produce_message_conversions: Family::default(),
            fetch_message_conversions: Family::default(),
            unclean_leader_elections_total: Counter::default(),
            audit_events_total: Counter::default(),
            audit_write_failures_total: Counter::default(),
            audit_spool_depth: Gauge::default(),
            audit_spool_bytes: Gauge::default(),
            audit_records_spooled_total: Counter::default(),
            audit_records_replayed_total: Counter::default(),
            audit_records_dropped_total: Counter::default(),
            client_metrics_otlp_dropped_total: Counter::default(),
            client_metrics_otlp_failed_total: Counter::default(),
            log_cleaner_runs_total: Counter::default(),
            log_compactions_total: Family::default(),
            barrier_epochs_started_total: Family::default(),
            barrier_epochs_committed_total: Family::default(),
            barrier_epochs_published_partial_total: Family::default(),
            barrier_injection_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(BARRIER_INJECTION_DURATION_BUCKETS)
            }),
            barrier_latest_epoch: Family::default(),
            barrier_markers_written_total: Family::default(),
            barrier_groups_coordinated: Gauge::default(),
            delivery_watermark: Family::default(),
            delivery_pending_records: Family::default(),
            delivery_activation_lateness_seconds: Histogram::new(
                DELIVERY_ACTIVATION_LATENESS_BUCKETS,
            ),
            delivery_scheduler_wakeups_total: Counter::default(),
            schema_validation_rejections: Family::default(),
            schema_validation_cache_hits: Counter::default(),
            schema_validation_cache_misses: Counter::default(),
            delivery_clock_uncertainty_seconds: Gauge::default(),
            topic_freeze_rejections: Family::default(),
            topic_freezes_active: Gauge::default(),
            break_glass_proposals: Family::default(),
            break_glass_refusals: Family::default(),
            break_glass_bypassed: Family::default(),
        }
    }

    fn register_group_1(&self, registry: &mut Registry) {
        registry.register(
            "topic_bytes_in",
            "Bytes received from producers, per topic (cumulative). \
             Operators compute throughput via rate(...).",
            self.topic_bytes_in.clone(),
        );

        registry.register(
            "topic_bytes_out",
            "Bytes delivered to fetchers, per topic (cumulative).",
            self.topic_bytes_out.clone(),
        );

        registry.register(
            "messages_in",
            "Cumulative count of records received from \
             producers, per topic. Mirrors Kafka's \
             BrokerTopicMetrics.MessagesInPerSec. Legacy v0/v1 \
             produce payloads are not counted (their per-record body \
             stays opaque on the Produce path); the paired \
             produce_message_conversions counter tracks the \
             legacy-arrival rate so operators can detect \
             under-counting.",
            self.topic_messages_in.clone(),
        );

        registry.register(
            "topic_produce_requests",
            "Produce requests handled, per topic (cumulative). One \
             increment per topic per Produce request.",
            self.topic_produce_requests.clone(),
        );

        registry.register(
            "topic_fetch_requests",
            "Fetch requests handled, per topic (cumulative). One \
             increment per topic per Fetch request.",
            self.topic_fetch_requests.clone(),
        );

        registry.register(
            "topic_failed_produce_requests",
            "Cumulative count of Produce partition \
             responses that returned a non-zero error code, per \
             topic. Mirrors Kafka's \
             BrokerTopicMetrics.FailedProduceRequestsPerSec. \
             Operators alert on rate(...) > 0 to catch quota / ACL \
             / NOT_ENOUGH_REPLICAS storms; the ratio against \
             topic_produce_requests yields the per-topic error rate.",
            self.topic_failed_produce_requests.clone(),
        );

        registry.register(
            "topic_failed_fetch_requests",
            "Cumulative count of Fetch partition \
             responses that returned a non-zero error code, per \
             topic. Mirrors Kafka's \
             BrokerTopicMetrics.FailedFetchRequestsPerSec. Pairs \
             with topic_fetch_requests for per-topic error rate.",
            self.topic_failed_fetch_requests.clone(),
        );

        registry.register(
            "partitions_led",
            "Number of partitions for which this broker is currently leader.",
            self.partitions_led.clone(),
        );

        registry.register(
            "partitions_total",
            "Total number of partitions (leader + follower \
             replicas) this broker hosts. Mirrors Kafka's \
             ReplicaManager.PartitionCount.",
            self.partitions_total.clone(),
        );

        registry.register(
            "under_replicated_partitions",
            "Count of partitions this broker leads whose ISR \
             is smaller than the assigned replica set. Mirrors Kafka's \
             ReplicaManager.UnderReplicatedPartitions; alert on > 0 \
             to spot stuck followers before they fail an unclean \
             election.",
            self.under_replicated_partitions.clone(),
        );
    }

    fn register_group_2(&self, registry: &mut Registry) {
        registry.register(
            "under_min_isr_partition_count",
            "Count of partitions this broker leads whose ISR \
             is strictly less than the topic's min.insync.replicas. \
             Mirrors Kafka's ReplicaManager.UnderMinIsrPartitionCount; \
             alert on > 0 — these partitions reject acks=all produces \
             with NOT_ENOUGH_REPLICAS.",
            self.under_min_isr_partition_count.clone(),
        );

        registry.register(
            "offline_partitions_count",
            "Count of partitions this broker leads that have \
             no live leader (leader dead with no eligible ISR \
             replacement). Mirrors Kafka's \
             ReplicaManager.OfflinePartitionsCount; alert on > 0 — \
             these partitions are wholly unavailable until an ISR \
             member returns or an unclean election runs.",
            self.offline_partitions_count.clone(),
        );

        registry.register(
            "active_controller",
            "1 if this broker is the raft (controller) leader, 0 otherwise.",
            self.active_controller.clone(),
        );

        registry.register(
            "ignored_static_voters",
            "Configured static controller voters ignored at kraft.version 1.",
            self.ignored_static_voters.clone(),
        );

        registry.register(
            "witness_role",
            "1 if this node carries the data-bearing witness role, 0 \
             otherwise. The value comes from the broker.witness config in \
             the metadata image, so it confirms that the role reached the \
             controller.",
            self.witness_role.clone(),
        );

        registry.register(
            "leader_site_drift_partitions",
            "Count of partitions this broker leads from a site other than \
             the stretch cluster's preferred leader site. It stays at zero \
             on a cluster that pins leadership to no site; alert on > 0 to \
             catch leadership that drifted away from the pinned site.",
            self.leader_site_drift_partitions.clone(),
        );

        registry.register(
            "voted_directory",
            "1 for the controller directory identity voted for in this epoch.",
            self.voted_directory.clone(),
        );

        registry.register(
            "controller_leader_changes",
            "Cumulative count of distinct controller-leader \
             transitions this broker has observed (any change in the \
             raft leader, including this broker becoming or ceasing \
             to be leader). Mirrors Kafka's \
             KafkaController.LeaderElectionRateAndTimeMs; alert on a \
             sustained rate() > 0 to spot flapping raft leadership.",
            self.controller_leader_changes_total.clone(),
        );

        registry.register(
            "isr_shrinks",
            "Cumulative count of ISR shrinks proposed by this broker's \
             ISR-maintenance loop.",
            self.isr_shrinks_total.clone(),
        );

        registry.register(
            "isr_expands",
            "Cumulative count of ISR expands proposed by this broker's \
             ISR-maintenance loop.",
            self.isr_expands_total.clone(),
        );

        registry.register(
            "partition_bytes_in",
            "Bytes received from producers, per partition (cumulative). \
             Rebalancer-targeted; rate(...) for throughput.",
            self.partition_bytes_in.clone(),
        );

        registry.register(
            "partition_bytes_out",
            "Bytes served to consumers, per partition (cumulative). \
             Rebalancer-targeted; rate(...) for throughput.",
            self.partition_bytes_out.clone(),
        );

        registry.register(
            "replication_bytes_in",
            "Bytes received from the partition leader by this broker as a \
             follower (cumulative). Rate(...) for follower throughput; \
             plotted alongside partition_bytes_in surfaces ingest vs. \
             replication-driven traffic.",
            self.replication_bytes_in.clone(),
        );

        registry.register(
            "replication_bytes_out",
            "Bytes this broker served to followers as the partition leader \
             (cumulative). Rate(...) for leader-out-to-followers throughput; \
             together with partition_bytes_out (consumer reads) it attributes \
             outbound traffic to its source.",
            self.replication_bytes_out.clone(),
        );
    }

    fn register_group_3(&self, registry: &mut Registry) {
        registry.register(
            "partition_disk_bytes",
            "On-disk size of a partition's log directory (gauge). Updated by \
             the broker's periodic disk scanner; suppress if scanner is disabled.",
            self.partition_disk_bytes.clone(),
        );

        registry.register(
            "share_group_backlog",
            "Share-group partition backlog in records, emitted by the group coordinator.",
            self.share_group_backlog.clone(),
        );

        registry.register(
            "partition_cpu_micros",
            "Cumulative handler-thread microseconds spent processing each \
             (topic, partition). Rebalancer-targeted; rate(...) divided by \
             1_000_000 yields core occupancy.",
            self.partition_cpu_micros.clone(),
        );

        registry.register(
            "incremental_fetch_sessions",
            "KIP-227: live incremental-fetch sessions cached by this broker (gauge).",
            self.incremental_fetch_sessions.clone(),
        );

        registry.register(
            "incremental_fetch_session_evictions",
            "KIP-227: cumulative count of incremental-fetch sessions evicted from \
             the cache to make room for a new allocation.",
            self.incremental_fetch_session_evictions_total.clone(),
        );

        registry.register(
            "incremental_fetch_partitions_cached",
            "KIP-227: total (topic, partition) tuples held across every live \
             incremental-fetch session (gauge).",
            self.incremental_fetch_partitions_cached.clone(),
        );

        registry.register(
            "client_software_versions",
            "KIP-511: cumulative count of accepted ApiVersions handshakes, \
             labelled by client software name and version. One increment \
             per successful v3+ ApiVersions call.",
            self.client_software_versions.clone(),
        );

        registry.register(
            "successful_authentication",
            "Cumulative count of SaslAuthenticate frames per \
             mechanism that ended in a successful auth state transition. \
             Mirrors Kafka's \
             kafka.network:type=Selector,name=successful-authentication-total. \
             Labelled by the canonical SASL mechanism wire name \
             (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER). \
             Paired with failed_authentication so rate(...) ratios \
             expose per-mechanism credential-failure rates.",
            self.successful_authentication.clone(),
        );

        registry.register(
            "failed_authentication",
            "Cumulative count of SaslAuthenticate frames per \
             mechanism that returned a non-zero error code. Mirrors \
             Kafka's failed-authentication-total. ILLEGAL_SASL_STATE \
             rejects (SaslAuthenticate without prior SaslHandshake) \
             land under the `Unknown` mechanism label.",
            self.failed_authentication.clone(),
        );

        registry.register(
            "api_requests",
            "Cumulative count of dispatched requests per \
             Kafka API key (variant name from the `ApiKey` enum, e.g. \
             Produce / Fetch / DescribeQuorum). Unknown api keys land \
             under the `Unknown` label. Mirrors Kafka's \
             RequestMetrics.RequestsPerSec; rate(...) yields per-API \
             throughput.",
            self.api_requests.clone(),
        );

        registry.register(
            "unsupported_api_requests",
            "Cumulative count of requests the dispatcher \
             answered with the synthetic UNSUPPORTED_VERSION response \
             because no handler matched the api_key. Labelled with \
             the ApiKey variant name (or `Unknown` for unrecognised \
             keys). Alert on rate(...) > 0 to catch upgrade-skew or \
             misconfigured clients.",
            self.unsupported_api_requests.clone(),
        );
    }

    fn register_group_4(&self, registry: &mut Registry) {
        registry.register(
            "request_duration_seconds",
            "Per-Kafka-API request-handling latency in \
             seconds, observed in the dispatch path around the full \
             handler round-trip (decode → handle → encode). Labelled by \
             the ApiKey variant name. Operators graph \
             histogram_quantile(0.99, rate(..._bucket[5m])) per api to \
             spot tail-latency regressions.",
            self.request_duration_seconds.clone(),
        );

        registry.register(
            "in_flight_requests",
            "Number of requests currently being handled by this broker \
             (gauge). Incremented on dispatch entry, decremented on exit; \
             a sustained climb signals handler stalls.",
            self.in_flight_requests.clone(),
        );

        registry.register(
            "active_connections",
            "Number of client connections currently open to this broker \
             (gauge). Incremented when the per-connection serve loop \
             starts, decremented when it exits (EOF / error / SASL expiry).",
            self.active_connections.clone(),
        );

        registry.register(
            "request_errors",
            "Per-Kafka-API count of requests whose handler \
             returned an error (dispatcher closed the connection). \
             Labelled by the ApiKey variant name; disjoint from \
             unsupported_api_requests. Alert on rate(...) > 0 to catch \
             handler-level faults.",
            self.request_errors.clone(),
        );

        registry.register(
            "tiered_storage_rlmm_topic_backed",
            "KIP-405: 1 when this broker is answering remote-log \
             metadata queries from the durable __remote_log_metadata topic \
             (production RLMM); 0 while still on the fail-closed \
             NotReadyRlmm placeholder. Bumped to 1 by the bootstrap task \
             after a successful SwappableRlmm swap; stays at 0 for \
             clusters that never asked for `metadataManager: Topic`.",
            self.tiered_storage_rlmm_topic_backed.clone(),
        );

        registry.register(
            "tiered_storage_rlmm_bootstrap_attempts",
            "Number of topic-backed RLMM bootstrap attempts; climbs while \
             stuck retrying, flat once tiered_storage_rlmm_topic_backed \
             flips to 1.",
            self.tiered_storage_rlmm_bootstrap_attempts.clone(),
        );

        registry.register(
            "produce_message_conversions",
            "Cumulative count of v0/v1 → v2 record-batch \
             up-conversions on the Produce path, per topic. Mirrors \
             Kafka's BrokerTopicMetrics.ProduceMessageConversionsPerSec; \
             rate(...) lets operators spot the overhead of legacy \
             producers in the cluster.",
            self.produce_message_conversions.clone(),
        );

        registry.register(
            "fetch_message_conversions",
            "Cumulative count of v2 → v0/v1 record-batch \
             down-conversions on the Fetch path, per topic. Mirrors \
             Kafka's BrokerTopicMetrics.FetchMessageConversionsPerSec; \
             rate(...) lets operators spot the overhead of legacy \
             consumers in the cluster.",
            self.fetch_message_conversions.clone(),
        );

        registry.register(
            "unclean_leader_elections",
            "KIP-841: cumulative count of unclean leader \
             elections driven by this broker (as controller leader). An \
             unclean election is one where the new leader was picked \
             from outside the ISR because the partition's ISR was empty \
             at failover time and the topic had \
             unclean.leader.election.enable=true. Each such election \
             accepts possible data loss. Mirrors Kafka's \
             ControllerStats.UncleanLeaderElectionsPerSec; an operator \
             alert on rate(unclean_leader_elections_total[5m]) > 0 \
             flags the data-loss footgun.",
            self.unclean_leader_elections_total.clone(),
        );

        registry.register(
            "audit_events_total",
            "Cumulative audit records successfully written to the audit topic",
            self.audit_events_total.clone(),
        );
    }

    fn register_group_5(&self, registry: &mut Registry) {
        registry.register(
            "audit_write_failures_total",
            "Cumulative audit records that failed to write to the audit topic",
            self.audit_write_failures_total.clone(),
        );

        registry.register(
            "audit_spool_depth",
            "Current count of audit records buffered in the durable spool",
            self.audit_spool_depth.clone(),
        );

        registry.register(
            "audit_spool_bytes",
            "Current bytes buffered in the durable audit spool",
            self.audit_spool_bytes.clone(),
        );

        registry.register(
            "audit_records_spooled",
            "Cumulative audit records diverted to the spool on topic-write failure",
            self.audit_records_spooled_total.clone(),
        );

        registry.register(
            "audit_records_replayed",
            "Cumulative audit records drained from the spool back to the topic",
            self.audit_records_replayed_total.clone(),
        );

        registry.register(
            "audit_records_dropped",
            "Cumulative audit records lost (channel-full or spool-full)",
            self.audit_records_dropped_total.clone(),
        );

        registry.register(
            "client_metrics_otlp_dropped",
            "Cumulative KIP-714 client-metric batches dropped before OTLP export",
            self.client_metrics_otlp_dropped_total.clone(),
        );
        registry.register(
            "client_metrics_otlp_failed",
            "Cumulative failed KIP-714 client-metric OTLP export attempts",
            self.client_metrics_otlp_failed_total.clone(),
        );

        registry.register(
            "log_cleaner_runs",
            "Cumulative count of completed log-compaction sweeps run by \
             this broker's cleaner (one per tick_all pass).",
            self.log_cleaner_runs_total.clone(),
        );

        registry.register(
            "log_compactions",
            "Per-partition cumulative count of compaction passes this \
             broker's cleaner completed successfully.",
            self.log_compactions_total.clone(),
        );
    }

    fn register_group_6(&self, registry: &mut Registry) {
        registry.register(
            "barrier_epochs_started",
            "Per-barrier-group cumulative count of epochs the coordinator \
             started. Bumped when it writes the injection-start record that \
             freezes the target set, before the first marker append.",
            self.barrier_epochs_started_total.clone(),
        );

        registry.register(
            "barrier_epochs_committed",
            "Per-barrier-group cumulative count of epochs whose marker \
             reached every partition of the group. The coordinator published \
             a complete cut for each one.",
            self.barrier_epochs_committed_total.clone(),
        );

        registry.register(
            "barrier_epochs_published_partial",
            "Per-barrier-group cumulative count of epochs whose cut names at \
             least one partition that got no marker. The coordinator consumes \
             the epoch either way. Alert on rate(...) > 0 to catch a group \
             that no longer reaches all of its partitions.",
            self.barrier_epochs_published_partial_total.clone(),
        );

        registry.register(
            "barrier_injection_duration_seconds",
            "Per-barrier-group wall-clock seconds from the injection-start \
             record to the published cut. Graph histogram_quantile(0.99, \
             rate(..._bucket[5m])) against barrier_injection_timeout to see \
             how much headroom a group has.",
            self.barrier_injection_duration_seconds.clone(),
        );

        registry.register(
            "barrier_latest_epoch",
            "Per-barrier-group epoch of the newest cut this coordinator \
             published (gauge). A flat value beside a live \
             barrier_min_injection_interval says that injection stopped.",
            self.barrier_latest_epoch.clone(),
        );

        registry.register(
            "barrier_markers_written",
            "Per-topic cumulative count of barrier markers this broker \
             appended, across every group and every partition it leads.",
            self.barrier_markers_written_total.clone(),
        );

        registry.register(
            "barrier_groups_coordinated",
            "Number of barrier groups this broker coordinates (gauge). Zero \
             on a broker that leads no __barrier_state partition.",
            self.barrier_groups_coordinated.clone(),
        );

        registry.register(
            "delivery_watermark",
            "KFC-1 deliver-at-time watermark of each scheduled partition this \
             broker leads (gauge): the first offset that is not visible to a \
             consumer yet. A partition whose topic delivers immediately \
             reports no series.",
            self.delivery_watermark.clone(),
        );

        registry.register(
            "delivery_pending_records",
            "KFC-1 records of each scheduled partition that are durable but \
             not visible yet (gauge): the log end offset minus the delivery \
             watermark.",
            self.delivery_pending_records.clone(),
        );

        registry.register(
            "delivery_activation_lateness_seconds",
            "KFC-1 seconds from a batch's activation deadline to the moment \
             the broker first made it visible. The deadline is the record \
             timestamp plus the topic's declared clock bound, so this measures \
             the delay beyond that bound and a healthy broker sits at zero. A \
             rising tail says the bound is not honest, or that the scheduler \
             is starved of CPU.",
            self.delivery_activation_lateness_seconds.clone(),
        );

        registry.register(
            "delivery_scheduler_wakeups",
            "KFC-1 cumulative count of delivery-scheduler wakeups, whether a \
             deadline came due, a produce re-armed the task, or its idle \
             bound elapsed.",
            self.delivery_scheduler_wakeups_total.clone(),
        );

        registry.register(
            "schema_validation_rejections",
            "KFC-7 cumulative count of records rejected by schema \
             validation, per topic and reason. The reason is one of \
             unframed, unknown_id, wrong_subject, body_mismatch, and \
             registry_unavailable. The broker bumps it once per rejected \
             record. Read the split by reason during a rollout to see which \
             producer is at fault.",
            self.schema_validation_rejections.clone(),
        );

        registry.register(
            "schema_validation_cache_hits",
            "KFC-7 cumulative count of schema lookups the broker answered \
             from its local cache, with no call to the registry.",
            self.schema_validation_cache_hits.clone(),
        );

        registry.register(
            "schema_validation_cache_misses",
            "KFC-7 cumulative count of schema lookups that cost a registry \
             round trip on the produce path. The ratio against \
             schema_validation_cache_hits is what says whether this feature \
             costs anything at steady state.",
            self.schema_validation_cache_misses.clone(),
        );

        registry.register(
            "delivery_clock_uncertainty_seconds",
            "KFC-8 the clock bound this broker declares: the seconds KFC-1 \
             adds to a batch's timestamp before the batch activates. Compare \
             measured clock uncertainty against this series, so an alert \
             tracks the bound the broker relies on instead of a copy of it.",
            self.delivery_clock_uncertainty_seconds.clone(),
        );
    }

    fn register_group_7(&self, registry: &mut Registry) {
        registry.register(
            "topic_freeze_rejections",
            "KFC-9 cumulative count of Produce partition rows the broker \
             refused because a freeze covers the topic, per topic. The gate \
             runs before the batch is parsed, so a refused row moves no log \
             end offset.",
            self.topic_freeze_rejections.clone(),
        );

        registry.register(
            "topic_freezes_active",
            "KFC-9 live entries in the freeze registry (gauge). One prefix \
             entry covers a whole namespace, so this counts entries and not \
             frozen topics. The freeze max_entries setting caps it.",
            self.topic_freezes_active.clone(),
        );

        registry.register(
            "break_glass_proposals",
            "KFC-9 break-glass proposals by state (gauge), where state is one \
             of pending, approved, expired, and consumed. A rise in pending \
             beside a flat approved is an incident where the second person \
             has not answered yet.",
            self.break_glass_proposals.clone(),
        );

        registry.register(
            "break_glass_refusals",
            "KFC-9 cumulative count of privileged transitions the broker \
             refused because no approved break-glass proposal covers them, \
             per action. A refusal is the expected answer when an operator \
             runs the tool before the approval lands.",
            self.break_glass_refusals.clone(),
        );

        registry.register(
            "break_glass_bypassed",
            "KFC-9 cumulative count of privileged transitions that ran \
             WITHOUT an approved break-glass proposal, per action. This is \
             the series to alert on: it counts data-losing unclean \
             recoveries that no second person approved, which the background \
             policy audit-only permits because that path has no caller to \
             refuse. Any non-zero rate needs an operator to read the audit \
             log for the partition it names.",
            self.break_glass_bypassed.clone(),
        );
    }

    /// Build and register every broker metric.
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn new() -> Self {
        let registry = Arc::new(Mutex::new(Registry::with_prefix("krabka_broker")));
        let metrics = Self::unregistered(registry);
        {
            let mut registry = metrics
                .registry
                .try_lock()
                .expect("fresh metrics registry cannot be locked");
            metrics.register_group_1(&mut registry);
            metrics.register_group_2(&mut registry);
            metrics.register_group_3(&mut registry);
            metrics.register_group_4(&mut registry);
            metrics.register_group_5(&mut registry);
            metrics.register_group_6(&mut registry);
            metrics.register_group_7(&mut registry);
        }
        metrics
    }

    /// KIP-511: bump the per-(name, version) handshake counter.
    /// Caller guarantees both inputs already passed
    /// `handlers::api_versions::is_valid_client_info` so the label
    /// values stay bounded.
    pub fn record_client_software(&self, name: &str, version: &str) {
        let lbl = ClientSoftwareLabel {
            software_name: name.to_string(),
            software_version: version.to_string(),
        };
        self.client_software_versions.get_or_create(&lbl).inc();
    }

    /// Account one completed `SaslAuthenticate` frame on
    /// `mechanism`. `success = true` increments
    /// `successful_authentication_total`; `success = false`
    /// increments `failed_authentication_total`. The mechanism
    /// label is the canonical Kafka wire name; pass `"Unknown"`
    /// for the `ILLEGAL_SASL_STATE` reject (no prior handshake)
    /// to keep cardinality bounded.
    pub fn record_authentication(&self, mechanism: &str, success: bool) {
        let lbl = SaslMechanismLabel {
            mechanism: mechanism.to_string(),
        };
        if success {
            self.successful_authentication.get_or_create(&lbl).inc();
        } else {
            self.failed_authentication.get_or_create(&lbl).inc();
        }
    }

    /// Account one dispatched request for `api_key`. The label is the
    /// human-readable name from `api_key_label_name`; unknown keys fold under
    /// `"Unknown"`.
    pub fn record_api_request(&self, api_key: crate::handlers::ApiKeyCode) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.api_requests.get_or_create(&lbl).inc();
    }

    /// Account one request the dispatcher rejected with
    /// `UNSUPPORTED_VERSION` because no handler matched `api_key`
    /// (e.g. unknown `api_key`, or known `api_key` with no version
    /// negotiated). Mirrors the labelling of `record_api_request`.
    pub fn record_unsupported_api_request(&self, api_key: crate::handlers::ApiKeyCode) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.unsupported_api_requests.get_or_create(&lbl).inc();
    }

    /// Observe the wall-clock handling latency for one dispatched
    /// request on the `request_duration_seconds{api}` histogram. `api_key`
    /// is resolved to the same human-readable label as
    /// `record_api_request` (unknown keys fold under `"Unknown"`), so the
    /// two families share one label set. Called from the dispatch path once
    /// per frame with the elapsed seconds of the full handler round-trip.
    pub fn observe_request_duration(&self, api_key: i16, seconds: f64) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.request_duration_seconds
            .get_or_create(&lbl)
            .observe(seconds);
    }

    /// Account one request whose handler returned an error (the
    /// dispatcher closed the connection). Labelled like
    /// `record_api_request`; disjoint from the
    /// `unsupported_api_requests` family.
    pub fn record_request_error(&self, api_key: i16) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.request_errors.get_or_create(&lbl).inc();
    }

    /// Convenience: record a Produce hit on `topic` with the given
    /// payload size. No-op on the error path — callers shouldn't call
    /// this if the request was rejected.
    pub fn record_produce(&self, topic: &str, bytes: u64) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.topic_produce_requests.get_or_create(&lbl).inc();
        if bytes > 0 {
            self.topic_bytes_in.get_or_create(&lbl).inc_by(bytes);
        }
    }

    /// Account `messages` records received on the Produce
    /// path for `topic`. Mirrors Kafka's
    /// `BrokerTopicMetrics.MessagesInPerSec`. Called once per
    /// `RecordBatch` with the batch's record count. Zero is a
    /// legitimate value (legacy batches whose record count we can't
    /// cheaply derive without a full conversion) and is a no-op.
    pub fn record_produce_messages(&self, topic: &str, messages: u64) {
        if messages == 0 {
            return;
        }
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.topic_messages_in.get_or_create(&lbl).inc_by(messages);
    }

    /// Convenience: record a Fetch hit on `topic` with the bytes
    /// delivered. The `bytes` arg may legitimately be zero (empty
    /// fetch); the request counter still increments.
    pub fn record_fetch(&self, topic: &str, bytes: u64) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.topic_fetch_requests.get_or_create(&lbl).inc();
        if bytes > 0 {
            self.topic_bytes_out.get_or_create(&lbl).inc_by(bytes);
        }
    }

    /// Record a single failed Produce partition response
    /// for `topic`. Callers bump once per partition whose response
    /// carries a non-zero error code — mirrors the JVM's per-row
    /// `failedProduceRequestRate.mark()`.
    pub fn record_failed_produce(&self, topic: &str) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.topic_failed_produce_requests.get_or_create(&lbl).inc();
    }

    /// Record a single failed Fetch partition response
    /// for `topic`. Same per-partition semantics as
    /// `record_failed_produce`.
    pub fn record_failed_fetch(&self, topic: &str) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.topic_failed_fetch_requests.get_or_create(&lbl).inc();
    }

    /// Convenience: account a partition's slice of a Produce request.
    /// Called once per partition by the request handler (alongside the
    /// existing topic-level `record_produce`).
    pub fn record_partition_produce(&self, topic: &str, partition: i32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.partition_bytes_in.get_or_create(&lbl).inc_by(bytes);
    }

    /// Convenience: account a partition's slice of a Fetch response.
    pub fn record_partition_fetch(&self, topic: &str, partition: i32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.partition_bytes_out.get_or_create(&lbl).inc_by(bytes);
    }

    /// Account bytes this broker received from the partition
    /// leader as a follower (inter-broker `Fetch` round-trip, follower
    /// side). Called from the replicator after a successful append.
    pub fn record_replication_in(&self, topic: &str, partition: i32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.replication_bytes_in.get_or_create(&lbl).inc_by(bytes);
    }

    /// Account one v0/v1 → v2 up-conversion on the Produce
    /// path (the partition's `records` field arrived as a legacy
    /// `MessageSet` and was decoded into a v2 `RecordBatch`).
    pub fn record_produce_message_conversion(&self, topic: &str) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.produce_message_conversions.get_or_create(&lbl).inc();
    }

    /// Account one v2 → v0/v1 down-conversion on the Fetch
    /// path (a legacy client's Fetch v < 4 response is being assembled
    /// from a v2 record batch).
    pub fn record_fetch_message_conversion(&self, topic: &str) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.fetch_message_conversions.get_or_create(&lbl).inc();
    }

    /// KIP-841: account one unclean leader election (an
    /// election that picked an out-of-ISR replica because the ISR was
    /// empty and the topic had `unclean.leader.election.enable=true`).
    pub fn record_unclean_leader_election(&self) {
        self.unclean_leader_elections_total.inc();
    }

    /// Account bytes this broker served to a follower as the
    /// partition leader (inter-broker `Fetch` round-trip, leader side).
    /// Called from the `Fetch` handler when `replica_id >= 0`.
    pub fn record_replication_out(&self, topic: &str, partition: i32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.replication_bytes_out.get_or_create(&lbl).inc_by(bytes);
    }

    /// Convenience: account handler-thread microseconds spent on a
    /// partition. Called from the produce / fetch hot paths around the
    /// per-partition work. No-ops on zero so we don't allocate a label
    /// entry for trivial measurements.
    pub fn record_partition_cpu_micros(&self, topic: &str, partition: i32, micros: u64) {
        if micros == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.partition_cpu_micros.get_or_create(&lbl).inc_by(micros);
    }

    /// Account one completed log-compaction sweep (a full `tick_all`
    /// pass). Called once per cleaner tick, whether or not any partition
    /// was eligible, so a test can observe that a full pass ran after it
    /// sealed a segment.
    pub fn record_cleaner_run(&self) {
        self.log_cleaner_runs_total.inc();
    }

    /// Account one successful per-partition compaction pass
    /// (`Partition::compact_log` returned `Ok`).
    pub fn record_compaction(&self, topic: &str, partition: i32) {
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.log_compactions_total.get_or_create(&lbl).inc();
    }

    /// KFC-1: publish one scheduled partition's delivery watermark and the
    /// count of records that are durable but not visible yet. Called from the
    /// delivery scheduler after it recomputes the partition. A partition whose
    /// topic delivers immediately never reaches this method, so an ordinary
    /// topic creates no series.
    pub fn record_delivery_watermark(
        &self,
        topic: &str,
        partition: i32,
        watermark: i64,
        pending: i64,
    ) {
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.delivery_watermark.get_or_create(&lbl).set(watermark);
        self.delivery_pending_records
            .get_or_create(&lbl)
            .set(pending);
    }

    /// KFC-7: account one record that failed schema validation on `topic`.
    ///
    /// Callers bump once per rejected record, so a Produce request with three
    /// bad records makes three calls. `reason` should be one of the five fixed
    /// label values the schema validator returns, because the label set stays
    /// bounded only while it is.
    pub fn record_schema_validation_rejection(&self, topic: &str, reason: &str) {
        let lbl = SchemaRejectionLabel {
            topic: topic.to_string(),
            reason: reason.to_string(),
        };
        self.schema_validation_rejections.get_or_create(&lbl).inc();
    }

    /// KFC-7: account one schema lookup the broker answered from its local
    /// cache.
    pub fn record_schema_cache_hit(&self) {
        self.schema_validation_cache_hits.inc();
    }

    /// KFC-7: account one schema lookup that cost a registry round trip on the
    /// produce path.
    pub fn record_schema_cache_miss(&self) {
        self.schema_validation_cache_misses.inc();
    }

    /// KFC-9: account one Produce partition row the broker refused because a
    /// freeze covers `topic`.
    ///
    /// The produce gate calls it once for each refused row, so one request
    /// that names three partitions of a frozen topic makes three calls.
    /// `topic` comes from a name that resolved in the metadata image, and the
    /// series count is bounded by the number of topics a freeze covers.
    pub fn record_topic_freeze_rejection(&self, topic: &str) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.topic_freeze_rejections.get_or_create(&lbl).inc();
    }

    /// KFC-9: publish the number of live entries in the freeze registry.
    ///
    /// The metadata-image watcher calls it after an apply changes the
    /// registry, so the gauge falls when a thaw removes an entry.
    pub fn record_topic_freezes_active(&self, entries: i64) {
        self.topic_freezes_active.set(entries);
    }

    /// KFC-9: publish the number of break-glass proposals in `state`.
    ///
    /// The caller publishes one value for each [`BreakGlassState`], so a
    /// proposal that moves from `Pending` to `Consumed` lowers one series and
    /// raises another.
    pub fn record_break_glass_proposals(&self, state: BreakGlassState, count: i64) {
        let lbl = BreakGlassStateLabel { state };
        self.break_glass_proposals.get_or_create(&lbl).set(count);
    }

    /// KFC-9: account one privileged transition the broker refused because no
    /// approved break-glass proposal covers `action`.
    pub fn record_break_glass_refusal(&self, action: BreakGlassAction) {
        let lbl = BreakGlassActionLabel { action };
        self.break_glass_refusals.get_or_create(&lbl).inc();
    }

    /// KFC-9: account one privileged transition that ran **without** an
    /// approved break-glass proposal.
    ///
    /// This is the series an operator alerts on. It counts a data-losing
    /// transition that no second person approved: the background
    /// unclean-recovery path has no caller to refuse, so the `audit-only`
    /// policy lets recovery run and calls this method instead of failing
    /// closed. A gated transition that an operator ran with an approval never
    /// reaches here.
    pub fn record_break_glass_bypass(&self, action: BreakGlassAction) {
        let lbl = BreakGlassActionLabel { action };
        self.break_glass_bypassed.get_or_create(&lbl).inc();
    }
}

impl Default for BrokerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve a wire `api_key` to the name used as the metric label.
///
/// A Kafka api key resolves to its `ApiKey` variant name. A krabka-private api
/// key resolves through [`krabka_private_api_key_label_name`], because
/// `ApiKey::from_i16` does not know that range. Anything else folds under
/// [`UNKNOWN_LABEL`].
fn api_key_label_name(api_key: crate::handlers::ApiKeyCode) -> &'static str {
    if api_key >= crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR {
        return krabka_private_api_key_label_name(api_key);
    }
    match krabka_protocol::api_key::ApiKey::from_i16(api_key) {
        Some(k) => k.into(),
        None => UNKNOWN_LABEL,
    }
}

/// Resolve a krabka-private wire `api_key` to its RPC name.
///
/// Without this arm every krabka-private request shares one
/// `api_requests{api_key="Unknown"}` series with genuine garbage traffic, and
/// an operator cannot tell the two apart. Cardinality stays bounded: the range
/// holds one label per krabka-private RPC, plus [`UNKNOWN_LABEL`].
fn krabka_private_api_key_label_name(api_key: crate::handlers::ApiKeyCode) -> &'static str {
    match api_key {
        crate::handlers::ALTER_BARRIER_GROUPS_API_KEY => "AlterBarrierGroups",
        crate::handlers::DESCRIBE_BARRIER_GROUPS_API_KEY => "DescribeBarrierGroups",
        crate::handlers::TRIGGER_BARRIER_API_KEY => "TriggerBarrier",
        crate::handlers::LIST_BARRIER_CUTS_API_KEY => "ListBarrierCuts",
        crate::handlers::WRITE_BARRIER_MARKERS_API_KEY => "WriteBarrierMarkers",
        _ => UNKNOWN_LABEL,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{convert::TimeExt as _, millis, secs};

    use super::*;

    /// The gauge exists so an alert can read the bound the broker relies on
    /// instead of carrying a copy of it, so what matters is that the exported
    /// value is the configured extent in seconds.
    #[tokio::test]
    async fn declared_clock_bound_is_exported_in_seconds() {
        for bound in [millis(250), millis(750), secs(2), millis(1)] {
            let m = BrokerMetrics::new();
            m.delivery_clock_uncertainty_seconds.set(bound.secs_f64());

            let mut buf = String::new();
            let r = m.registry.lock().await;
            prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
            drop(r);

            let name = "krabka_broker_delivery_clock_uncertainty_seconds ";
            let line = buf
                .lines()
                .find(|line| line.starts_with(name))
                .expect("the declared bound is registered and exported");
            let exported: f64 = line[name.len()..]
                .trim()
                .parse()
                .expect("the gauge encodes a number");

            // Bit equality rather than `==`: the assertion is that the
            // encode/parse round trip returns the same `f64`, not that two
            // computed values are near each other, and `clippy::float_cmp`
            // rejects the latter shape. Comparing bits says exactly what is
            // meant and needs no suppression.
            assert!(exported.to_bits() == bound.secs_f64().to_bits());
        }
    }

    #[tokio::test]
    async fn registry_has_broker_prefix_and_all_metrics() {
        let m = BrokerMetrics::new();
        m.record_produce("topic-a", 100);
        m.record_produce_messages("topic-a", 5);
        m.record_fetch("topic-a", 50);
        m.record_partition_produce("topic-a", 0, 100);
        m.record_partition_fetch("topic-a", 0, 50);
        m.record_partition_cpu_micros("topic-a", 0, 250);
        m.record_replication_in("topic-a", 0, 4096);
        m.record_replication_out("topic-a", 0, 8192);
        m.record_cleaner_run();
        m.record_compaction("topic-a", 0);
        let barrier_group = BarrierGroupLabel {
            group: "orders-cut".into(),
        };
        m.barrier_epochs_started_total
            .get_or_create(&barrier_group)
            .inc();
        m.barrier_epochs_committed_total
            .get_or_create(&barrier_group)
            .inc();
        m.barrier_epochs_published_partial_total
            .get_or_create(&barrier_group)
            .inc();
        m.barrier_injection_duration_seconds
            .get_or_create(&barrier_group)
            .observe(0.02);
        m.barrier_latest_epoch.get_or_create(&barrier_group).set(7);
        m.barrier_markers_written_total
            .get_or_create(&TopicLabel {
                topic: "topic-a".into(),
            })
            .inc_by(3);
        m.barrier_groups_coordinated.set(1);
        m.record_produce_message_conversion("topic-a");
        m.record_fetch_message_conversion("topic-a");
        m.record_failed_produce("topic-a");
        m.record_failed_fetch("topic-a");
        m.record_schema_validation_rejection("topic-a", "unknown_id");
        m.record_schema_cache_hit();
        m.record_schema_cache_miss();
        m.record_topic_freeze_rejection("topic-a");
        m.record_topic_freezes_active(1);
        m.record_break_glass_proposals(BreakGlassState::Pending, 1);
        m.record_break_glass_refusal(BreakGlassAction(GatedAction::DeleteTopic));
        m.record_break_glass_bypass(BreakGlassAction(GatedAction::UncleanRecovery));
        m.record_authentication("PLAIN", true);
        m.record_authentication("SCRAM-SHA-512", false);
        m.record_authentication("Unknown", false);
        m.record_unclean_leader_election();
        m.record_api_request(0); // Produce
        m.record_api_request(999); // unknown → "Unknown" label
        m.record_unsupported_api_request(999);
        m.observe_request_duration(0, 0.002); // Produce latency sample
        m.observe_request_duration(999, 1.5); // unknown → "Unknown" label
        m.record_request_error(1); // Fetch handler error
        m.in_flight_requests.set(3);
        m.active_connections.set(11);
        m.partition_disk_bytes
            .get_or_create(&PartitionLabel {
                topic: "topic-a".into(),
                partition: 0,
            })
            .set(42);
        m.share_group_backlog
            .get_or_create(&ShareGroupLabel {
                group_id: "workers".into(),
                topic: "topic-a".into(),
                partition: 0,
            })
            .set(9);
        m.partitions_led.set(7);
        m.partitions_total.set(42);
        m.under_replicated_partitions.set(3);
        m.under_min_isr_partition_count.set(2);
        m.offline_partitions_count.set(1);
        m.active_controller.set(1);
        m.ignored_static_voters.set(3);
        m.witness_role.set(1);
        m.leader_site_drift_partitions.set(4);
        m.voted_directory
            .get_or_create(&DirectoryLabel {
                directory_id: "00000000-0000-0000-0000-000000000001".into(),
            })
            .set(1);
        m.controller_leader_changes_total.inc();
        m.isr_shrinks_total.inc();
        m.isr_expands_total.inc_by(2);

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        // Spot-check every metric is present and prefixed.
        for needle in [
            "krabka_broker_topic_bytes_in_total",
            "krabka_broker_topic_bytes_out_total",
            "krabka_broker_topic_produce_requests_total",
            "krabka_broker_topic_fetch_requests_total",
            "krabka_broker_partitions_led",
            "krabka_broker_partitions_total",
            "krabka_broker_under_replicated_partitions",
            "krabka_broker_under_min_isr_partition_count",
            "krabka_broker_offline_partitions_count",
            "krabka_broker_active_controller",
            "krabka_broker_ignored_static_voters",
            "krabka_broker_witness_role",
            "krabka_broker_leader_site_drift_partitions",
            "krabka_broker_voted_directory",
            "krabka_broker_controller_leader_changes_total",
            "krabka_broker_isr_shrinks_total",
            "krabka_broker_isr_expands_total",
            "krabka_broker_partition_bytes_in_total",
            "krabka_broker_partition_bytes_out_total",
            "krabka_broker_partition_disk_bytes",
            "krabka_broker_share_group_backlog",
            "krabka_broker_partition_cpu_micros_total",
            "krabka_broker_incremental_fetch_sessions",
            "krabka_broker_incremental_fetch_session_evictions_total",
            "krabka_broker_incremental_fetch_partitions_cached",
            "krabka_broker_replication_bytes_in_total",
            "krabka_broker_replication_bytes_out_total",
            "krabka_broker_tiered_storage_rlmm_topic_backed",
            "krabka_broker_produce_message_conversions_total",
            "krabka_broker_fetch_message_conversions_total",
            "krabka_broker_unclean_leader_elections_total",
            "krabka_broker_log_cleaner_runs_total",
            "krabka_broker_log_compactions_total",
            "krabka_broker_api_requests_total",
            "krabka_broker_unsupported_api_requests_total",
            "krabka_broker_request_duration_seconds_bucket",
            "krabka_broker_request_duration_seconds_sum",
            "krabka_broker_request_duration_seconds_count",
            "krabka_broker_in_flight_requests",
            "krabka_broker_active_connections",
            "krabka_broker_request_errors_total",
            "krabka_broker_messages_in_total",
            "krabka_broker_topic_failed_produce_requests_total",
            "krabka_broker_topic_failed_fetch_requests_total",
            "krabka_broker_successful_authentication_total",
            "krabka_broker_failed_authentication_total",
            "krabka_broker_barrier_epochs_started_total",
            "krabka_broker_barrier_epochs_committed_total",
            "krabka_broker_barrier_epochs_published_partial_total",
            "krabka_broker_barrier_injection_duration_seconds_bucket",
            "krabka_broker_barrier_injection_duration_seconds_sum",
            "krabka_broker_barrier_injection_duration_seconds_count",
            "krabka_broker_barrier_latest_epoch",
            "krabka_broker_barrier_markers_written_total",
            "krabka_broker_barrier_groups_coordinated",
            "krabka_broker_schema_validation_rejections_total",
            "krabka_broker_schema_validation_cache_hits_total",
            "krabka_broker_schema_validation_cache_misses_total",
            "krabka_broker_topic_freeze_rejections_total",
            "krabka_broker_topic_freezes_active",
            "krabka_broker_break_glass_proposals",
            "krabka_broker_break_glass_refusals_total",
            "krabka_broker_break_glass_bypassed_total",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
        // Topic label and values made it through.
        for (needle, what) in [
            ("topic=\"topic-a\"", "topic label"),
            ("100", "bytes_in=100"),
            ("50", "bytes_out=50"),
            ("7", "partitions_led=7"),
        ] {
            assert!(buf.contains(needle), "expected {what} in:\n{buf}");
        }
    }

    #[test]
    fn api_key_label_name_names_every_krabka_private_api() {
        let cases = [
            (
                crate::handlers::ALTER_BARRIER_GROUPS_API_KEY,
                "AlterBarrierGroups",
            ),
            (
                crate::handlers::DESCRIBE_BARRIER_GROUPS_API_KEY,
                "DescribeBarrierGroups",
            ),
            (crate::handlers::TRIGGER_BARRIER_API_KEY, "TriggerBarrier"),
            (
                crate::handlers::LIST_BARRIER_CUTS_API_KEY,
                "ListBarrierCuts",
            ),
            (
                crate::handlers::WRITE_BARRIER_MARKERS_API_KEY,
                "WriteBarrierMarkers",
            ),
            // A Kafka api key still resolves through the generated enum.
            (0, "Produce"),
            // Garbage inside the krabka-private range, and outside it, both
            // fold under the sentinel.
            (crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR, UNKNOWN_LABEL),
            (9999, UNKNOWN_LABEL),
            (999, UNKNOWN_LABEL),
        ];

        for (api_key, want) in cases {
            assert!(api_key_label_name(api_key) == want, "api_key {api_key}");
        }
    }

    #[test]
    fn record_fetch_zero_bytes_still_bumps_request_count() {
        let m = BrokerMetrics::new();
        let lbl = TopicLabel {
            topic: "t".to_string(),
        };
        // Pre-condition: no entry for the label yet.
        m.record_fetch("t", 0);
        assert!(m.topic_fetch_requests.get_or_create(&lbl).get() == 1);
        assert!(m.topic_bytes_out.get_or_create(&lbl).get() == 0);
    }

    #[test]
    fn record_produce_increments_both_counters() {
        let m = BrokerMetrics::new();
        let lbl = TopicLabel {
            topic: "t".to_string(),
        };
        m.record_produce("t", 1024);
        m.record_produce("t", 2048);
        assert!(m.topic_produce_requests.get_or_create(&lbl).get() == 2);
        assert!(m.topic_bytes_in.get_or_create(&lbl).get() == 3072);
    }

    #[test]
    fn record_produce_messages_sums_across_calls_and_skips_zero() {
        let m = BrokerMetrics::new();
        let lbl = TopicLabel {
            topic: "t".to_string(),
        };
        // Zero is a no-op (legacy batches; the v2-conversion-time
        // counter tracks those arrivals separately).
        m.record_produce_messages("t", 0);
        // The label entry is intentionally NOT eagerly created on a
        // zero-bump; rate(...) over a never-seen topic should yield
        // 0, not a phantom series.
        m.record_produce_messages("t", 3);
        m.record_produce_messages("t", 7);
        assert!(m.topic_messages_in.get_or_create(&lbl).get() == 10);
    }

    #[test]
    fn record_authentication_splits_success_and_failure_per_mechanism() {
        let m = BrokerMetrics::new();
        let plain = SaslMechanismLabel {
            mechanism: "PLAIN".to_string(),
        };
        let scram = SaslMechanismLabel {
            mechanism: "SCRAM-SHA-256".to_string(),
        };
        let unknown = SaslMechanismLabel {
            mechanism: "Unknown".to_string(),
        };
        m.record_authentication("PLAIN", true);
        m.record_authentication("PLAIN", true);
        m.record_authentication("PLAIN", false);
        m.record_authentication("SCRAM-SHA-256", true);
        m.record_authentication("Unknown", false);
        // PLAIN: 2 successes, 1 failure. SCRAM-SHA-256: 1 success, 0
        // failures (must not lazily allocate a failure entry from the
        // success bump). ILLEGAL_SASL_STATE: 0 successes, 1 failure
        // under the `Unknown` sentinel.
        let cases = [
            ("successful", &m.successful_authentication, &plain, 2),
            ("failed", &m.failed_authentication, &plain, 1),
            ("successful", &m.successful_authentication, &scram, 1),
            ("failed", &m.failed_authentication, &unknown, 1),
            ("successful", &m.successful_authentication, &unknown, 0),
        ];
        for (outcome, family, label, want) in cases {
            // Each read is its own statement: `get_or_create` returns a
            // read guard, and a first-materialization on the same family
            // takes the write lock — holding several guards in one
            // expression self-deadlocks.
            let got = family.get_or_create(label).get();
            assert!(got == want, "{outcome} auth for {:?}", label.mechanism);
        }
    }

    #[test]
    fn record_client_software_accumulates_per_name_version() {
        let m = BrokerMetrics::new();
        let krabka_100 = ClientSoftwareLabel {
            software_name: "krabka".to_string(),
            software_version: "1.0.0".to_string(),
        };
        let krabka_101 = ClientSoftwareLabel {
            software_name: "krabka".to_string(),
            software_version: "1.0.1".to_string(),
        };
        let other = ClientSoftwareLabel {
            software_name: "other-lib".to_string(),
            software_version: "1.0.0".to_string(),
        };

        m.record_client_software("krabka", "1.0.0");
        m.record_client_software("krabka", "1.0.0");
        m.record_client_software("krabka", "1.0.1");
        m.record_client_software("other-lib", "1.0.0");

        for (label, want) in [(&krabka_100, 2), (&krabka_101, 1), (&other, 1)] {
            let got = m.client_software_versions.get_or_create(label).get();
            assert!(got == want, "label {label:?}");
        }
    }

    #[tokio::test]
    async fn record_client_software_renders_labelled_openmetrics_counter() {
        let m = BrokerMetrics::new();

        m.record_client_software("render-lib", "2.0.0");

        let mut body = String::new();
        let registry = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut body, &registry).unwrap();
        assert!(body.contains(
            "krabka_broker_client_software_versions_total{software_name=\"render-lib\",software_version=\"2.0.0\"} 1"
        ));
    }

    #[test]
    fn partition_helpers_increment_the_right_family() {
        let m = BrokerMetrics::new();
        m.record_partition_produce("t", 0, 1024);
        m.record_partition_produce("t", 1, 512);
        m.record_partition_fetch("t", 0, 2048);
        m.record_partition_cpu_micros("t", 0, 500);
        m.partition_disk_bytes
            .get_or_create(&PartitionLabel {
                topic: "t".into(),
                partition: 0,
            })
            .set(1_000_000);

        let lbl_p0 = PartitionLabel {
            topic: "t".into(),
            partition: 0,
        };
        let lbl_p1 = PartitionLabel {
            topic: "t".into(),
            partition: 1,
        };
        let cases = [
            ("bytes_in", &m.partition_bytes_in, &lbl_p0, 1024),
            ("bytes_in", &m.partition_bytes_in, &lbl_p1, 512),
            ("bytes_out", &m.partition_bytes_out, &lbl_p0, 2048),
            ("cpu_micros", &m.partition_cpu_micros, &lbl_p0, 500),
        ];
        for (family_name, family, label, want) in cases {
            // Each read is its own statement: `get_or_create` returns a
            // read guard, and a first-materialization on the same family
            // takes the write lock — holding several guards in one
            // expression self-deadlocks.
            let got = family.get_or_create(label).get();
            assert!(
                got == want,
                "{family_name} for partition {}",
                label.partition
            );
        }
        // `partition_disk_bytes` is a Gauge family (i64), so it stays
        // out of the Counter table above.
        let disk_p0 = m.partition_disk_bytes.get_or_create(&lbl_p0).get();
        assert!(disk_p0 == 1_000_000);
    }

    #[test]
    fn failed_request_counters_track_per_topic_and_per_call() {
        // `record_failed_produce` / `record_failed_fetch`
        // are bumped once per failed partition row. Two calls on
        // `t-good` and one on `t-bad` must land on the right labels
        // and yield independent series.
        let m = BrokerMetrics::new();
        m.record_failed_produce("t-good");
        m.record_failed_produce("t-good");
        m.record_failed_produce("t-bad");
        m.record_failed_fetch("t-good");

        let good = TopicLabel {
            topic: "t-good".into(),
        };
        let bad = TopicLabel {
            topic: "t-bad".into(),
        };
        // t-bad never saw a failed fetch — series is materialized by
        // `get_or_create` at read time but its value is 0, which is
        // what `rate(failed_fetch{topic="t-bad"}[1m])` should compute.
        let cases = [
            ("failed_produce", &m.topic_failed_produce_requests, &good, 2),
            ("failed_produce", &m.topic_failed_produce_requests, &bad, 1),
            ("failed_fetch", &m.topic_failed_fetch_requests, &good, 1),
            ("failed_fetch", &m.topic_failed_fetch_requests, &bad, 0),
        ];
        for (family_name, family, label, want) in cases {
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = family.get_or_create(label).get();
            assert!(got == want, "{family_name} for {:?}", label.topic);
        }
    }

    #[test]
    fn zero_bytes_no_op_on_partition_helpers() {
        let m = BrokerMetrics::new();
        m.record_partition_produce("t", 0, 0);
        m.record_partition_fetch("t", 0, 0);
        let lbl = PartitionLabel {
            topic: "t".into(),
            partition: 0,
        };
        // Counters still exist (get_or_create creates them) but at 0.
        assert!(m.partition_bytes_in.get_or_create(&lbl).get() == 0);
        assert!(m.partition_bytes_out.get_or_create(&lbl).get() == 0);
    }

    #[test]
    fn zero_micros_no_op() {
        let m = BrokerMetrics::new();
        m.record_partition_cpu_micros("t", 0, 0);
        let lbl = PartitionLabel {
            topic: "t".into(),
            partition: 0,
        };
        // Helper short-circuits at 0; the label entry isn't created.
        assert!(m.partition_cpu_micros.get_or_create(&lbl).get() == 0);
    }

    #[test]
    fn tiered_storage_rlmm_topic_backed_defaults_zero_and_can_be_set() {
        let m = BrokerMetrics::new();
        // Default for a fresh broker (in-memory placeholder, or no
        // tiered-storage at all) is `0`.
        assert!(m.tiered_storage_rlmm_topic_backed.get() == 0);
        // The bootstrap task bumps it to `1` after a successful
        // SwappableRlmm swap.
        m.tiered_storage_rlmm_topic_backed.set(1);
        assert!(m.tiered_storage_rlmm_topic_backed.get() == 1);
    }

    #[test]
    fn tiered_storage_rlmm_bootstrap_attempts_counts_up() {
        let m = BrokerMetrics::new();
        assert!(m.tiered_storage_rlmm_bootstrap_attempts.get() == 0);
        m.tiered_storage_rlmm_bootstrap_attempts.inc();
        m.tiered_storage_rlmm_bootstrap_attempts.inc();
        assert!(m.tiered_storage_rlmm_bootstrap_attempts.get() == 2);
    }

    #[test]
    fn message_conversion_helpers_accumulate_per_topic() {
        let m = BrokerMetrics::new();
        m.record_produce_message_conversion("orders");
        m.record_produce_message_conversion("orders");
        m.record_produce_message_conversion("payments");
        m.record_fetch_message_conversion("orders");
        m.record_fetch_message_conversion("payments");
        m.record_fetch_message_conversion("payments");

        let orders = TopicLabel {
            topic: "orders".into(),
        };
        let payments = TopicLabel {
            topic: "payments".into(),
        };
        let cases = [
            (
                "produce_conversions",
                &m.produce_message_conversions,
                &orders,
                2,
            ),
            (
                "produce_conversions",
                &m.produce_message_conversions,
                &payments,
                1,
            ),
            (
                "fetch_conversions",
                &m.fetch_message_conversions,
                &orders,
                1,
            ),
            (
                "fetch_conversions",
                &m.fetch_message_conversions,
                &payments,
                2,
            ),
        ];
        for (family_name, family, label, want) in cases {
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = family.get_or_create(label).get();
            assert!(got == want, "{family_name} for {:?}", label.topic);
        }
    }

    #[test]
    fn schema_validation_helpers_accumulate_per_topic_and_reason() {
        // KFC-7: rejections are keyed by (topic, reason), so a run of
        // `unframed` on one topic must not move any other pair. The two cache
        // counters carry no labels and must stay independent of each other.
        let m = BrokerMetrics::new();
        m.record_schema_validation_rejection("orders", "unframed");
        m.record_schema_validation_rejection("orders", "unframed");
        m.record_schema_validation_rejection("orders", "wrong_subject");
        m.record_schema_validation_rejection("payments", "unframed");
        m.record_schema_validation_rejection("payments", "registry_unavailable");
        m.record_schema_cache_hit();
        m.record_schema_cache_hit();
        m.record_schema_cache_hit();
        m.record_schema_cache_miss();

        // A pair that saw no rejection reads 0: `get_or_create` materializes
        // the series at read time, which is what
        // `rate(schema_validation_rejections_total{...}[1m])` computes over.
        let cases = [
            ("orders", "unframed", 2),
            ("orders", "wrong_subject", 1),
            ("orders", "registry_unavailable", 0),
            ("orders", "body_mismatch", 0),
            ("payments", "unframed", 1),
            ("payments", "wrong_subject", 0),
            ("payments", "registry_unavailable", 1),
        ];
        for (topic, reason, want) in cases {
            let lbl = SchemaRejectionLabel {
                topic: topic.to_string(),
                reason: reason.to_string(),
            };
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = m.schema_validation_rejections.get_or_create(&lbl).get();
            assert!(got == want, "rejections for {topic} / {reason}");
        }

        assert!(m.schema_validation_cache_hits.get() == 3);
        assert!(m.schema_validation_cache_misses.get() == 1);
    }

    #[test]
    fn unsupported_api_requests_counter_is_disjoint_from_api_requests() {
        let m = BrokerMetrics::new();
        // Invariant: `record_unsupported_api_request` bumps
        // only the `unsupported_api_requests` family — operators
        // expect `api_requests` to count *every* dispatched frame and
        // `unsupported_api_requests` to count just the ones that hit
        // the synthetic UNSUPPORTED_VERSION arm.
        m.record_unsupported_api_request(0); // Produce, unsupported
        m.record_unsupported_api_request(999); // truly unknown

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let unknown = ApiKeyLabel {
            api_key: "Unknown".into(),
        };
        // `record_unsupported_api_request` does NOT also bump
        // `api_requests`; the dispatcher already did that for the
        // request in question via `record_api_request`.
        let cases = [
            (
                "unsupported_api_requests",
                &m.unsupported_api_requests,
                &produce,
                1,
            ),
            (
                "unsupported_api_requests",
                &m.unsupported_api_requests,
                &unknown,
                1,
            ),
            ("api_requests", &m.api_requests, &produce, 0),
            ("api_requests", &m.api_requests, &unknown, 0),
        ];
        for (family_name, family, label, want) in cases {
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = family.get_or_create(label).get();
            assert!(got == want, "{family_name} for {:?}", label.api_key);
        }
    }

    #[test]
    fn api_requests_label_resolves_known_keys_and_folds_unknown() {
        let m = BrokerMetrics::new();
        // Three known + one unknown api_key. Verify per-label tallies.
        m.record_api_request(0); // Produce
        m.record_api_request(0); // Produce again
        m.record_api_request(1); // Fetch
        m.record_api_request(12_345); // out-of-range → Unknown

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let fetch = ApiKeyLabel {
            api_key: "Fetch".into(),
        };
        let unknown = ApiKeyLabel {
            api_key: "Unknown".into(),
        };
        for (label, want) in [(&produce, 2), (&fetch, 1), (&unknown, 1)] {
            let got = m.api_requests.get_or_create(label).get();
            assert!(got == want, "api_key {:?}", label.api_key);
        }
    }

    #[test]
    fn audit_counters_present() {
        let m = BrokerMetrics::new();
        m.audit_events_total.inc();
        m.audit_write_failures_total.inc();
        assert2::check!(m.audit_events_total.get() == 1);
        assert2::check!(m.audit_write_failures_total.get() == 1);
    }

    #[test]
    fn replication_helpers_accumulate_per_partition() {
        let m = BrokerMetrics::new();
        // Two appends from the same leader partition.
        m.record_replication_in("orders", 3, 1_500);
        m.record_replication_in("orders", 3, 2_500);
        // Different partition stays independent.
        m.record_replication_in("orders", 4, 100);
        // Outbound side: bytes this broker served to its followers.
        m.record_replication_out("orders", 3, 4_000);
        m.record_replication_out("orders", 4, 0); // no-op

        let lbl3 = PartitionLabel {
            topic: "orders".into(),
            partition: 3,
        };
        let lbl4 = PartitionLabel {
            topic: "orders".into(),
            partition: 4,
        };
        let cases = [
            ("replication_in", &m.replication_bytes_in, &lbl3, 4_000),
            ("replication_in", &m.replication_bytes_in, &lbl4, 100),
            ("replication_out", &m.replication_bytes_out, &lbl3, 4_000),
            ("replication_out", &m.replication_bytes_out, &lbl4, 0),
        ];
        for (family_name, family, label, want) in cases {
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = family.get_or_create(label).get();
            assert!(
                got == want,
                "{family_name} for partition {}",
                label.partition
            );
        }
    }

    #[tokio::test]
    async fn request_duration_errors_and_gauges_render() {
        let m = BrokerMetrics::new();
        // Two Produce latency samples + one unknown-key sample.
        m.observe_request_duration(0, 0.0008);
        m.observe_request_duration(0, 0.04);
        m.observe_request_duration(12_345, 2.0); // → "Unknown" label
        m.record_request_error(1); // Fetch handler fault
        m.record_request_error(1);
        m.in_flight_requests.inc();
        m.in_flight_requests.inc();
        m.in_flight_requests.dec();
        m.active_connections.set(5);

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let fetch = ApiKeyLabel {
            api_key: "Fetch".into(),
        };
        // Histogram Family exposes sample count via the encoded `_count`;
        // assert the render + the error/gauge values here.
        assert!(m.request_errors.get_or_create(&fetch).get() == 2);
        assert!(m.in_flight_requests.get() == 1);
        assert!(m.active_connections.get() == 5);

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        assert!(
            buf.contains("krabka_broker_request_duration_seconds_count{api_key=\"Produce\"} 2"),
            "expected 2 Produce latency samples in:\n{buf}"
        );
        assert!(
            buf.contains("krabka_broker_request_errors_total{api_key=\"Fetch\"} 2"),
            "expected 2 Fetch request errors in:\n{buf}"
        );
        assert!(buf.contains("krabka_broker_in_flight_requests 1"));
        assert!(buf.contains("krabka_broker_active_connections 5"));
        // Unknown api_key folds under the shared "Unknown" label.
        assert!(buf.contains("api_key=\"Unknown\""), "unknown label missing");
        // Keep `produce` referenced to document the intended label.
        let _ = produce;
    }

    #[tokio::test]
    async fn kfc9_families_scrape_under_their_names_with_their_labels() {
        // The registered name plus the counter suffix is what an alert rule
        // spells, and the label name is what it groups by, so both belong in
        // the assertion. Every value here is the movement one call makes.
        let m = BrokerMetrics::new();
        m.record_topic_freeze_rejection("orders");
        m.record_topic_freezes_active(2);
        m.record_break_glass_proposals(BreakGlassState::Pending, 3);
        m.record_break_glass_refusal(BreakGlassAction(GatedAction::DeleteTopic));
        m.record_break_glass_bypass(BreakGlassAction(GatedAction::UncleanRecovery));

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        drop(r);

        let cases = [
            (
                "freeze rejections",
                "crabka_broker_topic_freeze_rejections_total{topic=\"orders\"} 1",
            ),
            ("freezes active", "crabka_broker_topic_freezes_active 2"),
            (
                "proposals",
                "crabka_broker_break_glass_proposals{state=\"pending\"} 3",
            ),
            (
                "refusals",
                "crabka_broker_break_glass_refusals_total{action=\"delete_topic\"} 1",
            ),
            (
                "bypassed",
                "crabka_broker_break_glass_bypassed_total{action=\"unclean_recovery\"} 1",
            ),
        ];
        for (what, needle) in cases {
            assert!(buf.contains(needle), "{what}: missing {needle} in:\n{buf}");
        }
    }

    #[tokio::test]
    async fn break_glass_state_label_covers_every_state() {
        let cases = [
            ("pending", BreakGlassState::Pending, 1),
            ("approved", BreakGlassState::Approved, 2),
            ("expired", BreakGlassState::Expired, 3),
            ("consumed", BreakGlassState::Consumed, 4),
        ];
        // A new state needs a row here, so the closed label set stays covered.
        assert!(cases.len() == BreakGlassState::ALL.len());

        let m = BrokerMetrics::new();
        for (_, state, count) in cases {
            m.record_break_glass_proposals(state, count);
        }

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        drop(r);

        for (label, _, count) in cases {
            let needle =
                format!("crabka_broker_break_glass_proposals{{state=\"{label}\"}} {count}");
            assert!(buf.contains(&needle), "missing {needle} in:\n{buf}");
        }
    }

    #[tokio::test]
    async fn break_glass_action_label_covers_every_gated_transition() {
        // The expected label text is spelled out here rather than read back
        // from `action_name`, so renaming an action fails this test instead of
        // silently renaming the series an alert rule groups by.
        let cases = [
            ("thaw_topic_freeze", GatedAction::ThawTopicFreeze),
            ("unclean_elect_leaders", GatedAction::UncleanElectLeaders),
            ("unclean_recovery", GatedAction::UncleanRecovery),
            ("unregister_broker", GatedAction::UnregisterBroker),
            ("cancel_reassignment", GatedAction::CancelReassignment),
            ("delete_topic", GatedAction::DeleteTopic),
            ("delete_records", GatedAction::DeleteRecords),
        ];
        // An action added to the metadata enum needs a row here, so the closed
        // label set stays covered.
        assert!(cases.len() == crate::break_glass::ALL_ACTIONS.len());
        for action in crate::break_glass::ALL_ACTIONS {
            assert!(
                cases.iter().any(|(_, cased)| *cased == action),
                "no expected label for {action:?}"
            );
        }

        // Each action gets a distinct refusal count, so a label that resolves
        // to the wrong series shows up as the wrong number rather than as a
        // still-passing test.
        let m = BrokerMetrics::new();
        for (i, (_, action)) in cases.into_iter().enumerate() {
            for _ in 0..=i {
                m.record_break_glass_refusal(BreakGlassAction(action));
            }
            m.record_break_glass_bypass(BreakGlassAction(action));
        }

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        drop(r);

        for (i, (label, _)) in cases.into_iter().enumerate() {
            let refused = format!(
                "crabka_broker_break_glass_refusals_total{{action=\"{label}\"}} {}",
                i + 1
            );
            let bypassed =
                format!("crabka_broker_break_glass_bypassed_total{{action=\"{label}\"}} 1");
            assert!(buf.contains(&refused), "missing {refused} in:\n{buf}");
            assert!(buf.contains(&bypassed), "missing {bypassed} in:\n{buf}");
        }
    }

    #[test]
    fn topic_freeze_rejections_accumulate_per_topic() {
        // KFC-9: the produce gate bumps once per refused partition row, and a
        // topic no freeze covers keeps a flat series.
        let m = BrokerMetrics::new();
        m.record_topic_freeze_rejection("orders");
        m.record_topic_freeze_rejection("orders");
        m.record_topic_freeze_rejection("payments");

        let cases = [("orders", 2), ("payments", 1), ("unfrozen", 0)];
        for (topic, want) in cases {
            let lbl = TopicLabel {
                topic: topic.to_string(),
            };
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = m.topic_freeze_rejections.get_or_create(&lbl).get();
            assert!(got == want, "freeze rejections for {topic}");
        }
    }

    #[test]
    fn kfc9_gauges_fall_as_well_as_rise() {
        // A thaw removes a registry entry and consumes the proposal that
        // authorized it, so both gauges have to come back down.
        let m = BrokerMetrics::new();
        let pending = BreakGlassStateLabel {
            state: BreakGlassState::Pending,
        };
        let consumed = BreakGlassStateLabel {
            state: BreakGlassState::Consumed,
        };

        m.record_topic_freezes_active(3);
        assert!(m.topic_freezes_active.get() == 3);
        m.record_topic_freezes_active(1);
        assert!(m.topic_freezes_active.get() == 1);
        m.record_topic_freezes_active(0);
        assert!(m.topic_freezes_active.get() == 0);

        m.record_break_glass_proposals(BreakGlassState::Pending, 2);
        m.record_break_glass_proposals(BreakGlassState::Consumed, 5);
        // One `get_or_create` guard per statement (first materialization
        // takes the family write lock).
        let up = m.break_glass_proposals.get_or_create(&pending).get();
        assert!(up == 2);

        m.record_break_glass_proposals(BreakGlassState::Pending, 0);
        m.record_break_glass_proposals(BreakGlassState::Consumed, 6);
        let down = m.break_glass_proposals.get_or_create(&pending).get();
        assert!(down == 0);
        let rose = m.break_glass_proposals.get_or_create(&consumed).get();
        assert!(rose == 6);
    }

    #[test]
    fn audit_spool_metrics_present() {
        let m = BrokerMetrics::new();
        m.audit_records_spooled_total.inc();
        m.audit_records_replayed_total.inc();
        m.audit_records_dropped_total.inc();
        m.audit_spool_depth.set(7);
        m.audit_spool_bytes.set(123);
        assert2::check!(m.audit_records_spooled_total.get() == 1);
        assert2::check!(m.audit_spool_depth.get() == 7);
        assert2::check!(m.audit_spool_bytes.get() == 123);
    }
}
