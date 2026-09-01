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

use std::sync::{Arc, atomic::AtomicU64};

use prometheus_client::{
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

mod auth;
mod break_glass;
mod delivery;
mod diskless;
mod labels;
mod log_cleaner;
mod phases;
mod registration;
mod replication;
mod request;
mod schema_validation;
mod traffic;

pub use self::labels::{
    ApiKeyLabel, BarrierGroupLabel, BreakGlassAction, BreakGlassActionLabel, BreakGlassState,
    BreakGlassStateLabel, ClientSoftwareLabel, DirectoryLabel, PartitionLabel, QuotaType,
    QuotaTypeLabel, SaslMechanismLabel, SchemaRejectionLabel, ShareGroupLabel, TopicLabel,
    WalShardLabel, WalVoterLabel,
};
pub(crate) use self::{labels::UNKNOWN_LABEL, phases::RequestPhases};

/// Shared registry owning every metric the broker emits. Wrapped in
/// `Arc<Mutex<…>>` because `prometheus-client` requires `&mut Registry`
/// to register and we want lazy registration from multiple init paths.
pub type SharedRegistry = Arc<Mutex<Registry>>;

/// Cheaply-clonable bundle of counter / gauge handles. Construct once
/// in `Broker::start`; hand out clones (each clone is a single
/// `Arc::clone`) to every subsystem that emits.
#[derive(Clone, Debug)]
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
    /// rejected because the request version was outside the registered range.
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
    /// Per-Kafka-API seconds one request spent on this broker's own log
    /// (`krabka_broker_request_local_duration_seconds{api_key}`). It is the
    /// Produce writer round-trip — the enqueue plus the append acknowledgement
    /// — summed over the request's partitions, and the Fetch read of every
    /// planned partition, including the re-read a long poll performs. Mirrors
    /// Kafka's `RequestMetrics.LocalTimeMs`.
    ///
    /// It is one of the three phase families that partition
    /// [`Self::request_duration_seconds`]. See the family's rustdoc on
    /// [`Self::request_throttle_duration_seconds`] for what the three do and
    /// do not sum to.
    pub request_local_duration_seconds: Family<ApiKeyLabel, Histogram>,
    /// Per-Kafka-API seconds one request spent waiting on something that is
    /// not this broker's own log
    /// (`krabka_broker_request_remote_duration_seconds{api_key}`). For Produce
    /// it is the `acks=all` high-watermark gate, summed over the request's
    /// partitions: the wait for every in-sync replica to take the append. For
    /// Fetch it is the long poll that parks a `min_bytes`-unsatisfied read on
    /// the partitions' notifiers, plus the object-store round trip a KIP-405
    /// tiered read or a diskless WAL cold read makes when the local log no
    /// longer holds the offset. Mirrors Kafka's `RequestMetrics.RemoteTimeMs`,
    /// which covers the tiered read for the same reason: Kafka serves it out
    /// of a `DelayedRemoteFetch` in the purgatory, and the purgatory wait is
    /// what that metric measures.
    ///
    /// This is the series that separates a lagging follower or a slow object
    /// store from a slow local disk: a produce that is slow here and fast in
    /// [`Self::request_local_duration_seconds`] is waiting on replication, not
    /// on this broker, and a fetch that is slow here on a tiered topic is
    /// waiting on the tier.
    pub request_remote_duration_seconds: Family<ApiKeyLabel, Histogram>,
    /// Per-Kafka-API seconds one request spent asleep in the KIP-219 quota
    /// throttle (`krabka_broker_request_throttle_duration_seconds{api_key}`).
    /// Mirrors Kafka's `RequestMetrics.ThrottleTimeMs`. It is observed once
    /// per request whose quota the broker accounts for, with an explicit zero
    /// when no quota applied, so a throttled fleet is visible as a shift in
    /// the distribution rather than as an appearing series. The apis the
    /// dispatch registry marks quota-exempt are observed only where they
    /// resolve a throttle of their own — the KIP-599 sleep on `CreateTopics`,
    /// `CreatePartitions` and `DeleteTopics` — and not at all otherwise, so
    /// this `_count` is at most [`Self::request_duration_seconds`]'s.
    ///
    /// The three phase families are disjoint: a request is in exactly one of
    /// them at a time, and each interval is charged to exactly one. They do
    /// **not** cover the total. `local + remote + throttle <=
    /// request_duration_seconds`, and the remainder is the work no phase
    /// names — request decode, authorization, record validation, response
    /// encode. An operator checks the phases against the total by comparing
    /// `_sum` streams; a remainder that grows is handler-side CPU, not disk
    /// and not replication.
    pub request_throttle_duration_seconds: Family<ApiKeyLabel, Histogram>,
    /// Seconds of throttle this broker actually applied, by the quota that
    /// caused it (`krabka_broker_quota_throttle_duration_seconds{quota_type}`).
    ///
    /// A request charges several quotas and sleeps for the largest delay of
    /// them, so the sample lands under the [`QuotaType`] that produced that
    /// largest delay — the quota an operator would have to raise to make the
    /// throttle stop. Requests that no quota delayed are not observed here, so
    /// unlike [`Self::request_throttle_duration_seconds`] the `_count` of this
    /// family is the number of throttled requests, and `_sum` is the wall
    /// time the broker held clients back.
    pub quota_throttle_duration_seconds: Family<QuotaTypeLabel, Histogram>,
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
    /// `unsupported_api_requests` (which counts the unsupported-version arm).
    /// Operators alert on
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
    /// Quorum-durable offset for each diskless WAL shard led by this broker.
    pub diskless_wal_durable_watermark: Family<WalShardLabel, Gauge>,
    /// Leader log-end minus each WAL voter's durable offset.
    pub diskless_wal_voter_lag: Family<WalVoterLabel, Gauge>,
    /// Leader-side attempts that could not form a WAL quorum.
    pub diskless_wal_quorum_loss_events_total: Counter,
    /// Non-empty WAL objects submitted to object storage.
    pub diskless_wal_flush_attempts_total: Counter,
    /// Bytes successfully written as WAL objects.
    pub diskless_wal_flush_bytes_total: Counter,
    /// WAL object flushes that failed after an attempt began.
    pub diskless_wal_flush_failures_total: Counter,
    /// Durable offsets not yet represented by the committed object index.
    pub diskless_wal_index_projection_lag: Family<WalShardLabel, Gauge>,
    /// Local WAL log-start offset after trimming.
    pub diskless_wal_trim_frontier: Family<WalShardLabel, Gauge>,
    pub diskless_wal_cold_read_hits_total: Counter,
    pub diskless_wal_cold_read_misses_total: Counter,
    pub diskless_wal_cold_read_errors_total: Counter,
}
