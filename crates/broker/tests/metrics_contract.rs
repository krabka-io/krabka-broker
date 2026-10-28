//! The published metrics contract against the registry that emits it.
//!
//! `docs/operations/grafana-dashboard.json` and
//! `docs/operations/alert-rules.yaml` name series. Every one of those names
//! must be a series the broker exports, or the dashboard shows an empty panel
//! and the alert never fires. This suite builds the registry the broker
//! starts with, encodes it the way the `/metrics` route does, and checks each
//! name the two files spell against that body.
//!
//! `docs/operations/metrics-body.txt` is a checked-in copy of the same body.
//! The CI docs job has no Rust toolchain, so `aspect check-metrics-contract`
//! reads that copy instead of a live broker. This suite keeps the copy fresh:
//! it fails when the copy differs from the body a new registry encodes. To
//! regenerate the copy, run the suite with `KRABKA_METRICS_BODY_OUT` set to
//! the path to write:
//!
//! ```text
//! KRABKA_METRICS_BODY_OUT=docs/operations/metrics-body.txt \
//!     cargo nextest run -p krabka-broker --test metrics_contract
//! ```
#![cfg(test)]

use {
    assert2::assert,
    krabka_broker::metrics::{
        ApiKeyLabel, AuthorizationDeniedLabel, BarrierGroupLabel, BreakGlassAction,
        BreakGlassActionLabel, BreakGlassState, BreakGlassStateLabel, BrokerMetrics,
        CleanerFailureLabel, CleanerFailureReason, ClientSoftwareLabel, ConnectionCloseReason,
        ConnectionCloseReasonLabel, ConsumerGroupLabel, DirectoryLabel, MetadataPartitionLabel,
        PartitionLabel, QuotaEntityLabel, QuotaType, QuotaTypeLabel, RaftStateLabel,
        ReplicaLagLabel, SaslMechanismLabel, SchemaRejectionLabel, ShareGroupIdLabel,
        ShareGroupLabel, TopicLabel, WalShardLabel, WalVoterLabel,
    },
    krabka_metadata::BreakGlassAction as GatedAction,
    prometheus_client::{
        encoding::EncodeLabelSet,
        metrics::family::{Family, MetricConstructor},
    },
    std::{
        collections::BTreeSet,
        env, fs, hash,
        path::{Path, PathBuf},
    },
};

mod support;

/// The dashboard and rules files, relative to the repository root.
const CONTRACT_FILES: [&str; 2] = [
    "docs/operations/grafana-dashboard.json",
    "docs/operations/alert-rules.yaml",
];

/// The checked-in `/metrics` body, relative to the repository root.
const BODY_FILE: &str = "docs/operations/metrics-body.txt";

/// Environment variable naming a path to write the fresh body to.
const BODY_OUT_ENV: &str = "KRABKA_METRICS_BODY_OUT";

/// The repository root, under Cargo and under Bazel.
fn repo_root() -> PathBuf {
    let crate_dir = support::manifest_dir();
    crate_dir
        .parent()
        .and_then(Path::parent)
        .expect("crates/broker sits two levels below the repository root")
        .to_path_buf()
}

/// `body` with the sample lines of every counter and gauge family sorted.
///
/// A `Family` keeps its label sets in a hash map, so a family with more than
/// one live series, such as the three `path` series `fetch_response_drain`
/// registers at startup, encodes them in an order that changes from one run
/// to the next. The checked-in copy needs one order. A histogram's lines are
/// left as encoded: its `le` buckets are already in a fixed order, and a
/// lexical sort would scatter them.
fn canonical(body: &str) -> String {
    let mut out = String::new();
    let mut kind = "";
    let mut samples: Vec<&str> = Vec::new();
    let flush = |samples: &mut Vec<&str>, kind: &str, out: &mut String| {
        if kind != "histogram" {
            samples.sort_unstable();
        }
        for line in samples.drain(..) {
            out.push_str(line);
            out.push('\n');
        }
    };
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            flush(&mut samples, kind, &mut out);
            kind = rest.split_whitespace().nth(1).unwrap_or("");
        } else if line.starts_with('#') {
            flush(&mut samples, kind, &mut out);
        } else {
            samples.push(line);
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    flush(&mut samples, kind, &mut out);
    out
}

/// Gives one label set to every family that shares its label type with
/// others. The destructure names every field without `..`, so a family added
/// to [`BrokerMetrics`] fails to compile here until it is seeded, either in
/// one of these groups or in [`seed_single_families`]. A `_` marks a
/// label-free series, which the encoder writes on its own, or a family that
/// [`seed_single_families`] covers.
/// The one label set of each shape that [`seed_grouped_families`] gives every
/// family it seeds. The values are arbitrary; what matters is that each family
/// carries exactly one series so the encoder writes its `# TYPE` line.
struct SeedLabels {
    topic: TopicLabel,
    partition: PartitionLabel,
    api_key: ApiKeyLabel,
    group: BarrierGroupLabel,
    shard: WalShardLabel,
}

impl SeedLabels {
    fn new() -> Self {
        Self {
            topic: TopicLabel {
                topic: "orders".into(),
            },
            partition: PartitionLabel {
                topic: "orders".into(),
                partition: 0,
            },
            api_key: ApiKeyLabel { api_key: "Produce" },
            group: BarrierGroupLabel {
                group: "orders-cut".into(),
            },
            shard: WalShardLabel {
                topic_id: "00000000-0000-0000-0000-000000000001".into(),
                partition: 0,
            },
        }
    }
}

/// Materializes one series in each of `families` under `label`.
fn seed<L, M, C>(label: &L, families: &[&Family<L, M, C>])
where
    L: Clone + hash::Hash + Eq + EncodeLabelSet,
    C: MetricConstructor<M>,
{
    for family in families {
        drop(family.get_or_create(label));
    }
}

/// Fails to compile when a family is added to [`BrokerMetrics`] and not
/// accounted for by this suite.
///
/// The destructure names every field and uses no `..`, so a new family is a
/// compile error here until its author decides where it is seeded: in one of
/// [`seed_grouped_families`]'s groups, in [`seed_single_families`], or
/// nowhere, because the encoder writes a label-free series on its own.
fn every_family_is_accounted_for(metrics: &BrokerMetrics) {
    let BrokerMetrics {
        registry: _registry,
        topic_bytes_in: _topic_bytes_in,
        topic_bytes_out: _topic_bytes_out,
        topic_messages_in: _topic_messages_in,
        topic_produce_requests: _topic_produce_requests,
        topic_fetch_requests: _topic_fetch_requests,
        topic_failed_produce_requests: _topic_failed_produce_requests,
        topic_failed_fetch_requests: _topic_failed_fetch_requests,
        partition_bytes_in: _partition_bytes_in,
        partition_bytes_out: _partition_bytes_out,
        replication_bytes_in: _replication_bytes_in,
        replication_bytes_out: _replication_bytes_out,
        partition_disk_bytes: _partition_disk_bytes,
        replica_lag: _replica_lag,
        replica_lag_max: _replica_lag_max,
        consumer_group_lag: _consumer_group_lag,
        share_group_backlog: _share_group_backlog,
        share_group_dlq_records: _share_group_dlq_records,
        share_group_dlq_produce_requests: _share_group_dlq_produce_requests,
        share_group_dlq_failed_produce_requests: _share_group_dlq_failed_produce_requests,
        partition_cpu_micros: _partition_cpu_micros,
        partitions_led: _partitions_led,
        partitions_total: _partitions_total,
        under_replicated_partitions: _under_replicated_partitions,
        under_min_isr_partition_count: _under_min_isr_partition_count,
        offline_partitions_count: _offline_partitions_count,
        offline_audit_partitions: _offline_audit_partitions,
        active_controller: _active_controller,
        ignored_static_voters: _ignored_static_voters,
        witness_role: _witness_role,
        leader_site_drift_partitions: _leader_site_drift_partitions,
        voted_directory: _voted_directory,
        controller_leader_changes_total: _controller_leader_changes_total,
        controller_fencing_publications_total: _controller_fencing_publications_total,
        controller_event_queue_time_seconds: _controller_event_queue_time_seconds,
        controller_event_queue_processing_time_seconds:
            _controller_event_queue_processing_time_seconds,
        controller_event_queue_operations_started_total:
            _controller_event_queue_operations_started_total,
        controller_event_queue_operations_timed_out_total:
            _controller_event_queue_operations_timed_out_total,
        controller_event_queue_idle_seconds_total: _controller_event_queue_idle_seconds_total,
        isr_shrinks_total: _isr_shrinks_total,
        isr_expands_total: _isr_expands_total,
        fetch_response_drain: _fetch_response_drain,
        ktls_enabled: _ktls_enabled,
        incremental_fetch_sessions: _incremental_fetch_sessions,
        incremental_fetch_session_evictions_total: _incremental_fetch_session_evictions_total,
        incremental_fetch_partitions_cached: _incremental_fetch_partitions_cached,
        fetch_forgotten_partition_wakes_total: _fetch_forgotten_partition_wakes_total,
        client_software_versions: _client_software_versions,
        successful_authentication: _successful_authentication,
        failed_authentication: _failed_authentication,
        authorization_denied: _authorization_denied,
        api_requests: _api_requests,
        unsupported_api_requests: _unsupported_api_requests,
        request_duration_seconds: _request_duration_seconds,
        request_local_duration_seconds: _request_local_duration_seconds,
        request_remote_duration_seconds: _request_remote_duration_seconds,
        request_throttle_duration_seconds: _request_throttle_duration_seconds,
        quota_throttle_duration_seconds: _quota_throttle_duration_seconds,
        in_flight_requests: _in_flight_requests,
        active_connections: _active_connections,
        connection_closes: _connection_closes,
        request_errors: _request_errors,
        tiered_storage_rlmm_topic_backed: _tiered_storage_rlmm_topic_backed,
        tiered_storage_rlmm_bootstrap_attempts: _tiered_storage_rlmm_bootstrap_attempts,
        tiered_storage_rlmm_partition_ready: _tiered_storage_rlmm_partition_ready,
        tiered_storage_rlmm_partition_target_offset: _tiered_storage_rlmm_partition_target_offset,
        tiered_storage_rlmm_partition_applied_offset: _tiered_storage_rlmm_partition_applied_offset,
        worm_manifests_sealed_total: _worm_manifests_sealed_total,
        worm_manifest_seal_failures_total: _worm_manifest_seal_failures_total,
        produce_message_conversions: _produce_message_conversions,
        fetch_message_conversions: _fetch_message_conversions,
        unclean_leader_elections_total: _unclean_leader_elections_total,
        audit_events: _audit_events,
        audit_write_failures: _audit_write_failures,
        audit_spool_depth: _audit_spool_depth,
        audit_spool_bytes: _audit_spool_bytes,
        audit_records_spooled_total: _audit_records_spooled_total,
        audit_records_replayed_total: _audit_records_replayed_total,
        audit_records_dropped_total: _audit_records_dropped_total,
        client_metrics_otlp_dropped_total: _client_metrics_otlp_dropped_total,
        client_metrics_otlp_failed_total: _client_metrics_otlp_failed_total,
        client_metrics_series_accepted_total: _client_metrics_series_accepted_total,
        client_metrics_series_rejected: _client_metrics_series_rejected,
        client_metrics_series_evicted: _client_metrics_series_evicted,
        client_metrics_retained_series: _client_metrics_retained_series,
        log_cleaner_runs_total: _log_cleaner_runs_total,
        log_cleaner_failures: _log_cleaner_failures,
        log_retention_runs_total: _log_retention_runs_total,
        log_retention_failures: _log_retention_failures,
        log_cleaner_uncleanable_partitions: _log_cleaner_uncleanable_partitions,
        offline_log_dirs: _offline_log_dirs,
        metadata_load_error_count: _metadata_load_error_count,
        log_compactions_total: _log_compactions_total,
        barrier_epochs_started_total: _barrier_epochs_started_total,
        barrier_epochs_committed_total: _barrier_epochs_committed_total,
        barrier_epochs_published_partial_total: _barrier_epochs_published_partial_total,
        barrier_injection_duration_seconds: _barrier_injection_duration_seconds,
        barrier_latest_epoch: _barrier_latest_epoch,
        barrier_markers_written_total: _barrier_markers_written_total,
        barrier_groups_coordinated: _barrier_groups_coordinated,
        delivery_watermark: _delivery_watermark,
        delivery_pending_records: _delivery_pending_records,
        delivery_activation_lateness_seconds: _delivery_activation_lateness_seconds,
        delivery_scheduler_wakeups_total: _delivery_scheduler_wakeups_total,
        schema_validation_rejections: _schema_validation_rejections,
        schema_validation_cache_hits: _schema_validation_cache_hits,
        schema_validation_cache_misses: _schema_validation_cache_misses,
        delivery_clock_uncertainty_seconds: _delivery_clock_uncertainty_seconds,
        topic_freeze_rejections: _topic_freeze_rejections,
        topic_freezes_active: _topic_freezes_active,
        break_glass_proposals: _break_glass_proposals,
        break_glass_refusals: _break_glass_refusals,
        break_glass_bypassed: _break_glass_bypassed,
        diskless_wal_durable_watermark: _diskless_wal_durable_watermark,
        diskless_wal_voter_lag: _diskless_wal_voter_lag,
        diskless_wal_quorum_loss_events_total: _diskless_wal_quorum_loss_events_total,
        diskless_wal_flush_attempts_total: _diskless_wal_flush_attempts_total,
        diskless_wal_flush_bytes_total: _diskless_wal_flush_bytes_total,
        diskless_wal_flush_failures_total: _diskless_wal_flush_failures_total,
        diskless_wal_index_decode_failures_total: _diskless_wal_index_decode_failures_total,
        diskless_wal_index_projection_lag: _diskless_wal_index_projection_lag,
        diskless_wal_trim_frontier: _diskless_wal_trim_frontier,
        diskless_wal_expired_ranges_total: _diskless_wal_expired_ranges_total,
        diskless_wal_cold_read_hits_total: _diskless_wal_cold_read_hits_total,
        diskless_wal_cold_read_misses_total: _diskless_wal_cold_read_misses_total,
        diskless_wal_cold_read_errors_total: _diskless_wal_cold_read_errors_total,
        raft_current_state: _raft_current_state,
        raft_current_epoch: _raft_current_epoch,
        raft_high_watermark: _raft_high_watermark,
        raft_log_end_offset: _raft_log_end_offset,
        raft_voters: _raft_voters,
        raft_observers: _raft_observers,
        metadata_last_applied_offset: _metadata_last_applied_offset,
        metadata_last_committed_offset: _metadata_last_committed_offset,
        metadata_last_applied_timestamp_ms: _metadata_last_applied_timestamp_ms,
        metadata_last_applied_lag_ms: _metadata_last_applied_lag_ms,
        pending_record_processing_time_us_avg: _pending_record_processing_time_us_avg,
        pending_record_processing_time_us_max: _pending_record_processing_time_us_max,
        metadata_lag_records: _metadata_lag_records,
        broker_state: _broker_state,
        active_brokers: _active_brokers,
        fenced_brokers: _fenced_brokers,
        global_topics: _global_topics,
        global_partitions: _global_partitions,
        at_min_isr_partition_count: _at_min_isr_partition_count,
        reassigning_partitions: _reassigning_partitions,
        preferred_replica_imbalance: _preferred_replica_imbalance,
        remote_copy_bytes_total: _remote_copy_bytes_total,
        remote_fetch_bytes_total: _remote_fetch_bytes_total,
        remote_copy_requests_total: _remote_copy_requests_total,
        remote_fetch_requests_total: _remote_fetch_requests_total,
        remote_delete_requests_total: _remote_delete_requests_total,
        remote_copy_errors_total: _remote_copy_errors_total,
        remote_fetch_errors_total: _remote_fetch_errors_total,
        remote_delete_errors_total: _remote_delete_errors_total,
        remote_copy_lag_bytes: _remote_copy_lag_bytes,
        remote_copy_lag_segments: _remote_copy_lag_segments,
        remote_delete_lag_bytes: _remote_delete_lag_bytes,
        remote_delete_lag_segments: _remote_delete_lag_segments,
        replication_throttled_bytes_out_total: _replication_throttled_bytes_out_total,
        replication_throttled_bytes_in_total: _replication_throttled_bytes_in_total,
        replication_throttle_sleeps_total: _replication_throttle_sleeps_total,
        quota_entity_throttle_seconds_total: _quota_entity_throttle_seconds_total,
        queued_requests: _queued_requests,
        queued_request_bytes: _queued_request_bytes,
        remote_log_reader_task_queue_size: _remote_log_reader_task_queue_size,
        remote_log_reader_avg_idle_percent: _remote_log_reader_avg_idle_percent,
        remote_log_reader_fetch_duration_seconds: _remote_log_reader_fetch_duration_seconds,
        remote_log_reader_rejected_total: _remote_log_reader_rejected_total,
        remote_index_cache_hits_total: _remote_index_cache_hits_total,
        remote_index_cache_misses_total: _remote_index_cache_misses_total,
        remote_index_cache_evictions_total: _remote_index_cache_evictions_total,
        remote_index_cache_bytes: _remote_index_cache_bytes,
        remote_index_cache_entries: _remote_index_cache_entries,
        lag_series: _lag_series,
        metric_series: _metric_series,
    } = metrics;
}

fn seed_grouped_families(metrics: &BrokerMetrics) {
    every_family_is_accounted_for(metrics);
    let SeedLabels {
        topic,
        partition,
        api_key,
        group,
        shard,
    } = SeedLabels::new();

    seed(&topic, &[
        &metrics.topic_bytes_in,
        &metrics.topic_bytes_out,
        &metrics.topic_messages_in,
        &metrics.topic_produce_requests,
        &metrics.topic_fetch_requests,
        &metrics.topic_failed_produce_requests,
        &metrics.topic_failed_fetch_requests,
        &metrics.produce_message_conversions,
        &metrics.fetch_message_conversions,
        &metrics.barrier_markers_written_total,
        &metrics.topic_freeze_rejections,
        &metrics.remote_copy_bytes_total,
        &metrics.remote_fetch_bytes_total,
        &metrics.remote_copy_requests_total,
        &metrics.remote_fetch_requests_total,
        &metrics.remote_delete_requests_total,
        &metrics.remote_copy_errors_total,
        &metrics.remote_fetch_errors_total,
        &metrics.remote_delete_errors_total,
    ]);
    seed(&topic, &[
        &metrics.remote_copy_lag_bytes,
        &metrics.remote_copy_lag_segments,
        &metrics.remote_delete_lag_bytes,
        &metrics.remote_delete_lag_segments,
    ]);
    seed(&partition, &[
        &metrics.partition_bytes_in,
        &metrics.partition_bytes_out,
        &metrics.replication_bytes_in,
        &metrics.replication_bytes_out,
        &metrics.partition_cpu_micros,
        &metrics.log_compactions_total,
    ]);
    seed(&partition, &[
        &metrics.partition_disk_bytes,
        &metrics.delivery_watermark,
        &metrics.delivery_pending_records,
    ]);
    seed(&api_key, &[
        &metrics.api_requests,
        &metrics.unsupported_api_requests,
        &metrics.request_errors,
    ]);
    seed(&api_key, &[
        &metrics.request_duration_seconds,
        &metrics.request_local_duration_seconds,
        &metrics.request_remote_duration_seconds,
        &metrics.request_throttle_duration_seconds,
    ]);
    seed(&group, &[
        &metrics.barrier_epochs_started_total,
        &metrics.barrier_epochs_committed_total,
        &metrics.barrier_epochs_published_partial_total,
    ]);
    seed(&group, &[&metrics.barrier_injection_duration_seconds]);
    seed(&group, &[&metrics.barrier_latest_epoch]);
    seed(&shard, &[
        &metrics.diskless_wal_durable_watermark,
        &metrics.diskless_wal_index_projection_lag,
        &metrics.diskless_wal_trim_frontier,
    ]);
}

/// Gives one label set to every family whose label type no other family
/// shares. [`seed_grouped_families`] marks each of these with `_`.
///
/// `fetch_response_drain`, `client_metrics_series_rejected` and
/// `client_metrics_series_evicted` are also marked `_`, but need no seed:
/// registration creates every one of their closed-enum series at zero.
fn seed_single_families(metrics: &BrokerMetrics) {
    drop(metrics.replica_lag.get_or_create(&ReplicaLagLabel {
        topic: "orders".into(),
        partition: 0,
        replica: 2,
    }));
    drop(
        metrics
            .consumer_group_lag
            .get_or_create(&ConsumerGroupLabel {
                group_id: "billing".into(),
                topic: "orders".into(),
                partition: 0,
            }),
    );
    drop(metrics.share_group_backlog.get_or_create(&ShareGroupLabel {
        group_id: "workers".into(),
        topic: "orders".into(),
        partition: 0,
    }));
    seed(
        &ShareGroupIdLabel {
            group_id: "workers".into(),
        },
        &[
            &metrics.share_group_dlq_records,
            &metrics.share_group_dlq_produce_requests,
            &metrics.share_group_dlq_failed_produce_requests,
        ],
    );
    seed(
        &MetadataPartitionLabel {
            metadata_partition: 0,
        },
        &[
            &metrics.tiered_storage_rlmm_partition_ready,
            &metrics.tiered_storage_rlmm_partition_target_offset,
            &metrics.tiered_storage_rlmm_partition_applied_offset,
        ],
    );
    drop(metrics.voted_directory.get_or_create(&DirectoryLabel {
        directory_id: "00000000-0000-0000-0000-000000000001".into(),
    }));
    drop(
        metrics
            .client_software_versions
            .get_or_create(&ClientSoftwareLabel {
                software_name: "apache-kafka-java".into(),
                software_version: "4.0.0".into(),
            }),
    );
    for family in [
        &metrics.successful_authentication,
        &metrics.failed_authentication,
    ] {
        drop(family.get_or_create(&SaslMechanismLabel {
            mechanism: "SCRAM-SHA-512".into(),
        }));
    }
    drop(
        metrics
            .authorization_denied
            .get_or_create(&AuthorizationDeniedLabel {
                operation: "Write".into(),
                resource_type: "Topic".into(),
            }),
    );
    drop(
        metrics
            .quota_throttle_duration_seconds
            .get_or_create(&QuotaTypeLabel {
                quota_type: QuotaType::Produce,
            }),
    );
    drop(
        metrics
            .connection_closes
            .get_or_create(&ConnectionCloseReasonLabel {
                reason: ConnectionCloseReason::Idle,
            }),
    );
    drop(
        metrics
            .schema_validation_rejections
            .get_or_create(&SchemaRejectionLabel {
                topic: "orders".into(),
                reason: "unframed".into(),
            }),
    );
    drop(
        metrics
            .log_cleaner_failures
            .get_or_create(&CleanerFailureLabel {
                topic: "orders".into(),
                partition: 0,
                reason: CleanerFailureReason::Io,
            }),
    );
    drop(
        metrics
            .log_retention_failures
            .get_or_create(&CleanerFailureLabel {
                topic: "orders".into(),
                partition: 0,
                reason: CleanerFailureReason::Io,
            }),
    );
    drop(
        metrics
            .break_glass_proposals
            .get_or_create(&BreakGlassStateLabel {
                state: BreakGlassState::Pending,
            }),
    );
    for family in [&metrics.break_glass_refusals, &metrics.break_glass_bypassed] {
        drop(family.get_or_create(&BreakGlassActionLabel {
            action: BreakGlassAction(GatedAction::UncleanRecovery),
        }));
    }
    drop(
        metrics
            .diskless_wal_voter_lag
            .get_or_create(&WalVoterLabel {
                topic_id: "00000000-0000-0000-0000-000000000001".into(),
                partition: 0,
                voter: 1,
            }),
    );
    for state in RaftStateLabel::ALL {
        drop(metrics.raft_current_state.get_or_create(&state));
    }
    drop(
        metrics
            .quota_entity_throttle_seconds_total
            .get_or_create(&QuotaEntityLabel {
                quota_type: QuotaType::Produce,
                user: Some("alice".into()),
                client_id: Some("producer-1".into()),
            }),
    );
}

/// Every sample name the body can carry, expanded from its `# TYPE` lines.
///
/// A family with no live label set writes no sample line, so the `# TYPE`
/// line is the one place every registered family appears. `prometheus-client`
/// suffixes a counter with `_total` and a histogram with `_bucket`, `_sum` and
/// `_count`; a gauge keeps its bare name.
fn exported_sample_names(body: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for line in body.lines() {
        let Some(rest) = line.strip_prefix("# TYPE ") else {
            continue;
        };
        let mut parts = rest.split_whitespace();
        let (Some(name), Some(kind)) = (parts.next(), parts.next()) else {
            continue;
        };
        match kind {
            "counter" => {
                names.insert(format!("{name}_total"));
            }
            "gauge" | "unknown" => {
                names.insert(name.to_owned());
            }
            "histogram" => {
                for suffix in ["_bucket", "_sum", "_count"] {
                    names.insert(format!("{name}{suffix}"));
                }
            }
            other => panic!("unexpected metric type {other} on {name}"),
        }
    }
    names
}

/// Every `krabka_broker_*` token a contract file spells.
///
/// A token is the metric name and nothing else, so a `PromQL` expression such
/// as `rate(krabka_broker_api_requests_total{api_key="Produce"}[5m])` yields
/// one name and a label value never does.
fn referenced_names(text: &str) -> BTreeSet<String> {
    let token = regex::Regex::new(r"\bkrabka_broker_[A-Za-z0-9_]+").expect("valid pattern");
    token
        .find_iter(text)
        .map(|found| found.as_str().to_owned())
        .collect()
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, scenarios, then, when},
};
struct RegistryContract {
    metrics: Option<BrokerMetrics>,
    body: Option<String>,
    documents: Vec<(String, String)>,
    checked_in: Option<String>,
    snapshot_path: Option<PathBuf>,
}
#[fixture]
fn contract() -> RegistryContract {
    // Start this scenario with fresh inputs and observations.

    RegistryContract {
        metrics: None,
        body: None,
        documents: Vec::new(),
        checked_in: None,
        snapshot_path: None,
    }
}
#[given("a fresh broker metric registry has one placeholder label set in every registered family")]
fn registry(contract: &mut RegistryContract) {
    let metrics = BrokerMetrics::new();
    seed_grouped_families(&metrics);
    seed_single_families(&metrics);
    contract.metrics = Some(metrics);
}
#[given("the checked in Grafana dashboard and alert rules define the operational series contract")]
fn dashboards(contract: &mut RegistryContract) {
    contract.documents = CONTRACT_FILES
        .into_iter()
        .map(|file| {
            (
                file.to_owned(),
                fs::read_to_string(repo_root().join(file))
                    .unwrap_or_else(|error| panic!("read {file}: {error}")),
            )
        })
        .collect();
}
#[given("the checked in metrics body defines the complete encoded registry contract")]
fn snapshot(contract: &mut RegistryContract) {
    contract.snapshot_path = Some(repo_root().join(BODY_FILE));
}
#[when("the metric registry encodes its complete canonical exposition")]
async fn expose(contract: &mut RegistryContract) {
    let metrics = contract
        .metrics
        .as_ref()
        .expect("all metric families seeded");
    krabka_macros::metric_registry_fixture!(metrics, body, registry; expect("registry encodes"));
    contract.body = Some(canonical(&body));
}
#[then(
    "every referenced dashboard and alert series is actually exported and each document names at least ten series"
)]
fn references_exist(contract: &RegistryContract) {
    let exported = exported_sample_names(contract.body.as_ref().expect("registry encoded"));
    assert!(
        exported.contains("krabka_broker_api_requests_total"),
        "registry exports request counter"
    );
    assert!(contract.documents.len() == 2);
    for (file, text) in &contract.documents {
        let referenced = referenced_names(text);
        assert!(
            referenced.len() >= 10,
            "{file} names too few series: {referenced:?}"
        );
        let missing: Vec<&String> = referenced
            .iter()
            .filter(|name| !exported.contains(*name))
            .collect();
        assert!(
            missing.is_empty(),
            "{file} names unexported series: {missing:?}"
        );
    }
}
#[when("the checked in metrics body is read after any requested regeneration")]
fn regenerate(contract: &mut RegistryContract) {
    if let Ok(out) = env::var(BODY_OUT_ENV) {
        fs::write(&out, contract.body.as_ref().expect("registry encoded"))
            .unwrap_or_else(|error| panic!("write {out}: {error}"));
    }
    let path = contract
        .snapshot_path
        .as_ref()
        .expect("snapshot path configured");
    contract.checked_in =
        Some(fs::read_to_string(path).unwrap_or_else(|error| panic!("read {BODY_FILE}: {error}")));
}
#[then("the checked in body exactly matches the fresh canonical registry exposition")]
fn body_matches(contract: &RegistryContract) {
    assert!(
        contract
            .checked_in
            .as_ref()
            .expect("checked in body loaded")
            == contract.body.as_ref().expect("registry encoded"),
        "{BODY_FILE} is stale; regenerate it with {BODY_OUT_ENV}={BODY_FILE}"
    );
}
#[scenario(
    path = "tests/features/migrated/tests/metrics_contract.feature",
    name = "Operational dashboards and alert rules reference real exported samples"
)]
#[tokio::test]
async fn every_dashboard_and_rule_series_is_one_the_registry_exports(contract: RegistryContract) {
    drop(contract);
}
#[scenario(
    path = "tests/features/migrated/tests/metrics_contract.feature",
    name = "The checked in exposition stays synchronized with the complete registry"
)]
#[tokio::test]
async fn checked_in_body_matches_a_fresh_registry(contract: RegistryContract) { drop(contract); }

struct MetricReading {
    input: String,
    names: Option<BTreeSet<String>>,
    canonical: Option<String>,
}
#[fixture]
fn reading() -> MetricReading {
    // Start this scenario with fresh inputs and observations.

    MetricReading {
        input: String::new(),
        names: None,
        canonical: None,
    }
}
#[given("a metric exposition or query has the text")]
fn metric_text(reading: &mut MetricReading, docstring: String) {
    reading.input = format!("{}\n", docstring.trim_matches('\n'));
}
#[when("the exposition reader expands sample names according to metric type")]
fn sample_names(reading: &mut MetricReading) {
    reading.names = Some(exported_sample_names(&reading.input));
}
#[when("the query reader extracts referenced metric tokens")]
fn query_names(reading: &mut MetricReading) {
    reading.names = Some(referenced_names(&reading.input));
}
#[then("the extracted metric names are exactly")]
fn expected_names(reading: &MetricReading, datatable: Vec<Vec<String>>) {
    let expected: BTreeSet<String> = datatable
        .into_iter()
        .skip(1)
        .map(|row| row.into_iter().next().expect("expected sample name"))
        .collect();
    assert!(reading.names.as_ref().expect("names extracted") == &expected);
}
#[when("the exposition is canonicalized for a stable snapshot")]
fn canonicalize(reading: &mut MetricReading) {
    reading.canonical = Some(canonical(&reading.input));
}
#[then("counter samples sort while histogram buckets retain their encoded order")]
fn ordered(reading: &MetricReading, docstring: String) {
    let expected = format!("{}\n", docstring.trim_matches('\n'));
    assert!(
        reading
            .canonical
            .as_ref()
            .expect("exposition canonicalized")
            == &expected
    );
}
scenarios!("tests/features/migrated/tests/metrics_contract_readings.feature", fixtures=[reading:MetricReading]);
