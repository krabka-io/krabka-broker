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

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use assert2::assert;
use krabka_broker::metrics::{
    ApiKeyLabel, BarrierGroupLabel, BreakGlassAction, BreakGlassActionLabel, BreakGlassState,
    BreakGlassStateLabel, BrokerMetrics, ClientSoftwareLabel, ConnectionCloseReason,
    ConnectionCloseReasonLabel, ConsumerGroupLabel, DirectoryLabel, PartitionLabel, QuotaType,
    QuotaTypeLabel, ReplicaLagLabel, SaslMechanismLabel, SchemaRejectionLabel, ShareGroupLabel,
    TopicLabel, WalShardLabel, WalVoterLabel,
};
use krabka_metadata::BreakGlassAction as GatedAction;

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

/// The `/metrics` body of a registry in which every family carries one label
/// set, encoded exactly as `metrics_server::metrics` encodes it.
///
/// `prometheus-client` writes no `# TYPE` line for a family with no live
/// label set, so a fresh broker exports only its label-free series. One
/// placeholder label set per family makes every registered name appear.
async fn fresh_body() -> String {
    let metrics = BrokerMetrics::new();
    seed_grouped_families(&metrics);
    seed_single_families(&metrics);

    let mut body = String::new();
    let registry = metrics.registry.lock().await;
    prometheus_client::encoding::text::encode(&mut body, &registry).expect("registry encodes");
    canonical(&body)
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
fn seed_grouped_families(metrics: &BrokerMetrics) {
    let topic = TopicLabel {
        topic: "orders".into(),
    };
    let partition = PartitionLabel {
        topic: "orders".into(),
        partition: 0,
    };
    let api_key = ApiKeyLabel {
        api_key: "Produce".into(),
    };
    let group = BarrierGroupLabel {
        group: "orders-cut".into(),
    };
    let shard = WalShardLabel {
        topic_id: "00000000-0000-0000-0000-000000000001".into(),
        partition: 0,
    };
    let BrokerMetrics {
        registry: _,
        topic_bytes_in,
        topic_bytes_out,
        topic_messages_in,
        topic_produce_requests,
        topic_fetch_requests,
        topic_failed_produce_requests,
        topic_failed_fetch_requests,
        partition_bytes_in,
        partition_bytes_out,
        replication_bytes_in,
        replication_bytes_out,
        partition_disk_bytes,
        replica_lag: _,
        replica_lag_max: _,
        consumer_group_lag: _,
        share_group_backlog: _,
        partition_cpu_micros,
        partitions_led: _,
        partitions_total: _,
        under_replicated_partitions: _,
        under_min_isr_partition_count: _,
        offline_partitions_count: _,
        active_controller: _,
        ignored_static_voters: _,
        witness_role: _,
        leader_site_drift_partitions: _,
        voted_directory: _,
        controller_leader_changes_total: _,
        controller_fencing_publications_total: _,
        isr_shrinks_total: _,
        isr_expands_total: _,
        fetch_response_drain: _,
        ktls_enabled: _,
        incremental_fetch_sessions: _,
        incremental_fetch_session_evictions_total: _,
        incremental_fetch_partitions_cached: _,
        client_software_versions: _,
        successful_authentication: _,
        failed_authentication: _,
        api_requests,
        unsupported_api_requests,
        request_duration_seconds,
        request_local_duration_seconds,
        request_remote_duration_seconds,
        request_throttle_duration_seconds,
        quota_throttle_duration_seconds: _,
        in_flight_requests: _,
        active_connections: _,
        connection_closes: _,
        request_errors,
        tiered_storage_rlmm_topic_backed: _,
        tiered_storage_rlmm_bootstrap_attempts: _,
        produce_message_conversions,
        fetch_message_conversions,
        unclean_leader_elections_total: _,
        audit_events: _,
        audit_write_failures: _,
        audit_spool_depth: _,
        audit_spool_bytes: _,
        audit_records_spooled_total: _,
        audit_records_replayed_total: _,
        audit_records_dropped_total: _,
        client_metrics_otlp_dropped_total: _,
        client_metrics_otlp_failed_total: _,
        log_cleaner_runs_total: _,
        log_compactions_total,
        barrier_epochs_started_total,
        barrier_epochs_committed_total,
        barrier_epochs_published_partial_total,
        barrier_injection_duration_seconds,
        barrier_latest_epoch,
        barrier_markers_written_total,
        barrier_groups_coordinated: _,
        delivery_watermark,
        delivery_pending_records,
        delivery_activation_lateness_seconds: _,
        delivery_scheduler_wakeups_total: _,
        schema_validation_rejections: _,
        schema_validation_cache_hits: _,
        schema_validation_cache_misses: _,
        delivery_clock_uncertainty_seconds: _,
        topic_freeze_rejections,
        topic_freezes_active: _,
        break_glass_proposals: _,
        break_glass_refusals: _,
        break_glass_bypassed: _,
        diskless_wal_durable_watermark,
        diskless_wal_voter_lag: _,
        diskless_wal_quorum_loss_events_total: _,
        diskless_wal_flush_attempts_total: _,
        diskless_wal_flush_bytes_total: _,
        diskless_wal_flush_failures_total: _,
        diskless_wal_index_projection_lag,
        diskless_wal_trim_frontier,
        diskless_wal_cold_read_hits_total: _,
        diskless_wal_cold_read_misses_total: _,
        diskless_wal_cold_read_errors_total: _,
        lag_series: _,
    } = metrics;

    for family in [
        topic_bytes_in,
        topic_bytes_out,
        topic_messages_in,
        topic_produce_requests,
        topic_fetch_requests,
        topic_failed_produce_requests,
        topic_failed_fetch_requests,
        produce_message_conversions,
        fetch_message_conversions,
        barrier_markers_written_total,
        topic_freeze_rejections,
    ] {
        drop(family.get_or_create(&topic));
    }
    for family in [
        partition_bytes_in,
        partition_bytes_out,
        replication_bytes_in,
        replication_bytes_out,
        partition_cpu_micros,
        log_compactions_total,
    ] {
        drop(family.get_or_create(&partition));
    }
    for family in [
        partition_disk_bytes,
        delivery_watermark,
        delivery_pending_records,
    ] {
        drop(family.get_or_create(&partition));
    }
    for family in [api_requests, unsupported_api_requests, request_errors] {
        drop(family.get_or_create(&api_key));
    }
    for family in [
        request_duration_seconds,
        request_local_duration_seconds,
        request_remote_duration_seconds,
        request_throttle_duration_seconds,
    ] {
        drop(family.get_or_create(&api_key));
    }
    for family in [
        barrier_epochs_started_total,
        barrier_epochs_committed_total,
        barrier_epochs_published_partial_total,
    ] {
        drop(family.get_or_create(&group));
    }
    drop(barrier_injection_duration_seconds.get_or_create(&group));
    drop(barrier_latest_epoch.get_or_create(&group));
    for family in [
        diskless_wal_durable_watermark,
        diskless_wal_index_projection_lag,
        diskless_wal_trim_frontier,
    ] {
        drop(family.get_or_create(&shard));
    }
}

/// Gives one label set to every family whose label type no other family
/// shares. [`seed_grouped_families`] marks each of these with `_`.
///
/// `fetch_response_drain` is also marked `_`, but needs no seed: registration
/// creates all three of its `path` series at zero.
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
                names.insert(name.to_string());
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
        .map(|found| found.as_str().to_string())
        .collect()
}

#[tokio::test]
async fn every_dashboard_and_rule_series_is_one_the_registry_exports() {
    let exported = exported_sample_names(&fresh_body().await);
    assert!(
        exported.contains("krabka_broker_api_requests_total"),
        "the registry exports the request counter"
    );

    let root = repo_root();
    for file in CONTRACT_FILES {
        let text = std::fs::read_to_string(root.join(file))
            .unwrap_or_else(|error| panic!("read {file}: {error}"));
        let referenced = referenced_names(&text);
        assert!(
            referenced.len() >= 10,
            "{file} names too few series to be the contract: {referenced:?}"
        );
        let missing: Vec<&String> = referenced
            .iter()
            .filter(|name| !exported.contains(*name))
            .collect();
        assert!(
            missing.is_empty(),
            "{file} names series the registry does not export: {missing:?}"
        );
    }
}

#[tokio::test]
async fn checked_in_body_matches_a_fresh_registry() {
    let body = fresh_body().await;
    if let Ok(out) = std::env::var(BODY_OUT_ENV) {
        std::fs::write(&out, &body).unwrap_or_else(|error| panic!("write {out}: {error}"));
    }

    let checked_in = std::fs::read_to_string(repo_root().join(BODY_FILE))
        .unwrap_or_else(|error| panic!("read {BODY_FILE}: {error}"));
    assert!(
        checked_in == body,
        "{BODY_FILE} is stale; regenerate it with {BODY_OUT_ENV}={BODY_FILE}"
    );
}

/// The name expansion is what the Python check repeats, so its rules are
/// pinned here on a body small enough to read.
#[test]
fn sample_names_expand_by_metric_type() {
    let body = "\
# HELP krabka_broker_a A counter.
# TYPE krabka_broker_a counter
krabka_broker_a_total 1
# HELP krabka_broker_b A gauge.
# TYPE krabka_broker_b gauge
# HELP krabka_broker_c A histogram.
# TYPE krabka_broker_c histogram
# EOF
";
    let expected: BTreeSet<String> = [
        "krabka_broker_a_total",
        "krabka_broker_b",
        "krabka_broker_c_bucket",
        "krabka_broker_c_sum",
        "krabka_broker_c_count",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    assert!(exported_sample_names(body) == expected);
}

/// Counter and gauge samples take one order whatever the encoder's hash map
/// produced; histogram buckets keep the order the encoder wrote.
#[test]
fn canonical_body_sorts_counter_samples_and_keeps_bucket_order() {
    let body = "\
# HELP krabka_broker_a A counter.
# TYPE krabka_broker_a counter
krabka_broker_a_total{path=\"vectored\"} 0
krabka_broker_a_total{path=\"pread\"} 0
# HELP krabka_broker_c A histogram.
# TYPE krabka_broker_c histogram
krabka_broker_c_bucket{le=\"0.5\"} 0
krabka_broker_c_bucket{le=\"+Inf\"} 0
krabka_broker_c_sum 0.0
krabka_broker_c_count 0
# EOF
";
    let expected = "\
# HELP krabka_broker_a A counter.
# TYPE krabka_broker_a counter
krabka_broker_a_total{path=\"pread\"} 0
krabka_broker_a_total{path=\"vectored\"} 0
# HELP krabka_broker_c A histogram.
# TYPE krabka_broker_c histogram
krabka_broker_c_bucket{le=\"0.5\"} 0
krabka_broker_c_bucket{le=\"+Inf\"} 0
krabka_broker_c_sum 0.0
krabka_broker_c_count 0
# EOF
";
    assert!(canonical(body) == expected);
}

#[test]
fn referenced_names_are_metric_tokens_only() {
    let text = r#"sum by (topic) (rate(krabka_broker_topic_bytes_in_total{topic!="__x"}[5m]))
        / histogram_quantile(0.99, rate(krabka_broker_request_duration_seconds_bucket[5m]))"#;
    let expected: BTreeSet<String> = [
        "krabka_broker_topic_bytes_in_total",
        "krabka_broker_request_duration_seconds_bucket",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    assert!(referenced_names(text) == expected);
}
