//! A bucket that nothing charged for longer than the expiration is dropped
//! along with the metric series it published, and an active one is left alone.

use assert2::check;
use krabka_metadata::EntityKey;
use krabka_units::{millis, secs};

use super::*;
use crate::metrics::QuotaEntityLabel;

fn key(user: &str, client_id: &str) -> EntityKey {
    vec![
        ("user".into(), Some(user.into())),
        ("client-id".into(), Some(client_id.into())),
    ]
}

/// The series names a `/metrics` body carries for the per-entity throttle.
fn throttle_series(metrics: &BrokerMetrics) -> Vec<String> {
    let mut body = String::new();
    prometheus_client::encoding::text::encode(
        &mut body,
        &metrics.registry.try_lock().expect("registry"),
    )
    .expect("encode");
    body.lines()
        .filter(|line| line.starts_with("krabka_broker_quota_entity_throttle_seconds_total{"))
        .map(ToString::to_string)
        .collect()
}

#[test]
fn an_inactive_bucket_and_its_metric_series_are_both_dropped() {
    let buckets = QuotaBuckets::new();
    let metrics = BrokerMetrics::new();
    drop(buckets.get_or_create(
        "producer_byte_rate",
        &key("alice", "app"),
        "alice",
        "app",
        1024,
    ));
    drop(
        metrics
            .quota_entity_throttle_seconds_total
            .get_or_create(&QuotaEntityLabel {
                quota_type: QuotaType::Produce,
                user: Some("alice".into()),
                client_id: Some("app".into()),
            }),
    );
    check!(throttle_series(&metrics).len() == 1);

    // Nothing has touched the bucket since it was made, so any positive age
    // below "just now" expires it.
    sweep(&buckets, &metrics, millis(0));

    check!(buckets.len() == 0);
    check!(throttle_series(&metrics).is_empty());
}

#[test]
fn a_bucket_inside_the_window_keeps_its_series() {
    let buckets = QuotaBuckets::new();
    let metrics = BrokerMetrics::new();
    drop(buckets.get_or_create(
        "producer_byte_rate",
        &key("alice", "app"),
        "alice",
        "app",
        1024,
    ));
    drop(
        metrics
            .quota_entity_throttle_seconds_total
            .get_or_create(&QuotaEntityLabel {
                quota_type: QuotaType::Produce,
                user: Some("alice".into()),
                client_id: Some("app".into()),
            }),
    );

    sweep(&buckets, &metrics, secs(3600));

    check!(buckets.len() == 1);
    check!(throttle_series(&metrics).len() == 1);
}

/// Kafka charges every quota under its own config key, and the series is
/// labelled by the `QuotaType` that key names; a sweep that could not make
/// that trip would leave the label set behind.
#[test]
fn every_quota_key_a_bucket_is_created_under_names_a_quota_type() {
    for (config_key, want) in [
        ("producer_byte_rate", QuotaType::Produce),
        ("consumer_byte_rate", QuotaType::Fetch),
        ("request_percentage", QuotaType::Request),
        ("controller_mutation_rate", QuotaType::ControllerMutation),
        ("connection_creation_rate", QuotaType::ConnectionCreation),
    ] {
        check!(QuotaType::from_config_key(config_key) == Some(want));
    }
    check!(QuotaType::from_config_key("not_a_quota") == None);
}
