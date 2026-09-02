//! The audit topic itself, and the records that reach it on startup and on an
//! admin operation.
//!
//! The topic has to exist before anything can be audited, the broker's own
//! start has to arrive on it, and a successful `CreateTopics` has to arrive as
//! an `AdminOperation` record that names the topic it created. Each case
//! drives the request over a plain client and then reads the record back off
//! the topic, because fetch visibility can lag the durable write.

use krabka_broker::coordinator::AUDIT_TOPIC;
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    metadata_request::{MetadataRequest, MetadataRequestTopic},
};

use crate::support;

#[tokio::test]
async fn audit_topic_exists_after_startup() {
    let p = support::start().await;
    p.broker.wait_until_partition_present(AUDIT_TOPIC, 0).await;

    // Send a Metadata request for `__krabka_audit` and assert the broker
    // returns it with `error_code == 0` and at least one partition.
    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(AUDIT_TOPIC.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("MetadataRequest failed");

    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(AUDIT_TOPIC))
        .expect("__krabka_audit not in Metadata response");

    assert2::check!(
        topic.error_code == 0,
        "unexpected error code: {}",
        topic.error_code
    );
    assert2::check!(
        !topic.partitions.is_empty(),
        "__krabka_audit has no partitions"
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn broker_started_event_is_written_to_audit_topic() {
    let p = support::start().await;

    // Wait for the BrokerStarted event to be durably written to the audit topic
    // (the sink increments `audit_events` on each successful produce).
    p.broker
        .wait_for_metrics("audit event written", |m| m.audit_events.get() >= 1)
        .await;

    // Fetch visibility (the high watermark) can lag the durable write, so retry
    // until the record is consumable rather than single-shot fetching.
    support::wait_for_audit_record(&p.client, "BrokerStarted", |j| {
        j["class_uid"] == 6002 && j["activity_name"] == "BrokerStarted"
    })
    .await;

    p.broker.shutdown().await;
}

/// Verifies that a successful `CreateTopics` call emits an `AdminOperation`
/// audit record. That record must carry `class_uid == 6003`,
/// `api.operation == "CreateTopics"`, `status_id == 1`, and the topic name in
/// `resources[0].name`.
#[tokio::test]
async fn successful_create_topics_is_audited() {
    let p = support::start().await;

    let audit_before = p.broker.metrics().audit_events.get();
    let cr = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "audited-orders".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert2::check!(cr.topics[0].error_code == 0);

    // Wait for the CreateTopics AdminOperation audit record to be durable.
    p.broker
        .wait_for_metrics("audit event written", |m| {
            m.audit_events.get() > audit_before
        })
        .await;

    // Fetch visibility (the high watermark) can lag the durable write, so retry
    // until the record is consumable rather than single-shot fetching.
    support::wait_for_audit_record(&p.client, "CreateTopics admin audit", |j| {
        j["class_uid"] == 6003
            && j["api"]["operation"] == "CreateTopics"
            && j["status_id"] == 1
            && j["resources"][0]["name"] == "audited-orders"
    })
    .await;

    p.broker.shutdown().await;
}
