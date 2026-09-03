//! `AlterConfigs` (`api_key` 33): the round-trip that pushes a topic override
//! into the partition's log, the rejection of an unknown config key, and the
//! `min.insync.replicas` pre-flight that gates an `acks=-1` produce but leaves
//! `acks=1` alone.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    owned::{
        alter_configs_request::{AlterConfigsRequest, AlterConfigsResource, AlterableConfig},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

use crate::{
    RESOURCE_TYPE_TOPIC,
    admin_harness::{build_client, create_topic_helper},
    support::start_n_node,
};

/// `AlterConfigs` round-trip: a request that sets `retention.ms` on a known
/// topic returns `error_code == 0`. The supervisor then pushes the new config
/// into the partition's log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_configs_round_trip() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-alter", 1).await;

    let req = AlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "t-alter".into(),
            configs: vec![AlterableConfig {
                name: "retention.ms".into(),
                value: Some("60000".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("alter_configs");
    assert!(
        resp.responses[0].error_code == 0,
        "alter_configs response: {:?}",
        resp.responses[0].error_message
    );

    // Wait for the supervisor reconcile loop to push the new config into the
    // partition's log. The supervisor runs on every metadata-image update
    // (typically within a few hundred ms). The partition is queryable
    // immediately after `create_topic_helper` returns, carrying the broker's
    // default retention; we poll until the supervisor swaps in the override
    // (or until the deadline).
    //
    // intentional poll (not an awaiter): the override lands in the local log
    // config *after* the image commits, so no image/metric signal reflects it
    // — same convergence gate the recompression / tiered-storage tests use.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let want = Duration::from_mins(1);
    let last = loop {
        let cur = broker
            .partition_retention_ms_for_test("t-alter", 0)
            .and_then(|inner| inner);
        if cur == Some(want) {
            break cur;
        }
        if std::time::Instant::now() > deadline {
            break cur;
        }
        tokio::task::yield_now().await;
    };
    assert!(
        last == Some(want),
        "retention_ms did not converge within 10 s after AlterConfigs"
    );
}

/// `AlterConfigs` rejects an unknown key with `error_code == 40` (`INVALID_CONFIG`)
/// and includes the offending key name in the error message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_configs_rejects_unknown_key() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-bad-cfg", 1).await;

    let req = AlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "t-bad-cfg".into(),
            configs: vec![AlterableConfig {
                name: "not.a.topic.config".into(),
                value: Some("1000".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("alter_configs");
    // 40 = INVALID_CONFIG
    assert!(
        resp.responses[0].error_code == 40,
        "expected INVALID_CONFIG(40), got {}",
        resp.responses[0].error_code
    );
    assert!(
        resp.responses[0]
            .error_message
            .as_deref()
            .unwrap_or("")
            .contains("not.a.topic.config"),
        "expected error_message to mention `not.a.topic.config`, got {:?}",
        resp.responses[0].error_message
    );
}

/// `min.insync.replicas` pre-flight: the operator sets
/// `min.insync.replicas=2` with `AlterConfigs`. An `acks=-1` produce
/// against a 1-broker cluster (ISR={1}, isr.len()=1) must then fail fast
/// with `NOT_ENOUGH_REPLICAS` (19), before the writer queues the batch.
/// An `acks=1` produce against the same topic still succeeds, because
/// leader-only acks bypass the ISR threshold entirely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn min_insync_replicas_blocks_acks_all_when_isr_too_small() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-min-isr", 1).await;

    // Wait for partition 0 to materialize; otherwise the produce path returns
    // UNKNOWN_TOPIC_OR_PARTITION before the min.insync.replicas pre-flight runs.
    broker.wait_until_partition_present("t-min-isr", 0).await;

    // Produce v13+ drops `name` from the wire and demands `topic_id`.
    // Fetch it via Metadata so the produce calls below resolve.
    let md = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some("t-min-isr".into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    let topic_id: WireUuid = md
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("t-min-isr"))
        .expect("topic in Metadata response")
        .topic_id;

    // Set min.insync.replicas=2 on the topic. The 1-broker cluster only
    // has ISR={1}, so this is impossible to satisfy.
    let alter = AlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "t-min-isr".into(),
            configs: vec![AlterableConfig {
                name: "min.insync.replicas".into(),
                value: Some("2".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };
    let alter_resp = client.send(alter).await.expect("alter_configs");
    assert!(
        alter_resp.responses[0].error_code == 0,
        "AlterConfigs must accept min.insync.replicas=2: {:?}",
        alter_resp.responses[0].error_message
    );

    // Build a one-record batch for the produce calls below.
    let batch = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 0,
        records: vec![Record {
            offset_delta: 0,
            value: Some(Bytes::from_static(b"x")),
            ..Default::default()
        }],
        ..RecordBatch::default()
    };

    // acks=-1 ("all"): must be rejected pre-flight with NOT_ENOUGH_REPLICAS (19).
    let bad = client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "t-min-isr".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch.clone().into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce (acks=-1)");
    assert!(
        bad.responses[0].partition_responses[0].error_code == 19,
        "acks=-1 with isr.len()=1 < min.insync.replicas=2 must return NOT_ENOUGH_REPLICAS (19); \
         got code = {}",
        bad.responses[0].partition_responses[0].error_code
    );

    // acks=1: leader-only — min.insync.replicas does NOT gate, so this
    // must still succeed even though the threshold is unsatisfiable for
    // acks=all.
    let ok = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "t-min-isr".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce (acks=1)");
    assert!(
        ok.responses[0].partition_responses[0].error_code == 0,
        "acks=1 must succeed regardless of min.insync.replicas; got code = {}",
        ok.responses[0].partition_responses[0].error_code
    );
}
