//! The two wire-client reads the suite performs against a broker it has no
//! `BrokerHandle` for.
//!
//! Once the leader is killed, the survivor is reached only over its listener,
//! so topic-id resolution and the record read-back are plain `Metadata` and
//! `Fetch` exchanges that poll until the survivor's metadata settles. Both are
//! retry loops around a bare `Client`, which is what keeps them together and
//! away from the produce-side workload.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
    },
    primitives::uuid::Uuid as WireUuid,
};

/// Fetches the topic-id for `name` from the given client with a Metadata
/// request.
pub(crate) async fn topic_id_for(client: &Client, name: &str) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

/// Fetches all records from `(topic, partition)`, starting at `start_offset`,
/// from the broker at `bootstrap`. It retries until `expected_count` records
/// arrive or the deadline passes. Returns the total record count.
pub(crate) async fn fetch_all_records(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    start_offset: i64,
    expected_count: usize,
    deadline: Instant,
) -> usize {
    let client = Client::builder()
        .bootstrap(bootstrap)
        .client_id("tiered-multi-fetch-test")
        .build()
        .await
        .expect("fetch client build");

    // Resolve the topic id first (retry until the metadata is available from
    // the survivor — the leader election may still be settling).
    let topic_id = loop {
        let id = topic_id_for(&client, topic).await;
        if id != WireUuid::default() {
            break id;
        }
        assert!(
            Instant::now() <= deadline,
            "survivor never returned a valid topic id for {topic} within deadline"
        );
        // intentional: topic-id visibility is polled over the wire client (this
        // helper has no BrokerHandle); retry until the survivor's metadata settles.
        tokio::time::sleep(Duration::from_millis(300)).await;
    };

    let mut total_records = 0usize;
    let mut fetch_offset = start_offset;

    loop {
        let resp = client
            .send(FetchRequest {
                max_wait_ms: 1_000,
                min_bytes: 1,
                topics: vec![FetchTopic {
                    topic: topic.into(),
                    topic_id,
                    partitions: vec![FetchPartition {
                        partition,
                        fetch_offset,
                        partition_max_bytes: 2_097_152,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Fetch");

        let part = resp.responses.first().and_then(|t| t.partitions.first());

        if let Some(p) = part {
            if p.error_code == 1 {
                // OFFSET_OUT_OF_RANGE — local-log eviction moved past fetch_offset;
                // advance to log_start_offset.
                if p.log_start_offset > fetch_offset {
                    fetch_offset = p.log_start_offset;
                }
            } else if let Some(recs) = p.records.as_ref().and_then(|r| r.as_v2()) {
                for batch in recs {
                    for rec in &batch.records {
                        total_records += 1;
                        fetch_offset = batch.base_offset + i64::from(rec.offset_delta) + 1;
                    }
                }
            }
        }

        if total_records >= expected_count {
            break;
        }

        assert!(
            Instant::now() <= deadline,
            "survivor only served {total_records}/{expected_count} records before deadline; \
             fetch_offset={fetch_offset}"
        );
        // intentional: records are fetched over the wire client (no BrokerHandle
        // here); retry the bounded Fetch poll until the survivor serves them all.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    total_records
}
