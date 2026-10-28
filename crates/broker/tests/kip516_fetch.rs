//! KIP-516: Fetch by `topic_id` error semantics.
//!
//! The test client negotiates the broker's highest `Fetch` version, which
//! carries only the topic id. Kafka's `KafkaApis.handleFetchRequest` answers
//! `UNKNOWN_TOPIC_ID` on every partition row of a topic whose id does not
//! resolve to a name. The zero id is such an id.
#![cfg(test)]
use {
    crate::support::{
        fetch::{FetchLimits, RequestWaitMillis, fetch_request_for},
        topics::{creatable_topic, create_topic_request},
    },
    assert2::assert,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
};

krabka_macros::refused_fetch_partition_fixture!(refused_partition);
mod support;

use krabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchTopic},
        fetch_response::FetchableTopicResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};

/// Kafka's `UNKNOWN_TOPIC_ID` error code.
const UNKNOWN_TOPIC_ID: i16 = 100;

struct FetchIdentity {
    broker: Option<InProcess>,
    ids: Vec<WireUuid>,
    actual: Vec<(WireUuid, Vec<FetchableTopicResponse>)>,
}

#[fixture]
fn fetching() -> FetchIdentity {
    // Start this scenario with fresh inputs and observations.

    FetchIdentity {
        broker: None,
        ids: Vec::new(),
        actual: Vec::new(),
    }
}

#[given("a known fetch topic beside zero and unassigned topic ids")]
async fn unmatched_fetch_ids(fetching: &mut FetchIdentity) {
    let broker = support::start().await;
    broker
        .client
        .send(create_topic_request(
            creatable_topic().name("f_known".into()).call(),
        ))
        .await
        .expect("create topic");
    fetching.broker = Some(broker);
    fetching.ids = support::topics::unresolved_topic_ids(0xdead_beef)
        .into_iter()
        .map(|(_, id)| id)
        .collect();
}

#[when("partitions zero and one are fetched by each unresolved id")]
async fn request_unresolved_partitions(fetching: &mut FetchIdentity) {
    let broker = fetching.broker.as_ref().expect("a broker was started");
    for &topic_id in &fetching.ids {
        let partition = |partition| FetchPartition {
            partition,
            fetch_offset: 0,
            partition_max_bytes: 1_048_576,
            ..Default::default()
        };
        let response = broker
            .client
            .send(fetch_request_for(
                vec![FetchTopic {
                    topic: String::new(),
                    topic_id,
                    partitions: vec![partition(0), partition(1)],
                    ..Default::default()
                }],
                FetchLimits::wait_for_data(RequestWaitMillis(100)),
            ))
            .await
            .expect("fetch");
        fetching.actual.push((topic_id, response.responses));
    }
}

#[then("both fetched partitions report UNKNOWN_TOPIC_ID under the unchanged id")]
fn unknown_fetch_partitions(fetching: &FetchIdentity) {
    assert!(fetching.actual.len() == fetching.ids.len());
    for (topic_id, actual) in &fetching.actual {
        let refused = |partition| refused_partition(partition, UNKNOWN_TOPIC_ID);
        let expected = vec![FetchableTopicResponse {
            topic: String::new(),
            topic_id: *topic_id,
            partitions: vec![refused(0), refused(1)],
            ..Default::default()
        }];
        assert!(*actual == expected);
    }
}

#[scenario(
    path = "tests/features/migrated/tests/kip516_topic_identity.feature",
    name = "Fetching an unresolved identity refuses every requested partition"
)]
#[tokio::test]
async fn fetch_unresolved_topic_id_returns_unknown_topic_id(fetching: FetchIdentity) {
    fetching
        .broker
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}
