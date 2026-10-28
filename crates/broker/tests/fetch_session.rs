//! KIP-227 incremental fetch session round-trip tests against an in-process
//! broker.
//!
//! These tests drive the wire protocol directly through the shared `Client`,
//! so they exercise the exact `session_id` / `session_epoch` paths
//! end-to-end.
#![cfg(test)]

use {
    crate::support::{
        client::create_topic as create_support_topic,
        fetch::{
            FetchByteLimit, FetchLimits, FetchSessionEpoch, FetchSessionId, FetchSessionSetup,
            RequestWaitMillis, build_fetch_session_request, default_fetch_limits,
            fetch_request_for, fetch_topic_row, incremental_session_fetch,
        },
        produce::{SinglePartitionProduceSetup, batch_request},
    },
    assert2::{assert, check},
    krabka_protocol::owned::fetch_response::FetchResponse,
};
mod support;

use {
    crate::support::records::empty_record_batch as one_record_batch,
    krabka_protocol::{
        owned::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic},
        primitives::uuid::Uuid as WireUuid,
    },
    support::topic_id_for,
};

async fn incremental_after_produce(process: &support::InProcess, session: i32) -> FetchResponse {
    process
        .client
        .send(
            build_fetch_session_request()
                .limits(FetchLimits::wait_for_data(RequestWaitMillis(200)))
                .id(FetchSessionSetup::at_epoch(FetchSessionId(session), FetchSessionEpoch(2)).id)
                .epoch(
                    FetchSessionSetup::at_epoch(FetchSessionId(session), FetchSessionEpoch(2))
                        .epoch,
                )
                .forgotten(
                    FetchSessionSetup::at_epoch(FetchSessionId(session), FetchSessionEpoch(2))
                        .forgotten,
                )
                .call(),
        )
        .await
        .expect("Fetch incremental after produce")
}

async fn create_topic(p: &support::InProcess, name: &str, num_partitions: i32) {
    create_support_topic(&p.client, name, num_partitions).await;
}

async fn produce(p: &support::InProcess, topic: &str, partition: i32, records: i32) {
    let topic_id = topic_id_for(&p.client, topic).await;
    let req = batch_request(one_record_batch(records), SinglePartitionProduceSetup {
        topic: (topic).into(),
        topic_id,
        partition: krabka_ids::PartitionIndex(partition),
        ..Default::default()
    });
    let resp = p.client.send(req).await.expect("Produce");
    assert!(
        resp.responses[0].partition_responses[0].error_code == 0,
        "Produce error"
    );
}

fn fetch_partition(partition: i32, offset: i64) -> FetchPartition {
    FetchPartition {
        partition,
        fetch_offset: offset,
        partition_max_bytes: 1_048_576,
        ..Default::default()
    }
}

fn fetch_topic(name: &str, topic_id: WireUuid, partitions: Vec<FetchPartition>) -> FetchTopic {
    fetch_topic_row(name, topic_id, partitions)
}

async fn open_session(
    client: &krabka_client_core::Client,
    tid: WireUuid,
    partitions: i32,
    max_wait_ms: i32,
) -> FetchResponse {
    client
        .send(
            build_fetch_session_request()
                .request(fetch_request_for(
                    vec![fetch_topic(
                        "t",
                        tid,
                        (0..partitions).map(|p| fetch_partition(p, 0)).collect(),
                    )],
                    FetchLimits {
                        wait: RequestWaitMillis(max_wait_ms),
                        minimum: FetchByteLimit(0),
                        ..Default::default()
                    },
                ))
                .epoch(0)
                .call(),
        )
        .await
        .expect("new session")
}

struct FetchSessions {
    broker: Option<support::InProcess>,
    topic_id: Option<WireUuid>,
    allocated_id: Option<i32>,
    response: Option<FetchResponse>,
}
impl FetchSessions {
    async fn shutdown(self) {
        self.broker
            .expect("fetch broker started")
            .broker
            .shutdown()
            .await;
    }
}
#[rstest::fixture]
fn sessions() -> FetchSessions {
    // Start this scenario with fresh inputs and observations.

    FetchSessions {
        broker: None,
        topic_id: None,
        allocated_id: None,
        response: None,
    }
}
#[rstest_bdd_macros::given("a fresh broker is ready for fetch session requests")]
async fn fetch_session_broker(sessions: &mut FetchSessions) {
    sessions.broker = Some(support::start().await);
}
#[rstest_bdd_macros::given("topic t contains {partitions} initially empty partitions")]
async fn fetch_session_topic(sessions: &mut FetchSessions, partitions: i32) {
    let broker = sessions.broker.as_ref().expect("fetch broker started");
    create_topic(broker, "t", partitions).await;
    sessions.topic_id = Some(topic_id_for(&broker.client, "t").await);
}
#[rstest_bdd_macros::when(
    "a full fetch opens a session for {partitions} partitions with wait {wait} milliseconds"
)]
async fn open_fetch_session(sessions: &mut FetchSessions, partitions: i32, wait: i32) {
    let broker = sessions.broker.as_ref().expect("fetch broker started");
    let response = open_session(
        &broker.client,
        sessions.topic_id.expect("topic created"),
        partitions,
        wait,
    )
    .await;
    sessions.allocated_id = Some(response.session_id);
    sessions.response = Some(response);
}
#[rstest_bdd_macros::then("the broker allocates a positive fetch session id")]
fn fetch_session_allocated(sessions: &FetchSessions) {
    assert!(sessions.allocated_id.expect("session opened") > 0);
}
#[rstest_bdd_macros::then("the fetch response has error {error} and session id {session}")]
fn fetch_session_response(sessions: &FetchSessions, error: i16, session: String) {
    let response = sessions.response.as_ref().expect("fetch request sent");
    let expected = if session == "allocated" {
        sessions.allocated_id.expect("session opened")
    } else {
        session.parse().expect("expected session id")
    };
    check!(response.error_code == error);
    check!(response.session_id == expected);
}
#[rstest_bdd_macros::then("the full fetch response includes one topic and all three partitions")]
fn full_fetch_session_rows(sessions: &FetchSessions) {
    let response = sessions.response.as_ref().expect("full session requested");
    assert!(response.responses.len() == 1);
    check!(response.responses[0].partitions.len() == 3);
}
#[rstest_bdd_macros::when("an immediate incremental fetch uses the allocated id at epoch 1")]
async fn unchanged_incremental_fetch(sessions: &mut FetchSessions) {
    let p = sessions.broker.as_ref().expect("fetch broker started");
    let sid = sessions.allocated_id.expect("session opened");
    let r2 = p
        .client
        .send(
            incremental_session_fetch()
                .id(sid)
                .limits(FetchLimits::immediate())
                .call(),
        )
        .await
        .expect("Fetch incremental empty");
    sessions.response = Some(r2);
}
#[rstest_bdd_macros::then("the fetch response contains no topic rows")]
fn empty_fetch_session_rows(sessions: &FetchSessions) {
    check!(
        sessions
            .response
            .as_ref()
            .expect("fetch request sent")
            .responses
            .is_empty()
    );
}
#[rstest_bdd_macros::when("the producer appends {records} records to t partition {partition}")]
async fn append_fetch_session_records(sessions: &mut FetchSessions, records: i32, partition: i32) {
    produce(
        sessions.broker.as_ref().expect("fetch broker started"),
        "t",
        partition,
        records,
    )
    .await;
}
#[rstest_bdd_macros::when(
    "an incremental fetch uses the allocated id at epoch 2 and waits 200 milliseconds"
)]
async fn changed_incremental_fetch(sessions: &mut FetchSessions) {
    sessions.response = Some(
        incremental_after_produce(
            sessions.broker.as_ref().expect("fetch broker started"),
            sessions.allocated_id.expect("session opened"),
        )
        .await,
    );
}
#[rstest_bdd_macros::then("only partition 0 is returned with exactly five records")]
fn changed_fetch_session_rows(sessions: &FetchSessions) {
    let r3 = sessions
        .response
        .as_ref()
        .expect("incremental request sent");
    assert!(r3.responses.len() == 1);
    assert!(r3.responses[0].partitions.len() == 1);
    check!(r3.responses[0].partitions[0].partition_index == 0);
    let batches = r3.responses[0].partitions[0]
        .records
        .as_ref()
        .and_then(|p| p.as_v2())
        .expect("v2 records present");
    let total: usize = batches.iter().map(|b| b.records.len()).sum();
    assert!(total == 5);
}
#[rstest_bdd_macros::when("the session forgets t partition 1 at epoch 1")]
async fn forget_fetch_partition(sessions: &mut FetchSessions) {
    let p = sessions.broker.as_ref().expect("fetch broker started");
    let sid = sessions.allocated_id.expect("session opened");
    let tid = sessions.topic_id.expect("topic created");
    let r2 = p
        .client
        .send(
            build_fetch_session_request()
                .limits(FetchLimits::immediate())
                .id(sid)
                .epoch(1)
                .forgotten(vec![ForgottenTopic {
                    topic: "t".into(),
                    topic_id: tid,
                    partitions: vec![1],
                    ..Default::default()
                }])
                .call(),
        )
        .await
        .expect("forget t-1");
    sessions.response = Some(r2);
}
#[rstest_bdd_macros::then(
    "only partition 2 reappears after writes to forgotten partition 1 and retained partition 2"
)]
fn forgotten_fetch_partition_absent(sessions: &FetchSessions) {
    let r3 = sessions
        .response
        .as_ref()
        .expect("incremental request sent");
    let mut seen_partitions: Vec<i32> = r3
        .responses
        .iter()
        .flat_map(|t| t.partitions.iter().map(|p| p.partition_index))
        .collect();
    seen_partitions.sort_unstable();
    assert!(
        seen_partitions == vec![2],
        "t-1 forgotten, only t-2 should appear (t-0 had no new data)"
    );
}
#[rstest_bdd_macros::when("an immediate incremental fetch uses unknown id 999999")]
async fn unknown_fetch_session(sessions: &mut FetchSessions) {
    let p = sessions.broker.as_ref().expect("fetch broker started");
    let setup = FetchSessionSetup::incremental(FetchSessionId(999_999));
    let r = p
        .client
        .send(
            build_fetch_session_request()
                .limits(FetchLimits::immediate())
                .id(setup.id)
                .epoch(setup.epoch)
                .forgotten(setup.forgotten)
                .call(),
        )
        .await
        .expect("Fetch unknown sid");
    sessions.response = Some(r);
}
#[rstest_bdd_macros::when("the allocated session is fetched with stale epoch 99")]
async fn stale_fetch_session(sessions: &mut FetchSessions) {
    let p = sessions.broker.as_ref().expect("fetch broker started");
    let sid = sessions.allocated_id.expect("session opened");
    let setup = FetchSessionSetup::at_epoch(FetchSessionId(sid), FetchSessionEpoch(99));
    let r2 = p
        .client
        .send(
            build_fetch_session_request()
                .limits(default_fetch_limits())
                .id(setup.id)
                .epoch(setup.epoch)
                .forgotten(setup.forgotten)
                .call(),
        )
        .await
        .expect("stale epoch");
    sessions.response = Some(r2);
}
#[rstest_bdd_macros::when("the allocated session is closed with epoch -1")]
async fn close_fetch_session(sessions: &mut FetchSessions) {
    let p = sessions.broker.as_ref().expect("fetch broker started");
    let sid = sessions.allocated_id.expect("session opened");
    let tid = sessions.topic_id.expect("topic created");
    let setup = FetchSessionSetup::at_epoch(FetchSessionId(sid), FetchSessionEpoch(-1));
    let r2 = p
        .client
        .send(
            build_fetch_session_request()
                .request(fetch_request_for(
                    vec![fetch_topic("t", tid, vec![fetch_partition(0, 0)])],
                    default_fetch_limits(),
                ))
                .id(setup.id)
                .epoch(setup.epoch)
                .forgotten(setup.forgotten)
                .call(),
        )
        .await
        .expect("close");
    sessions.response = Some(r2);
}
#[rstest_bdd_macros::when("a sessionless fetch sends zero id with stray epoch 7")]
async fn stray_fetch_session(sessions: &mut FetchSessions) {
    let p = sessions.broker.as_ref().expect("fetch broker started");
    let r = p
        .client
        .send(
            build_fetch_session_request()
                .limits(default_fetch_limits())
                .epoch(7)
                .call(),
        )
        .await
        .expect("stray");
    sessions.response = Some(r);
}
#[rstest_bdd_macros::when(
    "a full sessionless fetch uses id 0 and epoch -1 with wait 100 milliseconds"
)]
async fn sessionless_fetch(sessions: &mut FetchSessions) {
    let p = sessions.broker.as_ref().expect("fetch broker started");
    let tid = sessions.topic_id.expect("topic created");
    let r = p
        .client
        .send(
            build_fetch_session_request()
                .request(fetch_request_for(
                    vec![fetch_topic("t", tid, vec![fetch_partition(0, 0)])],
                    FetchLimits::wait_for_data(RequestWaitMillis(100)),
                ))
                .epoch(-1)
                .call(),
        )
        .await
        .expect("sessionless");
    sessions.response = Some(r);
}
#[rstest_bdd_macros::when("an incremental fetch reuses the closed session id")]
async fn reuse_closed_fetch_session(sessions: &mut FetchSessions) {
    let p = sessions.broker.as_ref().expect("fetch broker started");
    let sid = sessions.allocated_id.expect("session opened");
    let r3 = p
        .client
        .send(
            incremental_session_fetch()
                .id(sid)
                .limits(default_fetch_limits())
                .call(),
        )
        .await
        .expect("after close");
    sessions.response = Some(r3);
}
#[rstest_bdd_macros::then("the sessionless fetch returns one topic with exactly two records")]
fn sessionless_fetch_records(sessions: &FetchSessions) {
    let r = sessions
        .response
        .as_ref()
        .expect("sessionless request sent");
    assert!(r.responses.len() == 1);
    let batches = r.responses[0].partitions[0]
        .records
        .as_ref()
        .and_then(|p| p.as_v2())
        .expect("v2 records");
    let total: usize = batches.iter().map(|b| b.records.len()).sum();
    assert!(total == 2);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/fetch_session.feature",
    name = "new session then incremental filters unchanged partitions"
)]
#[tokio::test]
async fn new_session_then_incremental_filters_unchanged_partitions(sessions: FetchSessions) {
    sessions.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/fetch_session.feature",
    name = "forgotten topics drop partitions from subscription"
)]
#[tokio::test]
async fn forgotten_topics_drop_partitions_from_subscription(sessions: FetchSessions) {
    sessions.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/fetch_session.feature",
    name = "unknown session id returns not found"
)]
#[tokio::test]
async fn unknown_session_id_returns_not_found(sessions: FetchSessions) {
    sessions.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/fetch_session.feature",
    name = "stale session epoch returns invalid epoch"
)]
#[tokio::test]
async fn stale_session_epoch_returns_invalid_epoch(sessions: FetchSessions) {
    sessions.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/fetch_session.feature",
    name = "close session drops cache entry"
)]
#[tokio::test]
async fn close_session_drops_cache_entry(sessions: FetchSessions) { sessions.shutdown().await; }

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/fetch_session.feature",
    name = "sessionless zero id with stray epoch is session id not found"
)]
#[tokio::test]
async fn sessionless_zero_id_with_stray_epoch_is_session_id_not_found(sessions: FetchSessions) {
    sessions.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/fetch_session.feature",
    name = "sessionless full fetch round trip"
)]
#[tokio::test]
async fn sessionless_full_fetch_round_trip(sessions: FetchSessions) { sessions.shutdown().await; }
