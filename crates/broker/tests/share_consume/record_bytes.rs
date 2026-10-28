//! The record bytes a `ShareFetch` returns for the offsets it acquired. An
//! acquired offset that begins a later on-disk batch must still carry its
//! payload, and a fetch whose acquired window is fragmented must carry every
//! acquired offset and nothing from the gap between them.

use {
    crate::{
        harness::{ConsumptionFixture, bootstrap_share_state, consumption_fixture, produce_n},
        share_rpc::{acquire_three_and_acknowledge, acquired_count, share_fetch},
        support::share::{
            MemberSession, ShareAck, ShareSessionEpoch, ShareSessionSetup, acknowledge_success,
            join_consume_member, permitted_topic_fixture, produce_values, record_values,
            refetch_while_empty,
        },
    },
    assert2::assert,
    krabka_client_core::Client,
    std::{collections::BTreeSet, time::Duration},
    tokio::time,
};

async fn acknowledge_prefix_release(
    client: &Client,
    session: ShareSessionSetup<'_>,
    epoch: i32,
    last: i64,
    starting_at: Option<krabka_ids::Offset>,
) {
    let prefix = || {
        ShareAck::prefix()
            .group(session.group)
            .member(session.member)
            .topic_id(session.topic_id)
            .partition(session.partition.0)
            .epoch(epoch)
            .last(last)
            .call()
    };
    let acknowledgement = starting_at.map_or_else(prefix, |offset| prefix().starting_at(offset));
    acknowledge_success(client, acknowledgement.release()).await;
}

/// Produce a single record carrying `value` into `(topic, partition)` as its OWN
/// batch, so each offset is a distinct on-disk batch. This matters for the
/// fragmented-window read test: the share-fetch read path reads verbatim bytes
/// at *batch* granularity, so each offset must be its own batch to surface
/// byte-exact disjoint offsets. This helper retries while the partition is still
/// materializing.
async fn produce_one(client: &Client, topic: &str, tid: uuid::Uuid, partition: i32, value: &str) {
    produce_values()
        .client(client)
        .values(vec![bytes::Bytes::from(value.to_owned())])
        .topic(topic)
        .topic_id(tid)
        .partition(partition)
        .call()
        .await;
}

use {
    krabka_protocol::owned::share_fetch_response::PartitionData,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct ShareRecordBytes {
    fixture: Option<ConsumptionFixture>,
    response: Option<PartitionData>,
}

#[fixture]
fn share_record_bytes() -> ShareRecordBytes {
    // Keep the actual acquired ranges and their returned record bytes together.
    ShareRecordBytes::default()
}

#[given("a share member has a leading three-record batch available")]
async fn leading_share_batch(share_record_bytes: &mut ShareRecordBytes) {
    share_record_bytes.fixture = Some(consumption_fixture().call().await);
}

#[given("a share member has three distinct single-record batches v0,v1,v2 available")]
async fn separate_share_batches(share_record_bytes: &mut ShareRecordBytes) {
    let (permit, broker, client, directory, topic) =
        Box::pin(permitted_topic_fixture().call()).await;
    bootstrap_share_state(&broker, &client, "g1").await;
    for value in ["v0", "v1", "v2"] {
        produce_one(&client, "t", topic, 0, value).await;
    }
    let MemberSession { member, .. } = join_consume_member(&broker, &client, topic).await;
    share_record_bytes.fixture = Some(ConsumptionFixture {
        _permit: permit,
        broker,
        client,
        _dir: directory,
        tid: topic,
        member,
    });
}

#[when("the member acquires and archives offsets zero through two")]
async fn archive_leading_share_batch(share_record_bytes: &ShareRecordBytes) {
    acquire_three_and_acknowledge(
        &share_record_bytes.fixture().client,
        ShareAck::prefix_for(
            &share_record_bytes.fixture().member,
            share_record_bytes.fixture().tid,
            krabka_ids::Offset(2),
        )
        .reject(),
    )
    .await;
}

#[when("a new single-record v0 batch is appended at offset three")]
async fn append_later_share_batch(share_record_bytes: &ShareRecordBytes) {
    produce_n()
        .client(&share_record_bytes.fixture().client)
        .topic_id(share_record_bytes.fixture().tid)
        .call()
        .await;
}

#[when("the member fetches the record beyond the archived leading batch")]
async fn fetch_later_share_batch(share_record_bytes: &mut ShareRecordBytes) {
    let session = ShareSessionSetup::opening(
        &share_record_bytes.fixture().member,
        share_record_bytes.fixture().tid,
    );
    let response = share_fetch(
        &share_record_bytes.fixture().client,
        session.fetch_at(ShareSessionEpoch(2)),
    )
    .await;
    share_record_bytes.response = Some(
        refetch_while_empty()
            .client(&share_record_bytes.fixture().client)
            .row(response)
            .session(ShareSessionSetup {
                member: &share_record_bytes.fixture().member,
                topic_id: share_record_bytes.fixture().tid,
                ..Default::default()
            })
            .call()
            .await,
    );
}

#[then("offset three is acquired alone and carries the decodable v0 payload")]
fn later_share_batch_bytes(share_record_bytes: &ShareRecordBytes) {
    let response = share_record_bytes
        .response
        .as_ref()
        .expect("later batch fetched");
    assert!(
        acquired_count(response) == 1,
        "{:?}",
        response.acquired_records
    );
    assert!(response.acquired_records[0].first_offset == 3);
    let batches = response
        .records
        .as_ref()
        .and_then(|records| records.as_v2())
        .expect("offset three carries decodable v2 bytes");
    assert!(record_values(batches) == vec!["v0"]);
}

#[when("the member acquires all three records and accepts only middle offset one")]
async fn accept_middle_share_record(share_record_bytes: &ShareRecordBytes) {
    acquire_three_and_acknowledge(
        &share_record_bytes.fixture().client,
        ShareAck::single_for(
            &share_record_bytes.fixture().member,
            share_record_bytes.fixture().tid,
            krabka_ids::Offset(1),
        ),
    )
    .await;
}

#[when("the member releases the outer offsets zero and two")]
async fn release_outer_share_records(share_record_bytes: &ShareRecordBytes) {
    let session = ShareSessionSetup::opening(
        &share_record_bytes.fixture().member,
        share_record_bytes.fixture().tid,
    );
    acknowledge_prefix_release(&share_record_bytes.fixture().client, session, 2, 0, None).await;
    acknowledge_prefix_release(
        &share_record_bytes.fixture().client,
        session,
        3,
        2,
        Some(krabka_ids::Offset(2)),
    )
    .await;
}

#[when("the member fetches until both disjoint outer offsets are reacquired")]
async fn fetch_disjoint_share_records(share_record_bytes: &mut ShareRecordBytes) {
    let session = ShareSessionSetup::opening(
        &share_record_bytes.fixture().member,
        share_record_bytes.fixture().tid,
    );
    let mut response = share_fetch(
        &share_record_bytes.fixture().client,
        session.fetch_at(ShareSessionEpoch(4)),
    )
    .await;
    for epoch in 5..20 {
        if acquired_count(&response) >= 2 {
            break;
        }
        time::sleep(Duration::from_millis(100)).await;
        response = share_fetch(
            &share_record_bytes.fixture().client,
            session.fetch_at(ShareSessionEpoch(epoch)),
        )
        .await;
    }
    share_record_bytes.response = Some(response);
}

#[then(
    "the acquired offsets are exactly zero and two and the record bytes exclude offset one's v1"
)]
fn disjoint_share_record_bytes(share_record_bytes: &ShareRecordBytes) {
    let response = share_record_bytes
        .response
        .as_ref()
        .expect("disjoint batch fetched");
    let acquired: BTreeSet<i64> = response
        .acquired_records
        .iter()
        .flat_map(|range| range.first_offset..=range.last_offset)
        .collect();
    assert!(acquired == maplit::btreeset! {0, 2});
    let batches = response
        .records
        .as_ref()
        .and_then(|records| records.as_v2())
        .expect("disjoint acquired ranges carry decodable v2 bytes");
    let offsets: BTreeSet<i64> = batches
        .iter()
        .flat_map(|batch| {
            let base = batch.base_offset;
            batch
                .records
                .iter()
                .map(move |record| base + i64::from(record.offset_delta))
        })
        .collect();
    assert!(
        offsets == acquired,
        "record offsets must exactly match acquired offsets"
    );
    assert!(!record_values(batches).contains(&"v1".to_owned()));
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/record_bytes.feature",
    name = "acquire past leading batch returns bytes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acquire_past_leading_batch_returns_bytes(share_record_bytes: ShareRecordBytes) {
    share_record_bytes
        .fixture
        .take()
        .expect("share fixture")
        .broker
        .shutdown()
        .await;
    drop(share_record_bytes);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/record_bytes.feature",
    name = "fragmented window records match acquired offsets"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_window_records_match_acquired_offsets(share_record_bytes: ShareRecordBytes) {
    share_record_bytes
        .fixture
        .take()
        .expect("share fixture")
        .broker
        .shutdown()
        .await;
    drop(share_record_bytes);
}

impl ShareRecordBytes {
    fn fixture(&self) -> &ConsumptionFixture {
        self.fixture.as_ref().expect("live scenario fixture")
    }
}
