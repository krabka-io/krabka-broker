//! What each KIP-932 acknowledgement type does to an acquired batch. Accept
//! advances the share-partition start offset, and the advance survives a
//! broker restart because the broker persisted it to the share coordinator.
//! Release re-delivers the same offsets at a higher `delivery_count`. Reject
//! archives them, so the start offset moves past the poison record.

use {
    crate::{
        harness::{ConsumptionFixture, consumption_fixture, join, produce_n},
        share_rpc::{acquired_count, fetch_until_acquired, share_fetch},
        support::share::{
            RetryEpochs, ShareAck, ShareSessionEpoch, ShareSessionSetup, acknowledge_success,
            refetch_while_empty, rejoin_group,
        },
    },
    assert2::{assert, check},
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::owned::share_fetch_response::PartitionData,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::sync::Arc,
    tempfile::TempDir,
    tokio::sync::OwnedSemaphorePermit,
};

struct AcknowledgementFixture {
    permit: OwnedSemaphorePermit,
    directory: TempDir,
    broker: Option<BrokerHandle>,
    client: Arc<Client>,
    topic: uuid::Uuid,
    member: String,
}

#[derive(Default)]
struct ShareAcknowledgement {
    fixture: Option<AcknowledgementFixture>,
    row: Option<PartitionData>,
}

#[fixture]
fn share_acknowledgement() -> ShareAcknowledgement {
    // Acquired records and their acknowledgement outcomes are actual wire observations.
    ShareAcknowledgement::default()
}

#[given("an acknowledgement-test member has {records} available records on t")]
async fn acknowledgement_records(share_acknowledgement: &mut ShareAcknowledgement, records: i64) {
    let ConsumptionFixture {
        _permit: permit,
        broker,
        client,
        _dir: directory,
        tid: topic,
        member,
    } = consumption_fixture().count(records).call().await;
    share_acknowledgement.fixture = Some(AcknowledgementFixture {
        permit,
        broker: Some(broker),
        client,
        directory,
        topic,
        member,
    });
}

#[when("the acknowledgement-test member opens its session and acquires records")]
async fn acquire_acknowledgement_records(share_acknowledgement: &mut ShareAcknowledgement) {
    share_acknowledgement.row = Some(
        fetch_until_acquired(
            &share_acknowledgement.fixture().client,
            ShareSessionSetup::opening(
                &share_acknowledgement.fixture().member,
                share_acknowledgement.fixture().topic,
            ),
        )
        .await,
    );
}

#[then("the acknowledgement fetch contains {records} records at delivery count {delivery_count}")]
fn acknowledgement_delivery_count(
    share_acknowledgement: &ShareAcknowledgement,
    records: i64,
    delivery_count: i16,
) {
    let response = share_acknowledgement.row.as_ref().expect("records fetched");
    check!(
        acquired_count(response) == records,
        "{:?}",
        response.acquired_records
    );
    check!(
        response
            .acquired_records
            .iter()
            .all(|range| range.delivery_count == delivery_count)
    );
    check!(response.records.is_some(), "acquired records carry bytes");
}

#[when("the member acknowledges through offset {last} with {kind}")]
async fn acknowledge_share_records(
    share_acknowledgement: &ShareAcknowledgement,
    last: i64,
    kind: String,
) {
    let acknowledgement = ShareAck::prefix_for(
        &share_acknowledgement.fixture().member,
        share_acknowledgement.fixture().topic,
        krabka_ids::Offset(last),
    );
    let acknowledgement = match kind.as_str() {
        "accept" => acknowledgement,
        "release" => acknowledgement.release(),
        "reject" => acknowledgement.reject(),
        _ => panic!("unknown acknowledgement {kind}"),
    };
    acknowledge_success(&share_acknowledgement.fixture().client, acknowledgement).await;
}

#[when("the acknowledgement-test member fetches at epoch {epoch}")]
async fn fetch_acknowledgement_records(
    share_acknowledgement: &mut ShareAcknowledgement,
    epoch: i32,
) {
    share_acknowledgement.row = Some(
        share_fetch(
            &share_acknowledgement.fixture().client,
            ShareSessionSetup::joined(
                &share_acknowledgement.fixture().member,
                share_acknowledgement.fixture().topic,
            )
            .fetch_at(ShareSessionEpoch(epoch)),
        )
        .await,
    );
}

#[then("the acknowledgement fetch acquires no records")]
fn acknowledged_records_absent(share_acknowledgement: &ShareAcknowledgement) {
    assert!(acquired_count(share_acknowledgement.row.as_ref().expect("records fetched")) == 0);
}

#[when("the accepted start offset 3 is persisted and the broker restarts")]
async fn restart_accepted_records(share_acknowledgement: &mut ShareAcknowledgement) {
    let fixture = share_acknowledgement
        .fixture
        .as_mut()
        .expect("acknowledgement fixture");
    fixture
        .broker
        .as_ref()
        .expect("first broker")
        .wait_until_share_spso("g1", fixture.topic, 0, 3)
        .await;
    fixture
        .broker
        .take()
        .expect("first broker")
        .shutdown()
        .await;
    let (broker, client) = rejoin_group(fixture.directory.path().to_path_buf(), "g1").await;
    fixture.member = join(&client, "g1", "t").await.member;
    broker
        .wait_for_share_state_summary("g1", fixture.topic, 0)
        .await;
    fixture.broker = Some(broker);
    fixture.client = client;
}

#[when("one new record is appended after the rejected pair and fetched from epoch three")]
async fn fetch_after_rejected_pair(share_acknowledgement: &mut ShareAcknowledgement) {
    produce_n()
        .client(&share_acknowledgement.fixture().client)
        .topic_id(share_acknowledgement.fixture().topic)
        .call()
        .await;
    let response = share_fetch(
        &share_acknowledgement.fixture().client,
        ShareSessionSetup::joined(
            &share_acknowledgement.fixture().member,
            share_acknowledgement.fixture().topic,
        )
        .fetch_at(ShareSessionEpoch(3)),
    )
    .await;
    share_acknowledgement.row = Some(
        refetch_while_empty()
            .client(&share_acknowledgement.fixture().client)
            .row(response)
            .session(ShareSessionSetup {
                member: &share_acknowledgement.fixture().member,
                topic_id: share_acknowledgement.fixture().topic,
                ..Default::default()
            })
            .epochs(RetryEpochs {
                start: ShareSessionEpoch(4),
                ..Default::default()
            })
            .call()
            .await,
    );
}

#[then("only the new offset two is acquired after the rejected records")]
fn rejected_offsets_skipped(share_acknowledgement: &ShareAcknowledgement) {
    let response = share_acknowledgement
        .row
        .as_ref()
        .expect("new record fetched");
    assert!(acquired_count(response) == 1);
    assert!(
        response.acquired_records[0].first_offset == 2
            && response.acquired_records[0].last_offset == 2
    );
}

impl ShareAcknowledgement {
    async fn shutdown(&mut self) {
        let AcknowledgementFixture {
            broker,
            client,
            directory,
            permit,
            ..
        } = self.fixture.take().expect("acknowledgement fixture");
        broker.expect("share broker").shutdown().await;
        drop(client);
        drop(directory);
        drop(permit);
    }
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/acknowledgements.feature",
    name = "consume accept restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_accept_restart(share_acknowledgement: ShareAcknowledgement) {
    share_acknowledgement.shutdown().await;
    drop(share_acknowledgement);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/acknowledgements.feature",
    name = "release redelivers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_redelivers(share_acknowledgement: ShareAcknowledgement) {
    share_acknowledgement.shutdown().await;
    drop(share_acknowledgement);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/acknowledgements.feature",
    name = "reject archives"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_archives(share_acknowledgement: ShareAcknowledgement) {
    share_acknowledgement.shutdown().await;
    drop(share_acknowledgement);
}

impl ShareAcknowledgement {
    fn fixture(&self) -> &AcknowledgementFixture {
        self.fixture.as_ref().expect("live scenario fixture")
    }
}
