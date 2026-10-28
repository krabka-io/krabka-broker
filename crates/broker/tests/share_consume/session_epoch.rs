//! The share-session state machine that sits in front of the acquire path.
//! The epoch checks reject a stale epoch and a member with no live session,
//! and an incremental request with no topic rows keeps fetching the cached
//! partitions until the client forgets one.

use {
    crate::{
        INVALID_SHARE_SESSION_EPOCH, NONE, ONE_MB, SHARE_SESSION_NOT_FOUND,
        harness::{ConsumptionFixture, join, produce_n, single_record_consumption_fixture, wire},
        share_rpc::{acquired_count, fetch_until_acquired, share_fetch_req},
        support::share::{
            MemberSession, ShareAck, ShareFetchSetup, ShareSessionEpoch, ShareSessionSetup,
            acknowledge_success, empty_session_request,
        },
    },
    assert2::assert,
    krabka_protocol::owned::{
        share_fetch_request::{ForgottenTopic, ShareFetchRequest},
        share_fetch_response::{PartitionData, ShareFetchResponse},
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct SessionEpochScenario {
    fixture: Option<ConsumptionFixture>,
    response: Option<ShareFetchResponse>,
    acquired: Option<PartitionData>,
}

#[fixture]
fn share_session_epoch() -> SessionEpochScenario {
    // Keep the live share session and each actual incremental response distinct.
    SessionEpochScenario::default()
}

#[given("a share member has one available record and no open fetch session")]
async fn unopened_share_session(share_session_epoch: &mut SessionEpochScenario) {
    share_session_epoch.fixture = Some(single_record_consumption_fixture().await);
}

#[when("the share member fetches using session epoch {epoch}")]
async fn fetch_share_session_epoch(share_session_epoch: &mut SessionEpochScenario, epoch: i32) {
    let setup = if epoch == 0 {
        ShareFetchSetup::from(ShareSessionSetup::opening(
            &share_session_epoch.fixture().member,
            share_session_epoch.fixture().tid,
        ))
    } else {
        ShareFetchSetup::at_epoch(
            &share_session_epoch.fixture().member,
            share_session_epoch.fixture().tid,
            ShareSessionEpoch(epoch),
        )
    };
    share_session_epoch.response = Some(
        share_session_epoch
            .fixture()
            .client
            .send(session_fetch_request(setup))
            .await
            .expect("ShareFetch"),
    );
}

#[then("the share session response reports {error}")]
fn share_session_error(share_session_epoch: &SessionEpochScenario, error: String) {
    let expected = match error.as_str() {
        "NONE" => NONE,
        "INVALID_SHARE_SESSION_EPOCH" => INVALID_SHARE_SESSION_EPOCH,
        "SHARE_SESSION_NOT_FOUND" => SHARE_SESSION_NOT_FOUND,
        _ => panic!("unknown session error {error}"),
    };
    let response = share_session_epoch
        .response
        .as_ref()
        .expect("ShareFetch answered");
    assert!(response.error_code == expected, "{response:?}");
}

#[when("another joined member without a fetch session requests epoch five")]
async fn missing_share_session(share_session_epoch: &mut SessionEpochScenario) {
    let MemberSession { member, .. } = join(&share_session_epoch.fixture().client, "g1", "t").await;
    let setup = ShareFetchSetup::at_epoch(
        &member,
        share_session_epoch.fixture().tid,
        ShareSessionEpoch(5),
    );
    share_session_epoch.response = Some(
        share_session_epoch
            .fixture()
            .client
            .send(session_fetch_request(setup))
            .await
            .expect("ShareFetch unknown session"),
    );
}

#[when("the session member opens a fetch and acquires the available record")]
async fn acquire_session_record(share_session_epoch: &mut SessionEpochScenario) {
    share_session_epoch.acquired = Some(
        fetch_until_acquired(
            &share_session_epoch.fixture().client,
            ShareSessionSetup::opening(
                &share_session_epoch.fixture().member,
                share_session_epoch.fixture().tid,
            ),
        )
        .await,
    );
}

#[then("the opening session acquires one record")]
fn opening_session_record(share_session_epoch: &SessionEpochScenario) {
    assert!(
        acquired_count(
            share_session_epoch
                .acquired
                .as_ref()
                .expect("records acquired")
        ) == 1
    );
}

#[when("the member accepts the first session record and another record is produced")]
async fn accept_first_session_record(share_session_epoch: &SessionEpochScenario) {
    acknowledge_success(
        &share_session_epoch.fixture().client,
        ShareAck::prefix_for(
            &share_session_epoch.fixture().member,
            share_session_epoch.fixture().tid,
            krabka_ids::Offset(0),
        ),
    )
    .await;
    produce_n()
        .client(&share_session_epoch.fixture().client)
        .topic_id(share_session_epoch.fixture().tid)
        .call()
        .await;
}

#[when("the member sends an empty incremental fetch at epoch {epoch}")]
async fn empty_incremental_share_fetch(share_session_epoch: &mut SessionEpochScenario, epoch: i32) {
    share_session_epoch.response = Some(
        share_session_epoch
            .fixture()
            .client
            .send(
                empty_session_request()
                    .member(share_session_epoch.fixture().member.clone())
                    .epoch(epoch)
                    .max_bytes(ONE_MB)
                    .call(),
            )
            .await
            .expect("incremental ShareFetch"),
    );
}

#[then("the cached session returns partition zero with one acquired record")]
fn cached_session_record(share_session_epoch: &SessionEpochScenario) {
    let response = share_session_epoch
        .response
        .as_ref()
        .expect("incremental ShareFetch answered");
    assert!(response.error_code == NONE);
    assert!(response.responses.len() == 1);
    assert!(response.responses[0].partitions[0].partition_index == 0);
    assert!(acquired_count(&response.responses[0].partitions[0]) == 1);
}

#[when("the member accepts cached offset one at epoch three")]
async fn accept_cached_session_record(share_session_epoch: &SessionEpochScenario) {
    acknowledge_success(
        &share_session_epoch.fixture().client,
        ShareAck::prefix()
            .member(&share_session_epoch.fixture().member)
            .topic_id(share_session_epoch.fixture().tid)
            .epoch(3)
            .last(1)
            .call()
            .starting_at(krabka_ids::Offset(1)),
    )
    .await;
}

#[when("the member forgets partition zero at session epoch four")]
async fn forget_session_partition(share_session_epoch: &mut SessionEpochScenario) {
    share_session_epoch.response = Some(
        share_session_epoch
            .fixture()
            .client
            .send(
                empty_session_request()
                    .member(share_session_epoch.fixture().member.clone())
                    .epoch(4)
                    .max_bytes(ONE_MB)
                    .forgotten_topics_data(vec![ForgottenTopic {
                        topic_id: wire(share_session_epoch.fixture().tid),
                        partitions: vec![0],
                        ..Default::default()
                    }])
                    .call(),
            )
            .await
            .expect("forget partition"),
    );
}

#[then("the session succeeds with no topic responses")]
fn forgotten_session_partition(share_session_epoch: &SessionEpochScenario) {
    let response = share_session_epoch
        .response
        .as_ref()
        .expect("incremental ShareFetch answered");
    assert!(response.error_code == NONE);
    assert!(response.responses.is_empty());
}

#[when("one further record is produced for the forgotten session partition")]
async fn produce_forgotten_session_record(share_session_epoch: &SessionEpochScenario) {
    produce_n()
        .client(&share_session_epoch.fixture().client)
        .topic_id(share_session_epoch.fixture().tid)
        .call()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/session_epoch.feature",
    name = "session epoch validation"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_epoch_validation(share_session_epoch: SessionEpochScenario) {
    share_session_epoch
        .fixture
        .take()
        .expect("share fixture")
        .broker
        .shutdown()
        .await;
    drop(share_session_epoch);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/session_epoch.feature",
    name = "incremental session uses cached and forgotten partitions"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_session_uses_cached_and_forgotten_partitions(
    share_session_epoch: SessionEpochScenario,
) {
    share_session_epoch
        .fixture
        .take()
        .expect("share fixture")
        .broker
        .shutdown()
        .await;
    drop(share_session_epoch);
}

impl SessionEpochScenario {
    fn fixture(&self) -> &ConsumptionFixture {
        self.fixture.as_ref().expect("live scenario fixture")
    }
}

fn session_fetch_request(setup: ShareFetchSetup<'_>) -> ShareFetchRequest {
    share_fetch_req()
        .session(setup.session)
        .max_wait(setup.max_wait)
        .acks(setup.acks)
        .call()
}
