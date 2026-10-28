//! Tests for the share state that survives a restart: the restored
//! `delivery_complete_count`, and the delivery counts that an acquisition
//! leaves in memory until a write.
//!
//! The delivery complete count is Kafka's `SharePartition.deliveryCompleteCount`:
//! the number of Acknowledged and Archived records in the in-flight window at
//! or above the SPSO. Share-group lag is `end - start - count`, so a recovered
//! group that reset it to 0 would over-report its lag. The test lives apart
//! from the admin-RPC surfaces because it asserts on the recovered share-state
//! summary rather than on a Describe, Alter, or Delete response.

use {
    crate::{
        harness::{
            AdminOffsetSession, acquired_count, fetch_until_acquired, join, wait_for_share_init,
        },
        support::share::{ShareSessionSetup, acknowledge_success, rejoin_group},
    },
    assert2::assert,
    krabka_protocol::owned::share_fetch_response::{AcquiredRecords, PartitionData},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct ShareStateRecovery {
    session: Option<AdminOffsetSession>,
    acquired: Option<PartitionData>,
}

#[fixture]
fn share_state_recovery() -> ShareStateRecovery {
    // Retain the actual disk and topic identity across graceful restart or crash.
    ShareStateRecovery::default()
}

#[given("initialized share group g1 has {records} records on t")]
async fn recoverable_share_group(share_state_recovery: &mut ShareStateRecovery, records: i64) {
    share_state_recovery.session = Some(AdminOffsetSession::initialized(records).await);
}

#[when("the recovery-test member acquires its available records")]
async fn acquire_recovery_records(share_state_recovery: &mut ShareStateRecovery) {
    share_state_recovery.acquired = Some(
        fetch_until_acquired(
            &share_state_recovery.session().client,
            ShareSessionSetup::opening(
                share_state_recovery
                    .session()
                    .member
                    .as_ref()
                    .expect("joined member"),
                share_state_recovery.session().topic,
            ),
        )
        .await,
    );
}

#[then("the recovery-test acquisition contains {records} records")]
fn recovery_acquisition_count(share_state_recovery: &ShareStateRecovery, records: i64) {
    assert!(
        acquired_count(
            share_state_recovery
                .acquired
                .as_ref()
                .expect("records acquired")
        ) == records
    );
}

#[when("the member accepts offsets 1 through 3 and the delivery-complete count is persisted")]
async fn persist_delivery_completion(share_state_recovery: &ShareStateRecovery) {
    acknowledge_success(
        &share_state_recovery.session().client,
        share_state_recovery
            .session()
            .acknowledgement_through(krabka_ids::Offset(3))
            .starting_at(krabka_ids::Offset(1)),
    )
    .await;
    share_state_recovery
        .session()
        .broker()
        .wait_until_share_delivery_complete("g1", share_state_recovery.session().topic, 0, 3)
        .await;
}

#[then("the share-state summary keeps start offset 0 and delivery-complete count 3")]
async fn retained_delivery_completion(share_state_recovery: &ShareStateRecovery) {
    let summary = share_state_recovery
        .session()
        .broker()
        .share_state_summary_for_test("g1", share_state_recovery.session().topic, 0)
        .await
        .map(|(_, _, start, count)| (start, count));
    assert!(summary == Some((0, 3)));
}

#[when("the share-state broker gracefully restarts and its summary is loaded")]
async fn restart_completed_share_state(share_state_recovery: &mut ShareStateRecovery) {
    let session = share_state_recovery
        .session
        .as_mut()
        .expect("initialized group");
    session
        .broker
        .take()
        .expect("first broker")
        .shutdown()
        .await;
    let (broker, client) = rejoin_group(session.directory.path().to_path_buf(), "g1").await;
    broker
        .wait_for_share_state_summary("g1", session.topic, 0)
        .await;
    session.broker = Some(broker);
    session.client = client;
}

#[when("the recovery-test member releases offsets 0 through 2")]
async fn release_recovery_records(share_state_recovery: &ShareStateRecovery) {
    acknowledge_success(
        &share_state_recovery.session().client,
        share_state_recovery
            .session()
            .acknowledgement_through(krabka_ids::Offset(2))
            .release(),
    )
    .await;
}

#[when("the share-state broker crashes and a new member reacquires after recovery")]
async fn crash_share_acquisition(share_state_recovery: &mut ShareStateRecovery) {
    let session = share_state_recovery
        .session
        .as_mut()
        .expect("initialized group");
    session
        .broker
        .take()
        .expect("first broker")
        .crash_for_test()
        .await;
    let (broker, client) = rejoin_group(session.directory.path().to_path_buf(), "g1").await;
    let member = join(&client, "g1", "t").await.member;
    wait_for_share_init(&broker, "g1", session.topic, 0).await;
    share_state_recovery.acquired = Some(
        fetch_until_acquired(&client, ShareSessionSetup::opening(&member, session.topic)).await,
    );
    session.member = Some(member);
    session.broker = Some(broker);
    session.client = client;
}

#[then(
    "the recovered acquisition is exactly offsets 0 through 2 at delivery count {delivery_count}"
)]
fn recovered_delivery_count(share_state_recovery: &ShareStateRecovery, delivery_count: i16) {
    assert!(
        share_state_recovery
            .acquired
            .as_ref()
            .expect("recovered acquisition")
            .acquired_records
            == vec![AcquiredRecords {
                first_offset: 0,
                last_offset: 2,
                delivery_count,
                ..Default::default()
            }]
    );
}

#[scenario(
    path = "tests/features/migrated/tests/share_admin_offsets/state_restore.feature",
    name = "delivery complete count restored across restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_complete_count_restored_across_restart(share_state_recovery: ShareStateRecovery) {
    share_state_recovery
        .session
        .take()
        .expect("share fixture")
        .shutdown()
        .await;
    drop(share_state_recovery);
}

#[scenario(
    path = "tests/features/migrated/tests/share_admin_offsets/state_restore.feature",
    name = "an acquisition is durable only once a release writes it"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_acquisition_is_durable_only_once_a_release_writes_it(
    share_state_recovery: ShareStateRecovery,
) {
    share_state_recovery
        .session
        .take()
        .expect("share fixture")
        .shutdown()
        .await;
    drop(share_state_recovery);
}

#[scenario(
    path = "tests/features/migrated/tests/share_admin_offsets/state_restore.feature",
    name = "a release persists the delivery count before a crash"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_release_persists_the_delivery_count_before_a_crash(
    share_state_recovery: ShareStateRecovery,
) {
    share_state_recovery
        .session
        .take()
        .expect("share fixture")
        .shutdown()
        .await;
    drop(share_state_recovery);
}

impl ShareStateRecovery {
    fn session(&self) -> &AdminOffsetSession {
        self.session.as_ref().expect("live scenario session")
    }
}
