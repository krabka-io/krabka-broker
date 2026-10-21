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
            InitializedRestartFixture, acquired_count, fetch_until_acquired,
            initialized_restart_fixture, join, wait_for_share_init,
        },
        support::share::{ShareAck, ShareSessionSetup, acknowledge_success, rejoin_group},
    },
    assert2::assert,
    krabka_protocol::owned::share_fetch_response::AcquiredRecords,
};

async fn assert_share_summary(
    broker: &krabka_broker::BrokerHandle,
    tid: uuid::Uuid,
    expected_dcc: i32,
) {
    let summary = broker
        .share_state_summary_for_test("g1", tid, 0)
        .await
        .map(|(_, _, start, dcc)| (start, dcc));
    assert!(summary == Some((0, expected_dcc)));
}

/// Lag restore: `delivery_complete_count` survives a broker restart.
///
/// Produce N. Acquire all of them and Accept `1..N-1`, so offset 0 still holds
/// the SPSO at 0 and the window holds N-1 terminal records. Wait for the
/// persist. Restart on the same dir with Rejoin. Then read the share-state
/// summary: the SPSO is 0 and the count is the restored N-1, not 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_complete_count_restored_across_restart() {
    const N: i64 = 4;
    const COMPLETE: i32 = 3; // N - 1
    let InitializedRestartFixture {
        _permit,
        _dir,
        log_dir,
        topic: initial,
    } = initialized_restart_fixture().count(N).call().await;

    let tid;
    {
        let (broker, client, topic, member) = initial;
        tid = topic;

        // Acquire 0..N-1 and Accept 1..N-1: the SPSO stays at 0 behind the
        // still-acquired offset 0, and the window holds N-1 terminal records.
        let row = fetch_until_acquired(&client, ShareSessionSetup::opening(&member, tid)).await;
        assert!(acquired_count(&row) == N, "must acquire all {N} offsets");
        acknowledge_success(
            &client,
            ShareAck::prefix_for(&member, tid, krabka_ids::Offset(N - 1))
                .starting_at(krabka_ids::Offset(1)),
        )
        .await;

        // Wait until the persisted summary reflects the count before restarting.
        broker
            .wait_until_share_delivery_complete("g1", tid, 0, COMPLETE)
            .await;
        assert_share_summary(&broker, tid, COMPLETE).await;

        // The awaiter above confirms the count is durable; shut down immediately.
        broker.shutdown().await;
    }

    {
        let (broker, _client) = rejoin_group(log_dir, "g1").await;

        // The summary load is driven by the share coordinator reading the
        // persisted record; await until the recovered state is present.
        broker.wait_for_share_state_summary("g1", tid, 0).await;
        assert_share_summary(&broker, tid, COMPLETE).await;
    }
}

/// What a member holding records leaves behind when the leader crashes.
#[derive(Clone, Copy, Debug)]
enum BeforeTheCrash {
    /// The member acquired the records and did nothing else.
    Acquired,
    /// The member released the records.
    Released,
}

/// An acquisition alone is not written, as in Kafka 4.3.1, where
/// `SharePartition.acquire` changes memory and schedules the lock timer only.
/// A release is written, with the delivery count the acquisition raised.
///
/// Produce 3 and acquire them (delivery count 1). Then either do nothing or
/// release them, and crash the broker, so no session close releases them on
/// the way down. After the restart the records are acquired again. With no
/// write since the load, the delivery count starts over at 1. The release
/// wrote count 1, so the next acquisition is the second delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_acquisition_is_durable_only_once_a_release_writes_it() {
    let mut actual = Vec::new();
    for before in [BeforeTheCrash::Acquired, BeforeTheCrash::Released] {
        let InitializedRestartFixture {
            _permit,
            _dir,
            log_dir,
            topic: (broker, client, tid, member),
        } = initialized_restart_fixture().count(3).call().await;
        let row = fetch_until_acquired(&client, ShareSessionSetup::opening(&member, tid)).await;
        assert!(acquired_count(&row) == 3, "{before:?}: {row:?}");
        if matches!(before, BeforeTheCrash::Released) {
            acknowledge_success(
                &client,
                ShareAck::prefix_for(&member, tid, krabka_ids::Offset(2)).release(),
            )
            .await;
        }
        broker.crash_for_test().await;

        let (broker, client) = rejoin_group(log_dir, "g1").await;
        let member = join(&client, "g1", "t").await.member;
        wait_for_share_init(&broker, "g1", tid, 0).await;
        let row = fetch_until_acquired(&client, ShareSessionSetup::opening(&member, tid)).await;
        actual.push((format!("{before:?}"), row.acquired_records));
        broker.shutdown().await;
    }

    let acquired = |delivery_count| {
        vec![AcquiredRecords {
            first_offset: 0,
            last_offset: 2,
            delivery_count,
            ..Default::default()
        }]
    };
    assert!(
        actual
            == vec![
                ("Acquired".to_owned(), acquired(1)),
                ("Released".to_owned(), acquired(2)),
            ]
    );
}
