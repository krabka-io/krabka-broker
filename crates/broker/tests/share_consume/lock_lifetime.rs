//! The lifetime of the acquisition lock a `ShareFetch` takes. The background
//! sweep re-delivers a lock that expires unacknowledged, a record that
//! exhausts `max_delivery_attempts` is archived rather than re-delivered, and
//! a renew-ack extends the deadline so the sweep leaves the record acquired.
//! The last test is the un-renewed control for the renew case.

use {
    crate::{
        NONE,
        harness::initialize_consumption,
        share_rpc::{acquired_count, fetch_until_acquired, share_fetch, share_renew},
        support::share::{
            ConsumptionSetup, MemberSession, ShareRecordCount, ShareSessionEpoch,
            ShareSessionSetup, permitted_topic_fixture,
        },
    },
    assert2::assert,
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{sync::OwnedSemaphorePermit, time},
};

/// A joined share-group member and the owners that keep its broker running.
struct LockFixture {
    permit: OwnedSemaphorePermit,
    broker: krabka_broker::BrokerHandle,
    client: Arc<krabka_client_core::Client>,
    dir: tempfile::TempDir,
    tid: uuid::Uuid,
    member: String,
}

/// Hold the broker permit, materialize t, and join its single-record share group.
async fn lock_fixture(lock_duration: Duration, max_delivery_attempts: Option<i16>) -> LockFixture {
    let (permit, broker, client, dir, tid) = Box::pin(
        permitted_topic_fixture()
            .customize(Box::new(move |config| {
                config.share_group.record_lock_duration = lock_duration;
                if let Some(attempts) = max_delivery_attempts {
                    config.share_group.max_delivery_attempts = attempts;
                }
            }))
            .call(),
    )
    .await;
    let MemberSession { member, .. } = initialize_consumption(&broker, &client, ConsumptionSetup {
        topic_id: tid,
        count: ShareRecordCount(1),
    })
    .await;

    LockFixture {
        permit,
        broker,
        client,
        dir,
        tid,
        member,
    }
}

use {
    krabka_protocol::owned::share_fetch_response::PartitionData,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct ShareLockLifetime {
    fixture: Option<LockFixture>,
    acquired_at: Option<Instant>,
    response: Option<PartitionData>,
    renewal_error: Option<i16>,
}

#[fixture]
fn share_lock_lifetime() -> ShareLockLifetime {
    // Record the real acquisition instant so renewal checks retain calibrated timing.
    ShareLockLifetime::default()
}

#[given(
    "one share record has a {milliseconds}-millisecond lock and delivery attempt limit {attempts}"
)]
async fn share_lock_policy(
    share_lock_lifetime: &mut ShareLockLifetime,
    milliseconds: u64,
    attempts: String,
) {
    let attempts = if attempts == "default" {
        None
    } else {
        Some(attempts.parse().unwrap())
    };
    share_lock_lifetime.fixture =
        Some(Box::pin(lock_fixture(Duration::from_millis(milliseconds), attempts)).await);
}

#[when("the lock-test member acquires its single record without acknowledging")]
async fn acquire_share_lock(share_lock_lifetime: &mut ShareLockLifetime) {
    share_lock_lifetime.acquired_at = Some(Instant::now());
    share_lock_lifetime.response = Some(
        fetch_until_acquired(
            &share_lock_lifetime.fixture().client,
            ShareSessionSetup::opening(
                &share_lock_lifetime.fixture().member,
                share_lock_lifetime.fixture().tid,
            ),
        )
        .await,
    );
}

#[then("the lock-test fetch acquires one record at delivery count {delivery_count}")]
fn share_lock_delivery(share_lock_lifetime: &ShareLockLifetime, delivery_count: i16) {
    let response = share_lock_lifetime
        .response
        .as_ref()
        .expect("lock fetch completed");
    assert!(
        acquired_count(response) == 1
            && response.acquired_records[0].delivery_count == delivery_count,
        "{:?}",
        response.acquired_records
    );
}

#[when("the background sweeper expires the unacknowledged acquisition")]
async fn expire_share_lock(share_lock_lifetime: &ShareLockLifetime) {
    share_lock_lifetime
        .fixture()
        .broker
        .wait_until_share_acquired_count("g1", share_lock_lifetime.fixture().tid, 0, 0)
        .await;
}

#[when("the lock-test member fetches at session epoch {epoch}")]
async fn fetch_share_lock(share_lock_lifetime: &mut ShareLockLifetime, epoch: i32) {
    share_lock_lifetime.response = Some(
        share_fetch(
            &share_lock_lifetime.fixture().client,
            ShareSessionSetup::joined(
                &share_lock_lifetime.fixture().member,
                share_lock_lifetime.fixture().tid,
            )
            .fetch_at(ShareSessionEpoch(epoch)),
        )
        .await,
    );
}

#[then("the lock-test fetch acquires no records")]
fn archived_or_locked_record(share_lock_lifetime: &ShareLockLifetime) {
    assert!(
        acquired_count(
            share_lock_lifetime
                .response
                .as_ref()
                .expect("lock fetch completed")
        ) == 0
    );
}

#[when("the member renews the acquisition after 200 milliseconds")]
async fn renew_share_lock(share_lock_lifetime: &mut ShareLockLifetime) {
    time::sleep(Duration::from_millis(200)).await;
    let response = share_renew()
        .client(&share_lock_lifetime.fixture().client)
        .session(ShareSessionSetup::joined(
            &share_lock_lifetime.fixture().member,
            share_lock_lifetime.fixture().tid,
        ))
        .call()
        .await;
    share_lock_lifetime.renewal_error = Some(response.error_code);
}

#[then("the acquisition renewal succeeds")]
fn renewed_share_lock(share_lock_lifetime: &ShareLockLifetime) {
    assert!(share_lock_lifetime.renewal_error == Some(NONE));
}

#[when("the clock reaches 600 milliseconds after the original acquisition")]
async fn original_share_lock_deadline(share_lock_lifetime: &ShareLockLifetime) {
    let target = share_lock_lifetime
        .acquired_at
        .expect("original acquisition instant")
        + Duration::from_millis(600);
    if let Some(remaining) = target.checked_duration_since(Instant::now()) {
        time::sleep(remaining).await;
    }
}

#[when("800 milliseconds elapse without a renewal")]
async fn unrenewed_share_lock_deadline() { time::sleep(Duration::from_millis(800)).await; }

#[scenario(
    path = "tests/features/migrated/tests/share_consume/lock_lifetime.feature",
    name = "lock timeout redelivers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lock_timeout_redelivers(share_lock_lifetime: ShareLockLifetime) {
    share_lock_lifetime.shutdown().await;
    drop(share_lock_lifetime);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/lock_lifetime.feature",
    name = "delivery limit archives"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_limit_archives(share_lock_lifetime: ShareLockLifetime) {
    share_lock_lifetime.shutdown().await;
    drop(share_lock_lifetime);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/lock_lifetime.feature",
    name = "renew extends lock not redelivered"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renew_extends_lock_not_redelivered(share_lock_lifetime: ShareLockLifetime) {
    share_lock_lifetime.shutdown().await;
    drop(share_lock_lifetime);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/lock_lifetime.feature",
    name = "no renew redelivers after lock expiry"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_renew_redelivers_after_lock_expiry(share_lock_lifetime: ShareLockLifetime) {
    share_lock_lifetime.shutdown().await;
    drop(share_lock_lifetime);
}

impl ShareLockLifetime {
    fn fixture(&self) -> &LockFixture { self.fixture.as_ref().expect("live scenario fixture") }
}

impl ShareLockLifetime {
    async fn shutdown(&mut self) {
        let LockFixture {
            broker,
            client,
            dir,
            permit,
            ..
        } = self.fixture.take().expect("lock fixture");
        broker.shutdown().await;
        drop((client, dir, permit));
    }
}
