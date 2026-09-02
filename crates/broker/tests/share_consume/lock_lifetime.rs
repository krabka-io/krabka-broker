//! The lifetime of the acquisition lock a `ShareFetch` takes. The background
//! sweep re-delivers a lock that expires unacknowledged, a record that
//! exhausts `max_delivery_attempts` is archived rather than re-delivered, and
//! a renew-ack extends the deadline so the sweep leaves the record acquired.
//! The last test is the un-renewed control for the renew case.

use std::time::Duration;

use assert2::assert;
use krabka_broker::Broker;

use crate::{
    NONE,
    harness::{
        bootstrap_share_state, broker_config, broker_test_permit, connect, create_topic, join,
        produce_n, topic_id, wait_for_share_init,
    },
    share_rpc::{acquired_count, fetch_until_acquired, share_fetch, share_renew},
};

/// The background sweep reverts an acquired-but-unacknowledged lock that
/// expires, so the next fetch re-delivers at an incremented count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lock_timeout_redelivers() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    cfg.share_group.record_lock_duration = Duration::from_millis(200);
    let broker = Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, "g1", tid, 0).await;
    produce_n(&client, "t", tid, 0, 1).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // Fetch but DO NOT acknowledge.
    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 1, "acquire the single offset");
    assert!(row.acquired_records[0].delivery_count == 1);

    // Wait until the lock expires and the background sweeper reverts the
    // record to Available (acquired-batch count drops to 0).
    broker
        .wait_until_share_acquired_count("g1", tid, 0, 0)
        .await;

    // Next fetch (epoch 1) re-acquires the same offset at delivery_count 2.
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 1, 0).await;
    assert!(
        acquired_count(&row2) == 1,
        "expired-lock offset must be re-acquired, got {:?}",
        row2.acquired_records
    );
    assert!(
        row2.acquired_records[0].delivery_count == 2,
        "re-delivery after lock timeout must bump delivery_count to 2, got {}",
        row2.acquired_records[0].delivery_count
    );
}

/// The broker archives a record that exhausts `max_delivery_attempts` without
/// an Accept (poison pill). Later fetches acquire nothing and the SPSO advances.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_limit_archives() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    cfg.share_group.record_lock_duration = Duration::from_millis(150);
    cfg.share_group.max_delivery_attempts = 2;
    let broker = Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, "g1", tid, 0).await;
    produce_n(&client, "t", tid, 0, 1).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // Delivery 1 (no ack).
    let row1 = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(row1.acquired_records[0].delivery_count == 1);

    // Wait until the lock expires and the sweeper reverts the record to Available
    // (acquired-batch count drops to 0), then re-fetch for delivery 2.
    broker
        .wait_until_share_acquired_count("g1", tid, 0, 0)
        .await;

    // Delivery 2 (no ack).
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 1, 0).await;
    assert!(
        acquired_count(&row2) == 1 && row2.acquired_records[0].delivery_count == 2,
        "second delivery must be count 2, got {:?}",
        row2.acquired_records
    );

    // Wait until that lock expires too — the sweeper reverts the record back to
    // Available (delivery_count=2, which equals max_delivery_attempts). The
    // archiving (dcc increment) happens during the next acquire call when the
    // broker detects the poison pill.
    broker
        .wait_until_share_acquired_count("g1", tid, 0, 0)
        .await;

    // Subsequent fetch: the acquire path detects delivery_count >= max_attempts
    // and archives the record — SPSO advances, nothing is returned.
    let row3 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
    assert!(
        acquired_count(&row3) == 0,
        "poison record must be archived, not re-delivered, got {:?}",
        row3.acquired_records
    );
}

/// F1 (renew): a renew-ack extends the acquisition lock. The broker does NOT
/// re-acquire a record that it would otherwise re-deliver after the lock expires.
///
/// Config: `record_lock_duration = 500ms` (sweeper ticks at 250ms). Acquire
/// offset 0 with a 500ms lock, then send a renew-ack about 200ms in. The
/// renew-ack resets the deadline to renew-time + 500ms (≈ T0+700ms). Then check
/// at about T0+600ms, which is PAST the original 500ms deadline but BEFORE the
/// renewed 700ms deadline. The sweeper would already have swept and re-delivered
/// an un-renewed lock. The renew kept the record Acquired, so the fetch acquires
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renew_extends_lock_not_redelivered() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    cfg.share_group.record_lock_duration = Duration::from_millis(500);
    let broker = Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, "g1", tid, 0).await;
    produce_n(&client, "t", tid, 0, 1).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // Acquire offset 0 (lock 500ms, delivery_count 1). Epoch is now 1.
    let acquire_at = std::time::Instant::now();
    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 1, "acquire the single offset");
    assert!(row.acquired_records[0].delivery_count == 1);

    // Intentional calibrated timing: renew ~200ms in (before the 500ms lock
    // expires) to reset the deadline to renew-time + 500ms ≈ T0+700ms. Epoch
    // is now 2. This sleep proves renew timing; it is NOT a flaky state-guess.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let renew = share_renew(&client, &member, tid, 1, 0, 0).await;
    assert!(
        renew.error_code == NONE,
        "renew must succeed for an acquired offset, got {}",
        renew.error_code
    );

    // Intentional calibrated timing: wait until ~600ms after the ORIGINAL
    // acquire — past the original 500ms deadline (un-renewed lock would have
    // been swept) but before the renewed ~700ms deadline. The remaining sleep
    // is computed from the real acquire instant so scheduling jitter doesn't
    // overshoot the renewed window. This proves the renew suppressed redelivery;
    // it is NOT a flaky state-guess.
    let target = acquire_at + Duration::from_millis(600);
    if let Some(rem) = target.checked_duration_since(std::time::Instant::now()) {
        tokio::time::sleep(rem).await;
    }
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
    assert!(
        acquired_count(&row2) == 0,
        "renew must keep the lock; offset 0 must NOT be re-acquired, got {:?}",
        row2.acquired_records
    );
}

/// F1 (control): the SAME timing WITHOUT a renew re-acquires the offset after
/// the lock expires, at `delivery_count` 2. This proves the renew above
/// suppressed the redelivery, and that slack in the timing did not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_renew_redelivers_after_lock_expiry() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    cfg.share_group.record_lock_duration = Duration::from_millis(500);
    let broker = Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, "g1", tid, 0).await;
    produce_n(&client, "t", tid, 0, 1).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 1, "acquire the single offset");
    assert!(row.acquired_records[0].delivery_count == 1);

    // Intentional calibrated timing: no renew — wait 800ms (well past the 500ms
    // lock + a sweeper tick) so the record is reverted to Available and
    // re-delivered. This sleep mirrors the renew test's timing to prove that
    // WITHOUT a renew the lock IS swept; it is NOT a flaky state-guess.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 1, 0).await;
    assert!(
        acquired_count(&row2) == 1,
        "without renew the expired lock must re-acquire, got {:?}",
        row2.acquired_records
    );
    assert!(
        row2.acquired_records[0].delivery_count == 2,
        "re-delivery after lock timeout must bump delivery_count to 2, got {}",
        row2.acquired_records[0].delivery_count
    );
}
