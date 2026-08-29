//! What each KIP-932 acknowledgement type does to an acquired batch. Accept
//! advances the share-partition start offset, and the advance survives a
//! broker restart because the broker persisted it to the share coordinator.
//! Release re-delivers the same offsets at a higher `delivery_count`. Reject
//! archives them, so the start offset moves past the poison record.

use std::time::Duration;

use assert2::{assert, check};
use krabka_broker::{BootstrapMode, Broker};

use crate::{
    ACCEPT, NONE, REJECT, RELEASE,
    harness::{
        bootstrap_share_state, broker_config, broker_test_permit, connect, create_topic, join,
        produce_n, topic_id, wait_for_share_init,
    },
    share_rpc::{acquired_count, fetch_until_acquired, share_ack, share_fetch},
};

/// Acquire 3 records, Accept them all, and observe the SPSO advance. The test
/// then restarts the broker on the same data dir to prove the broker persisted
/// the advance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_accept_restart() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let tid;
    {
        let broker = Broker::start(broker_config(log_dir.clone())).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        create_topic(&broker, &client, "t", 1).await;
        tid = topic_id(&broker, "t");
        bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
        produce_n(&client, "t", tid, 0, 3).await;
        let (member, member_epoch) = join(&client, "g1", "t").await;
        // The group lifecycle initializes share state asynchronously; wait until
        // it is durable so the SPSO advance from the Accept below also persists.
        wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

        // First fetch (epoch 0 opens the session): acquire offsets 0..2.
        let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
        check!(
            acquired_count(&row) == 3,
            "must acquire all 3 offsets, got {:?}",
            row.acquired_records
        );
        check!(
            row.acquired_records.iter().all(|r| r.delivery_count == 1),
            "first delivery_count must be 1, got {:?}",
            row.acquired_records
        );
        check!(
            row.records.is_some(),
            "acquired records must carry record bytes"
        );

        // Accept offsets 0..2 (session epoch is now 1 after the open).
        let ack = share_ack(&client, &member, tid, 1, 0, 2, ACCEPT).await;
        assert!(
            ack.error_code == NONE,
            "accept ack error: {}",
            ack.error_code
        );

        // Next fetch (epoch 2): the SPSO advanced past 2 — nothing left.
        let row2 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
        assert!(
            acquired_count(&row2) == 0,
            "SPSO must have advanced; no records re-acquired, got {:?}",
            row2.acquired_records
        );

        // Wait until the persister has landed the advanced SPSO (>= 3, past
        // offset 2) in __share_group_state before shutting down, so the
        // restart below sees the durable SPSO.
        broker.wait_until_share_spso("g1", tid, 0, 3).await;
        broker.shutdown().await;
    }

    {
        let mut cfg = broker_config(log_dir);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;

        // A fresh member rejoins the recovered group; a fresh-session fetch
        // must observe the recovered SPSO (past offset 2) — zero acquired.
        // Wait until the share state is recovered on the new broker, then
        // assert in a single fetch (no timing guess needed).
        let (member, _) = join(&client, "g1", "t").await;
        broker.wait_for_share_state_summary("g1", tid, 0).await;
        let row = share_fetch(&client, "g1", &member, tid, 0, 0, 0).await;
        let acquired = acquired_count(&row);
        assert!(
            acquired == 0,
            "recovered SPSO must skip the accepted records; re-acquired {acquired}"
        );
    }
}

/// Release re-delivers the same offsets with an incremented `delivery_count`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_redelivers() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 2).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 2, "acquire both offsets");
    assert!(row.acquired_records.iter().all(|r| r.delivery_count == 1));

    // Release offsets 0..1 (epoch 1).
    let ack = share_ack(&client, &member, tid, 1, 0, 1, RELEASE).await;
    assert!(ack.error_code == NONE, "release error: {}", ack.error_code);

    // Next fetch (epoch 2): the same offsets are re-acquired at delivery_count 2.
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
    assert!(
        acquired_count(&row2) == 2,
        "released offsets must be re-acquired, got {:?}",
        row2.acquired_records
    );
    assert!(
        row2.acquired_records.iter().all(|r| r.delivery_count == 2),
        "redelivery must bump delivery_count to 2, got {:?}",
        row2.acquired_records
    );
}

/// Reject archives the records: the broker never re-delivers them AND the SPSO
/// advances past them. A freshly produced offset is the only thing the test
/// acquires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_archives() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 2).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 2, "acquire both offsets");

    // Reject offsets 0..1 (epoch 1) → archived.
    let ack = share_ack(&client, &member, tid, 1, 0, 1, REJECT).await;
    assert!(ack.error_code == NONE, "reject error: {}", ack.error_code);

    // Next fetch (epoch 2): nothing re-acquired.
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
    assert!(
        acquired_count(&row2) == 0,
        "rejected offsets must not be re-acquired, got {:?}",
        row2.acquired_records
    );

    // Produce one more (offset 2). The SPSO advanced past the rejected pair, so
    // only the new offset is acquired — proving the rejected ones were skipped.
    produce_n(&client, "t", tid, 0, 1).await;
    let mut row3 = share_fetch(&client, "g1", &member, tid, 0, 3, 0).await;
    for epoch in 4..18 {
        if acquired_count(&row3) > 0 {
            break;
        }
        // intentional: bounded RPC poll — acquiring the freshly produced offset
        // 2 requires re-fetching; no image/metric signals when it becomes
        // acquirable.
        tokio::time::sleep(Duration::from_millis(100)).await;
        row3 = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
    }
    assert!(
        acquired_count(&row3) == 1,
        "only the new offset must be acquired, got {:?}",
        row3.acquired_records
    );
    assert!(
        row3.acquired_records[0].first_offset == 2 && row3.acquired_records[0].last_offset == 2,
        "acquired offset must be 2 (past the rejected 0..1), got {:?}",
        row3.acquired_records
    );
}
