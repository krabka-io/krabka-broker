//! Share fetches under `ShareIsolationLevel::ReadCommitted`. The acquire
//! window is clamped to the last stable offset, so the records of an open
//! transaction stay invisible until that transaction commits, and the broker
//! surfaces them afterwards rather than losing them.

use std::time::Duration;

use assert2::assert;
use krabka_broker::{Broker, coordinator::unified::share::config::ShareIsolationLevel};
use krabka_client_producer::{Producer, ProducerRecord};

use crate::{
    harness::{
        bootstrap_share_state, broker_config, broker_test_permit, connect, create_topic, join,
        topic_id, wait_for_share_init,
    },
    share_rpc::{acquired_count, share_fetch},
};

/// F2 (`read_committed`): with `isolation_level = ReadCommitted`, a share fetch
/// never surfaces records from an OPEN transaction (offsets past the LSO).
///
/// A transactional producer begins a txn and sends 3 records but does NOT
/// commit. The partition's HWM is then 3 while the LSO stays at 0. A
/// `read_committed` share fetch clamps its read window to `min(LSO, HWM) = 0`,
/// so it acquires nothing. After the txn commits, the LSO advances to 3 and the
/// same group then acquires all 3. This proves the clamp tracked the LSO, and
/// that the broker merely deferred the records and did not lose them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_skips_open_txn_then_sees_committed() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    cfg.share_group.isolation_level = ShareIsolationLevel::ReadCommitted;
    let broker = Broker::start(cfg).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let client = connect(&bootstrap).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, "g1", tid, 0).await;

    // Open a transaction and send 3 records WITHOUT committing: HWM=3, LSO=0.
    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("share-rc-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["a", "b", "c"] {
        drop(
            producer
                .send(ProducerRecord {
                    topic: "t".into(),
                    value: Some(bytes::Bytes::from(v.to_string())),
                    ..Default::default()
                })
                .await,
        );
    }
    // Flush the records to the log (advances HWM) but keep the txn OPEN (LSO=0).
    producer.flush().await.unwrap();

    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // A read_committed share fetch must acquire NOTHING: every record is past
    // the LSO (still 0). Poll a few times to be sure it never spuriously acquires.
    for epoch in 0..6 {
        let row = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
        assert!(
            acquired_count(&row) == 0,
            "read_committed must not surface open-txn records, got {:?}",
            row.acquired_records
        );
        // intentional: deliberately observe that nothing is acquired across a
        // window while the txn stays open (behavior under test, not a
        // state-settle guess).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Commit the transaction → the LSO advances past the records (a commit
    // control marker is appended, so HWM == LSO). The same group now acquires
    // the committed records (proving they were deferred, not dropped). The
    // acquired window also covers the control-marker offset, whose bytes the
    // read path filters out — so we assert on the surfaced record VALUES.
    txn.commit().await.unwrap();
    let mut values: Vec<String> = Vec::new();
    for epoch in 6..30 {
        let row = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
        if acquired_count(&row) > 0
            && let Some(batches) = row.records.as_ref().and_then(|r| r.as_v2())
        {
            values = batches
                .iter()
                .flat_map(|b| b.records.iter())
                .filter_map(|r| r.value.as_ref())
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .collect();
            values.sort();
            if values == vec!["a", "b", "c"] {
                break;
            }
        }
        // intentional: bounded RPC poll for the post-commit LSO advance
        // (transaction-coordinator state, not in the metadata image) to surface
        // via ShareFetch.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        values == vec!["a", "b", "c"],
        "after commit the read_committed fetch must surface the 3 committed \
         records, got {values:?}"
    );

    producer.close().await.unwrap();
}
